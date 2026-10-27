(ns ^:mb/driver-tests metabase-enterprise.sandbox.generative-corpus-test
  "Systematic sandboxing tests over [[metabase-enterprise.sandbox.corpus-test-dataset]]: every role a sandboxed table
  can play, under every kind of sandbox, judged by detectors that need no expected answer per case.

  Each probe is a legacy-MBQL inner query built from an ids map, so the same probe renders against two databases:

  * the base dataset, queried through the dataset API as the sandboxed user `:rasta`;
  * a restricted variant of the dataset with the sandbox applied by hand, queried as an admin.

  Detectors, kept separate and all recorded on every result:

  * `:differential` -- the sandboxed user's rows must equal the admin's rows on the restricted variant (a sandbox is
    a view), or the user's query must fail.
  * `:canary` -- no canary of a row the sandbox hides may appear anywhere in the user's response.
  * `:structural` -- after preprocessing as the user and alias resolution, no field ref outside a sandbox stage may
    have a sandboxed table as its physical source.

  See the dataset namespace for the schema, the canaries, and the restricted variants."
  (:require
   [clojure.data :as data]
   [clojure.string :as str]
   [clojure.test :refer :all]
   [clojure.test.check :as tc]
   [clojure.test.check.generators :as gen]
   [clojure.test.check.properties :as prop]
   [clojure.walk :as walk]
   [metabase-enterprise.sandbox.api.util :as sandbox.api.util]
   [metabase-enterprise.sandbox.corpus-test-dataset :as corpus]
   [metabase-enterprise.sandbox.query-processor.middleware.sandboxing :as sandboxing]
   [metabase-enterprise.sandbox.test-util :as met]
   [metabase.driver :as driver]
   [metabase.driver.util :as driver.u]
   [metabase.legacy-mbql.normalize :as mbql.normalize]
   [metabase.lib.convert :as lib.convert]
   [metabase.lib.core :as lib]
   [metabase.lib.metadata :as lib.metadata]
   [metabase.lib.test-util.generators :as lib.tu.gen]
   [metabase.lib.walk :as lib.walk]
   [metabase.permissions.core :as perms]
   [metabase.permissions.models.data-permissions :as data-perms]
   [metabase.query-processor.middleware.enterprise :as qp.middleware.enterprise]
   [metabase.query-processor.preprocess :as qp.preprocess]
   [metabase.query-processor.setup :as qp.setup]
   [metabase.query-processor.util.add-alias-info :as add]
   [metabase.sync.core :as sync]
   [metabase.test :as mt]
   [metabase.test.util :as tu]
   [metabase.test.util.random :as tu.rng]
   [metabase.util :as u]
   [metabase.util.json :as json]
   [metabase.util.malli.fn :as mu.fn]
   [metabase.util.match :as match]
   [toucan2.core :as t2]))

(set! *warn-on-reflection* true)

;;; ---------------------------------------------------- Ids -------------------------------------------------------

(def ^:private tables [:regions :shops :orders :items])

(def ^:private columns
  {:regions [:id :name :tier :hq_shop_id :secret :secret_num]
   :shops   [:id :name :tier :price :region_id :parent_shop_id :rating :opened_on :secret :secret_num]
   :orders  [:id :name :shop_id :return_shop_id :amount]
   :items   [:id :name :order_id :qty]})

(defn- ids-map
  "Every table, field and card id of the current database, by name, plus the physical column names."
  [cards]
  {:db     (mt/id)
   :tables (into {} (for [t tables] [t (mt/id t)]))
   :fields (into {} (for [[t cols] columns, c cols] [[t c] (mt/id t c)]))
   :names  (into {} (for [[t cols] columns, c cols] [[t c] (mt/format-name (name c))]))
   :cards  cards})

(defn- tid [ids table] (get-in ids [:tables table]))
(defn- fid [ids table column] (get-in ids [:fields [table column]]))
(defn- fname [ids table column] (get-in ids [:names [table column]]))
(defn- card-source [ids card-k] (str "card__" (get-in ids [:cards card-k])))

;;; -------------------------------------------------- Sandboxes ---------------------------------------------------

(defn- tier-remapping [table]
  {"tier" ["variable" [:field (mt/id table :tier) nil]]})

(defn- tier-dimension-remapping [table]
  {"tier" ["dimension" [:field (mt/id table :tier) nil]]})

(defn- shops-table-gtap []
  {:shops {:remappings (tier-remapping :shops)}})

(defn- shops-card-gtap []
  {:shops {:query (mt/mbql-query shops) :remappings (tier-dimension-remapping :shops)}})

(defn- shops-card-join-gtap
  "A sandbox card that joins REGIONS and filters on the region's tier; returns only SHOPS columns."
  []
  {:shops {:query      (mt/mbql-query shops
                         {:joins  [{:source-table $$regions
                                    :alias        "R"
                                    :condition    [:= $region_id &R.regions.id]
                                    :fields       :none}]
                          :fields [$id $name $tier $price $region_id $parent_shop_id $rating $opened_on $secret $secret_num]})
           :remappings {"tier" ["dimension" [:field (mt/id :regions :tier) {:join-alias "R"}]]}}})

(defn- shops-card-extra-column-gtap
  "A sandbox card that also returns a REGIONS column, which the sandbox drops in an appended stage."
  []
  {:shops {:query      (mt/mbql-query shops
                         {:joins  [{:source-table $$regions
                                    :alias        "R"
                                    :condition    [:= $region_id &R.regions.id]
                                    :fields       [&R.regions.name]}]})
           :remappings (tier-dimension-remapping :shops)}})

(defn- shops-rating-gtap
  "A table sandbox on the Float column RATING, so the attribute is coerced to a double."
  []
  {:shops {:remappings {"rating" ["variable" [:field (mt/id :shops :rating) nil]]}}})

(defn- persist-sandbox-card!
  "Turn the SHOPS sandbox card into a model, so that it is persisted along with the other models."
  []
  (t2/update! :model/Card (t2/select-one-fn :card_id :model/Sandbox :table_id (mt/id :shops)) {:type :model}))

(defn- shops-hide-price-gtap []
  {:shops {:query (mt/mbql-query shops {:fields [$id $name $tier $region_id $parent_shop_id]})}})

(defn- shops-rows+hide-price-gtap []
  {:shops {:query      (mt/mbql-query shops {:fields [$id $name $tier $region_id $parent_shop_id]})
           :remappings (tier-dimension-remapping :shops)}})

(defn- native-shops-gtap
  "A native sandbox card on SHOPS with a `{{tier}}` template tag. `sql` is a format string over `[[mt/format-name]]`d
  `names`; unquoted identifiers, which every supported driver resolves in its own case."
  [sql & names]
  {:shops {:query      (mt/native-query {:query         (apply format sql (map mt/format-name names))
                                         :template-tags {"tier" {:name         "tier"
                                                                 :display-name "tier"
                                                                 :type         :number
                                                                 :required     true}}})
           :remappings {"tier" ["variable" ["template-tag" "tier"]]}}})

(defn- shops-native-gtap []
  (native-shops-gtap "SELECT * FROM %s WHERE %s = {{tier}}" "shops" "tier"))

(defn- shops-native-extra-column-gtap
  "A native sandbox card that also returns a REGIONS column, which the sandbox drops in an appended stage."
  []
  (native-shops-gtap "SELECT s.*, r.%s AS %s FROM %s s LEFT JOIN %s r ON s.%s = r.%s WHERE s.%s = {{tier}}"
                     "name" "region_name" "shops" "regions" "region_id" "id" "tier"))

(defn- shops-native-hide-price-gtap []
  (native-shops-gtap "SELECT %s, %s, %s, %s, %s FROM %s WHERE %s = {{tier}}"
                     "id" "name" "tier" "region_id" "parent_shop_id" "shops" "tier"))

(defn- add-rasta-to-group-with-view-data!
  "Put `:rasta` in a fresh group whose view-data on the current database is `level`. Granting query access too would
  raise view-data back to `:unrestricted`, so only an unrestricted group gets it."
  [level]
  (let [group (t2/insert-returning-instance! :model/PermissionsGroup {:name (str "corpus-" (name level) "-" (random-uuid))})]
    (perms/add-user-to-group! (mt/user->id :rasta) group)
    (data-perms/set-database-permission! group (mt/id) :perms/view-data level)
    (when (= level :unrestricted)
      (data-perms/set-database-permission! group (mt/id) :perms/create-queries :query-builder))
    group))

(def ^:private secret-num-labels
  "A value remap of SHOPS.SECRET_NUM whose labels carry each row's text canary, so a label of a hidden row anywhere in
  a response is a leak."
  (into {} (for [id (range 1 9)] [(corpus/canary-num :shops id) (str "label " (corpus/canary-text :shops id))])))

(def ^:private sandbox-configs
  "Config name -> how VENUES-style sandboxing is set up on SHOPS (and REGIONS):

  * `:gtaps`       fn returning the `met/with-gtaps!` map; evaluated inside the temp copy of the base database
  * `:attributes`  the user's login attributes
  * `:restricted`  the dataset variant that equals the sandbox applied by hand
  * `:expect`      `:restricted` (user == admin on restricted), `:unrestricted` (user == admin on base) or `:error`
  * `:hidden-cols` SHOPS columns the sandbox card leaves out
  * `:sandboxed`   tables with an enforced sandbox, for the structural detector
  * `:setup`       fn run inside `with-gtaps!` for extra arrangements (second groups)
  * `:remaps`      display remappings, set up in both databases; see [[do-with-remaps!]]
  * `:feature`     driver feature the config needs
  * `:persist?`    persist every model as an admin before probing
  * `:premium-features` features granted besides `:sandboxes`"
  (array-map
   :table-tier      {:gtaps shops-table-gtap           :attributes {"tier" "1"} :restricted #'corpus/sandbox-corpus-tier1}
   :card-rows       {:gtaps shops-card-gtap            :attributes {"tier" "1"} :restricted #'corpus/sandbox-corpus-tier1}
   :card-join       {:gtaps shops-card-join-gtap       :attributes {"tier" "1"} :restricted #'corpus/sandbox-corpus-region-tier1}
   ;; ratings are tier + 0.5, so rating 1.5 selects the tier 1 rows
   :table-rating    {:gtaps shops-rating-gtap :attributes {"rating" "1.5"} :restricted #'corpus/sandbox-corpus-tier1}
   ;; the sandbox card itself is a persisted model: its cache holds every row
   :card-rows+persisted-sandbox {:gtaps shops-card-gtap :attributes {"tier" "1"} :restricted #'corpus/sandbox-corpus-tier1
                                 :setup persist-sandbox-card! :persist? true :feature :persist-models}
   :card-extra-column {:gtaps shops-card-extra-column-gtap :attributes {"tier" "1"} :restricted #'corpus/sandbox-corpus-tier1}
   :card-hide-price {:gtaps shops-hide-price-gtap      :attributes {}           :restricted #'corpus/sandbox-corpus-hide-price
                     :hidden-cols #{:price :secret :secret_num}}
   :card-rows+hide  {:gtaps shops-rows+hide-price-gtap :attributes {"tier" "1"} :restricted #'corpus/sandbox-corpus-tier1-hide-price
                     :hidden-cols #{:price :secret :secret_num}}
   :native          {:gtaps shops-native-gtap          :attributes {"tier" "1"} :restricted #'corpus/sandbox-corpus-tier1
                     :feature :native-parameters}
   :native-extra-column {:gtaps shops-native-extra-column-gtap :attributes {"tier" "1"}
                         :restricted #'corpus/sandbox-corpus-tier1 :feature :native-parameters}
   :native-hide-price {:gtaps shops-native-hide-price-gtap :attributes {"tier" "1"}
                       :restricted #'corpus/sandbox-corpus-tier1-hide-price :feature :native-parameters
                       :hidden-cols #{:price :secret :secret_num}}
   :two-tables      {:gtaps (fn [] (merge (shops-table-gtap) {:regions {:remappings (tier-remapping :regions)}}))
                     :attributes {"tier" "1"} :restricted #'corpus/sandbox-corpus-tier1-both :sandboxed #{:shops :regions}}
   ;; ORDERS.SHOP_ID displays SHOPS.NAME: remaps add implicit joins to the sandboxed table in the outer query
   :table-tier+remap {:gtaps shops-table-gtap :attributes {"tier" "1"} :restricted #'corpus/sandbox-corpus-tier1
                      :remaps [[:orders :shop_id :shops :name]]}
   ;; every model persisted by an admin: the cache tables hold every row and column
   :table-tier+persisted {:gtaps shops-table-gtap :attributes {"tier" "1"} :restricted #'corpus/sandbox-corpus-tier1
                          :persist? true :feature :persist-models}
   :card-hide-price+persisted {:gtaps shops-hide-price-gtap :attributes {} :restricted #'corpus/sandbox-corpus-hide-price
                               :hidden-cols #{:price :secret :secret_num} :persist? true :feature :persist-models}
   ;; SHOPS.SECRET_NUM displays admin-entered labels, one per row, from FieldValues of the whole table
   :table-tier+value-remap {:gtaps shops-table-gtap :attributes {"tier" "1"} :restricted #'corpus/sandbox-corpus-tier1
                            :remaps [[:shops :secret_num secret-num-labels]]}
   ;; another group grants unrestricted access, so the sandbox is not enforced (by design)
   :cancelled       {:gtaps shops-table-gtap :attributes {"tier" "1"} :restricted #'corpus/sandbox-corpus :expect :unrestricted
                     :sandboxed #{} :setup #(add-rasta-to-group-with-view-data! :unrestricted)}
   ;; another group blocks the database; the sandbox must still apply
   :plus-blocked    {:gtaps shops-table-gtap :attributes {"tier" "1"} :restricted #'corpus/sandbox-corpus-tier1
                     :setup #(add-rasta-to-group-with-view-data! :blocked)}
   ;; another group impersonates a database role: sandbox and impersonation on one database conflict, an error
   :plus-impersonation {:gtaps shops-table-gtap :attributes {"tier" "1" "role" "corpus_role"}
                        :restricted #'corpus/sandbox-corpus-tier1 :expect :error
                        :feature :connection-impersonation :premium-features #{:advanced-permissions}
                        :setup (fn []
                                 (let [group (add-rasta-to-group-with-view-data! :unrestricted)]
                                   (t2/insert! :model/ConnectionImpersonation {:db_id     (mt/id)
                                                                               :group_id  (:id group)
                                                                               :attribute "role"})))}
   ;; two sandboxed groups on one table is an error, and must stay one
   :two-sandboxes   {:gtaps shops-table-gtap :attributes {"tier" "1"} :restricted #'corpus/sandbox-corpus-tier1 :expect :error
                     :setup (fn []
                              (let [group (add-rasta-to-group-with-view-data! :unrestricted)]
                                (t2/insert! :model/Sandbox {:group_id             (:id group)
                                                            :table_id             (mt/id :shops)
                                                            :attribute_remappings (tier-remapping :shops)})))}))

(def ^:private attribute-variants
  "Variant name -> a base config from [[sandbox-configs]] run with other login attributes. Attribute values reach the
  sandbox as parameter values; JWT attributes merge into login attributes, so numbers and lists can arrive too."
  (array-map
   :attr-leading-zero {:base :table-tier :attributes {"tier" "01"}}
   :attr-number       {:base :table-tier :attributes {"tier" 1}}
   :attr-padded       {:base :table-tier :attributes {"tier" " 1"}       :expect :error}
   :attr-decimal      {:base :table-tier :attributes {"tier" "1.0"}      :expect :error}
   :attr-sql-like     {:base :table-tier :attributes {"tier" "1 OR 1=1"} :expect :error}
   :attr-empty        {:base :table-tier :attributes {"tier" ""}         :expect :error}
   :attr-missing      {:base :table-tier :attributes {}                  :expect :error}
   :attr-wrong-case   {:base :table-tier :attributes {"Tier" "1"}        :expect :error}
   ;; no shop has tier 3, so a list that lifts the filter shows as extra rows
   :attr-list         {:base :table-tier :attributes {"tier" ["3"]}      :expect :error}
   :attr-map          {:base :table-tier :attributes {"tier" {"a" "1"}}  :expect :error}
   :card-sql-like     {:base :card-rows  :attributes {"tier" "1 OR 1=1"} :expect :error}
   :card-list         {:base :card-rows  :attributes {"tier" ["3"]}      :expect :error}
   ;; a native sandbox fails safe on a list: its `{{tier}}` matches nothing
   :native-list       {:base :native     :attributes {"tier" ["3"]}      :restricted #'corpus/sandbox-corpus-no-shops}
   :rating-integer    {:base :table-rating :attributes {"rating" "1"}   :restricted #'corpus/sandbox-corpus-no-shops}
   :rating-junk       {:base :table-rating :attributes {"rating" "1.5x"} :expect :error}
   :rating-list       {:base :table-rating :attributes {"rating" ["2.5"]} :expect :error}
   :native-sql-like   {:base :native     :attributes {"tier" "1 OR 1=1"} :expect :error}
   :native-missing    {:base :native     :attributes {}                  :expect :error}))

(defn- corpus-config
  "The config named `config-k`, from [[sandbox-configs]] or [[attribute-variants]]."
  [config-k]
  (or (sandbox-configs config-k)
      (let [{:keys [base] :as variant} (attribute-variants config-k)]
        (merge (sandbox-configs base) (dissoc variant :base)))))

(defn- config-sandboxed-tables [config-k]
  (let [config (corpus-config config-k)]
    (if (contains? config :sandboxed) (:sandboxed config) #{:shops})))

;;; ---------------------------------------------- Cards and harness -----------------------------------------------

(defn- model-metadata
  "Result metadata for `query`, computed as an admin, so a model has what a saved one would."
  [query]
  (-> (mt/user-http-request :crowberto :post 202 "dataset" query)
      (get-in [:data :results_metadata :columns])))

(defn- do-with-cards!
  "Create the source cards the probes use in the current database and call `(f cards)` with card key -> id."
  [f]
  (let [shops-q  (mt/mbql-query shops)
        orders-q (mt/mbql-query orders {:fields [$id $name $shop_id $shop_id->shops.name $shop_id->shops.price]})]
    (mt/with-temp [:model/Card {shops-card :id}   {:dataset_query shops-q}
                   :model/Card {shops-model :id}  {:dataset_query shops-q :type :model :result_metadata (model-metadata shops-q)}
                   :model/Card {orders-card :id}  {:dataset_query orders-q}
                   :model/Card {orders-model :id} {:dataset_query orders-q :type :model :result_metadata (model-metadata orders-q)}
                   :model/Card {native-card :id}  {:dataset_query (mt/native-query {:query (format "SELECT * FROM %s"
                                                                                                   (mt/format-name "shops"))})}]
      (f {:native/shops          native-card
          :card/shops            shops-card
          :model/shops           shops-model
          :card/orders-implicit  orders-card
          :model/orders-implicit orders-model}))))

(defn- do-with-maybe-persisted-cards!
  "[[do-with-cards!]], with every model persisted by an admin first when `persist?`. The cache tables hold unsandboxed
  rows, so a sandboxed user's query must never read them."
  [persist? f]
  (if-not persist?
    (do-with-cards! f)
    (mt/with-persistence-enabled! [persist-models!]
      (do-with-cards! (fn [cards]
                        (mt/with-test-user :crowberto
                          (persist-models!))
                        (assert (every? #{"persisted"} (t2/select-fn-vec :state :model/PersistedInfo :database_id (mt/id)))
                                "every model persisted")
                        (f cards))))))

(defn- allowed-canary-ids
  "The canaries in the rows an admin gets from the current (restricted) database. Rows only: response metadata can
  carry values of the whole table (a value remap's list, for one), which would hide a leak."
  []
  (into #{}
        (mapcat (fn [table]
                  (corpus/canary-ids
                   (get-in (mt/user-http-request :crowberto :post 202 "dataset"
                                                 {:database (mt/id) :type "query" :query {:source-table (mt/id table)}})
                           [:data :rows]))))
        [:shops :regions]))

(defonce ^:private fully-synced-dbs (atom #{}))

(defn- fully-synced!
  "Give the current database the fingerprints and field values a real instance has, once per JVM. The dataset loader
  only syncs the schema, and binning needs fingerprints."
  []
  (let [db (mt/db)]
    (when-not (contains? @fully-synced-dbs (:id db))
      (sync/sync-database! db {:scan :full})
      (swap! fully-synced-dbs conj (:id db)))))

(defn- config-supported? [config-k]
  (if-let [feature (:feature (corpus-config config-k))]
    (driver.u/supports? driver/*driver* feature (mt/db))
    true))

(defn- do-with-remaps!
  "Call `thunk` with the config's display remappings in place for the current database: an FK remap
  `[table column target-table target-column]`, or a value remap `[table column {value label}]`."
  [remaps thunk]
  (tu/do-with-column-remappings!
   (into {} (for [[table column target-table target-column] remaps]
              [(mt/id table column) (if (map? target-table) target-table (mt/id target-table target-column))]))
   thunk))

(defn- do-with-corpus-config!
  "Load the base dataset and the config's restricted variant, create the cards in both, set up the sandbox on a temp
  copy of the base database as `:rasta`, and call `(f ctx)` inside it. `ctx` has `:ids` for the base copy,
  `:restricted-ids` for the restricted database, `:allowed` canaries, and the config's fields. `base-dataset` (default
  the corpus) is the dataset the sandboxed user queries."
  ([config-k f]
   (do-with-corpus-config! config-k #'corpus/sandbox-corpus f))
  ([config-k base-dataset f]
   (let [{:keys [gtaps attributes restricted setup expect hidden-cols remaps persist? premium-features]
          :or   {expect :restricted}} (corpus-config config-k)
         in-sandbox-copy (fn [base restricted-ids allowed]
                           (mt/with-db base
                             (met/with-gtaps! {:gtaps (gtaps) :attributes attributes}
                               (doseq [table tables]
                                 (data-perms/set-table-permission! &group (mt/id table) :perms/create-queries :query-builder))
                               (data-perms/set-database-permission! &group (mt/id) :perms/download-results :one-million-rows)
                               (when setup (setup))
                               (mt/with-additional-premium-features (or premium-features #{})
                                 (do-with-remaps!
                                  remaps
                                  (fn []
                                    (do-with-maybe-persisted-cards!
                                     persist?
                                     (fn [cards]
                                       (f {:config         config-k
                                           :expect         expect
                                           :hidden-cols    (or hidden-cols #{})
                                           :sandboxed      (config-sandboxed-tables config-k)
                                           :ids            (ids-map cards)
                                           :restricted-ids restricted-ids
                                           :allowed        allowed})))))))))]
     (mt/dataset (var-get base-dataset)
       (corpus/ensure-fk-metadata!)
       (fully-synced!)
       (let [base (mt/db)]
         (mt/dataset (var-get restricted)
           (corpus/ensure-fk-metadata!)
           (fully-synced!)
           (do-with-remaps! remaps
                            (fn []
                              (do-with-cards!
                               (fn [restricted-cards]
                                 (in-sandbox-copy base (ids-map restricted-cards) (allowed-canary-ids))))))))))))

;;; ------------------------------------------------ Schema checks -------------------------------------------------

(defn do-without-schema-checks!
  "Call `thunk` with `mu/defn` input and output checks off. Production does not compile those checks in, so a query
  that fails closed on a schema check in a test can fail open in production."
  [thunk]
  ;; the root value, not a binding: API requests run on other threads
  (alter-var-root #'mu.fn/*enforce* (constantly false))
  (try
    (thunk)
    (finally
      (alter-var-root #'mu.fn/*enforce* (constantly true)))))

(def ^:private schema-check-modes
  "Mode name -> fn of a thunk that runs it in that mode."
  (array-map
   :test-mode       (fn [thunk] (thunk))
   :production-mode do-without-schema-checks!))

;;; -------------------------------------------------- Detectors ---------------------------------------------------

(defn- legacy-outer
  "The legacy outer query for a probe's inner query and parameters against `ids`' database."
  [ids {:keys [query parameters]}]
  (cond-> {:database (:db ids) :type "query" :query query}
    parameters (assoc :parameters parameters)))

(defn- legacy->mbql5
  "`outer`, a legacy query as a client would send it, converted to MBQL 5 the way a client using Lib would send it."
  [outer]
  (-> (json/decode+kw (json/encode outer))
      mbql.normalize/normalize
      lib.convert/->mbql5
      (dissoc :lib.convert/converted?)))

(defn- legacy-ref-order
  "Every MBQL 5 `[:field opts id-or-name]` in `query` turned into the legacy order `[:field id-or-name opts]`: an MBQL 5
  query carrying legacy-shaped refs, which no client produces."
  [query]
  (walk/postwalk (fn [x]
                   (if (and (vector? x) (= 3 (count x)) (#{:field "field"} (first x)) (map? (second x)))
                     [(first x) (nth x 2) (second x)]
                     x))
                 query))

(def ^:private query-formats
  "Format name -> fn turning a legacy outer query into the request body sent in that format."
  (array-map
   :legacy      identity
   :mbql5       legacy->mbql5
   :mbql5-mixed (comp legacy-ref-order legacy->mbql5)))

(defn- run-as
  "Run a probe (its `:outer` request body, or its legacy inner query and parameters) through the dataset API as `user`
  against `ids`' database. Returns `:rows`, `:cols` (names) and `:body`, or `:error` and `:body`."
  [user ids probe]
  (let [outer                 (or (:outer probe) (legacy-outer ids probe))
        {:keys [status body]} (mt/user-http-request-full-response user :post "dataset" outer)]
    (if (and (= 202 status) (= "completed" (:status body)))
      {:rows (get-in body [:data :rows])
       :cols (mapv :name (get-in body [:data :cols]))
       :body body}
      {:error (u/truncate (str status " " (or (:error body) (:message body) body)) 300)
       :body  body})))

(defn- project
  "The rows of `result` restricted to `col-names`, as a multiset."
  [{:keys [rows cols]} col-names]
  (let [idxs (keep #(let [i (.indexOf ^java.util.List cols %)] (when (nat-int? i) i)) col-names)]
    (frequencies (map (fn [row] (mapv #(nth row %) idxs)) rows))))

(defn- differential-verdict
  "Compare the sandboxed user's result with the expected one (an admin on the restricted variant, or on the base
  database when nothing should be restricted). `admin-base` tells whether the probe could tell restricted data from
  unrestricted data at all."
  [{:keys [expect hidden-cols] :as ctx} user expected admin-base]
  (let [hidden-names (set (map name hidden-cols))]
    (cond
      ;; a probe that cannot tell the restricted data from the rest may succeed; one that can must fail
      (= expect :error)                   (if (:error user)
                                            :user-error
                                            (let [v (differential-verdict (assoc ctx :expect :restricted) user expected admin-base)]
                                              (if (= v :consistent) :missing-error v)))
      (= expect :unrestricted)            (cond
                                            (and (:error expected) (:error user)) :rejected
                                            (:error user)                         :over-restricted
                                            (= (project user (:cols expected)) (project expected (:cols expected))) :consistent
                                            :else                                 :over-restricted)
      (and (:error expected) (:error user)) :rejected
      (:error expected)                   :expected-error
      (:error user)                       :user-error
      :else
      (let [common    (filter (set (:cols expected)) (:cols user))
            user-m    (project user common)
            expected-m (project expected common)]
        (cond
          ;; a column the sandbox hides must never carry a value for the user
          (some (fn [col] (and (hidden-names col)
                               (some some? (map #(nth % (.indexOf ^java.util.List (:cols user) col)) (:rows user)))))
                (:cols user))
          :extra-rows

          (= user-m expected-m)
          (if (= (project admin-base common) expected-m) :vacuous :consistent)

          (some (fn [[row n]] (> n (get expected-m row 0))) user-m)
          :extra-rows

          :else
          :missing-rows)))))

(defn- canary-verdict [{:keys [allowed]} user]
  (let [leaked (into (sorted-set) (remove allowed) (corpus/canary-ids (:body user)))]
    (if (seq leaked) {:verdict :leak :leaked leaked} {:verdict :clean})))

(defn- raw-refs-to-tables
  "Field refs in the user's query, outside any sandbox stage, whose physical source after alias resolution is one of
  `table-ids`. Runs as the current user. `{:refs [...]}` or `{:error msg}` when the query does not preprocess."
  [ids probe table-ids]
  (let [outer (json/decode+kw (json/encode (or (:outer probe) (legacy-outer ids probe))))
        acc   (volatile! [])]
    (try
      (qp.setup/with-qp-setup [outer outer]
        (lib.walk/walk-stages
         (-> outer qp.preprocess/preprocess add/add-alias-info)
         (fn [_query path stage]
           (when-not (:query-permissions/sandboxed-table stage)
             ;; a stage still reading the raw table is the plainest failure of the rewrite
             (when (contains? table-ids (:source-table stage))
               (vswap! acc conj {:path path :unmarked-source-table (:source-table stage)}))
             (let [own (-> stage
                           (dissoc :lib/stage-metadata)
                           (update :joins (partial mapv #(dissoc % :stages :lib/stage-metadata))))]
               (vswap! acc into
                       (map (fn [ref] {:path path :ref ref}))
                       (match/match-many own
                         [:field (opts :guard (contains? table-ids (::add/source-table opts))) _] &match))))
           nil)))
      {:refs @acc}
      (catch Throwable e
        {:error (u/truncate (ex-message e) 300)}))))

(defn- structural-verdict [{:keys [ids sandboxed]} probe]
  (let [table-ids (into #{} (map #(tid ids %)) sandboxed)
        {:keys [refs error]} (raw-refs-to-tables ids probe table-ids)]
    (cond
      error      {:verdict :error :error error}
      (seq refs) {:verdict :raw-ref :refs refs}
      :else      {:verdict :clean})))

;;; ------------------------------------------- Detector cross-checks ----------------------------------------------

(defn detector-disagreements
  "Results where two detectors contradict each other. A non-empty answer is a bug in a detector, not in sandboxing:
  the differential oracle saying the rows are right while a canary of a hidden row got through, or the rows holding
  extra data from a canary column without the canary scan noticing."
  [results]
  (for [{:keys [differential canary user probe config] :as r} results
        :let [secret-col? (some #(str/includes? (u/lower-case-en (str %)) "secret") (:cols user))
              reason (cond
                       (and (#{:consistent :vacuous} differential) (= :leak (:verdict canary)))
                       :rows-right-but-canary-leaked

                       (and (= :extra-rows differential) secret-col? (= :clean (:verdict canary)))
                       :extra-rows-with-canary-column-but-no-canary)]
        :when reason]
    {:config config :probe probe :reason reason :result (select-keys r [:differential :canary :user])}))

(defn- scan-paths
  "Seq of `[path value]` for every scalar in `x`."
  [x]
  (let [acc (volatile! [])]
    (letfn [(scan [path v]
              (cond
                (map? v)        (doseq [[k child] v] (scan (conj path k) child))
                (sequential? v) (doall (map-indexed (fn [i child] (scan (conj path i) child)) v))
                :else           (vswap! acc conj [path v])))]
      (scan [] x))
    @acc))

(def ^:private sql-text-pattern #"(?i)\bSELECT\b|\bFROM\b.*\bJOIN\b")

(defn- hygiene-verdict
  "What a sandboxed user's response must never carry, wherever it sits: a fingerprint, a native or preprocessed form,
  internal query stages, the physical name of a sandboxed table, or SQL text in an error. `:json_query` echoes the
  user's own input and is skipped."
  [{:keys [sandboxed]} user]
  (let [body        (let [b (:body user)] (if (map? b) (dissoc b :json_query :dataset_query) b))
        table-names (into #{} (map #(mt/format-name (name %))) sandboxed)
        problems    (concat
                     (for [[path v] (scan-paths body)
                           :when (and (some? v)
                                      (or (some #{:fingerprint "fingerprint"} path)
                                          (some #{:native :preprocessed :native_form "native_form"} path)))]
                       {:kind :internal-metadata :path path})
                     (for [[path v] (scan-paths body)
                           :when (and (string? v)
                                      (or (some #(str/includes? v (str "\"" % "\"")) table-names)
                                          (re-find sql-text-pattern v)))]
                       {:kind :sql-text :path path :value (u/truncate v 120)})
                     (for [[path _] (scan-paths body)
                           :when (some #{:stages "stages"} path)]
                       {:kind :internal-query :path path}))
        problems    (vec (distinct (map #(update % :path (partial take 4)) problems)))]
    (if (seq problems) {:verdict :dirty :problems (vec (take 6 problems))} {:verdict :clean})))

(def ^:private compared-col-keys [:base_type :effective_type :semantic_type :binning_info])

(defn- metadata-diff-verdict
  "Compare the result columns the user got with the expected ones, by name, on the keys that describe a column's
  type and binning. Differences point at metadata that was merged in from the unsandboxed query."
  [user expected]
  (if (or (:error user) (:error expected))
    {:verdict :skipped}
    (let [by-name (fn [{:keys [body]}] (into {} (map (juxt :name #(select-keys % compared-col-keys))) (get-in body [:data :cols])))
          u       (by-name user)
          e       (by-name expected)
          diffs   (for [col (keys u) :when (and (contains? e col) (not= (u col) (e col)))]
                    {:col col :user (u col) :expected (e col)})]
      (if (seq diffs) {:verdict :differs :diffs (vec diffs)} {:verdict :same}))))

(defn- outcome
  "One keyword for a result, worst first, from its separate verdicts."
  [{:keys [differential canary structural metadata-diff]}]
  (cond
    (= :leak (:verdict canary))         :canary-leak
    (= :raw-ref (:verdict structural))  :raw-ref
    ;; bin bounds come from fingerprints; bounds that differ from the restricted data's reveal the hidden range
    (some (fn [{u :user e :expected}]
            (let [bounds #(select-keys (:binning_info %) [:min_value :max_value])]
              (and (seq (bounds u)) (seq (bounds e)) (not= (bounds u) (bounds e)))))
          (:diffs metadata-diff))
    :binning-bounds-leak
    :else                               differential))

(def ^:private failing-outcomes
  #{:canary-leak :raw-ref :binning-bounds-leak :extra-rows :missing-rows :missing-error :over-restricted})

(defn- render-probe
  "`probe` with `:outer`, its request body in query format `fmt`, or nil when it cannot be rendered in that format."
  [fmt ids probe]
  (if (= fmt :legacy)
    probe
    (try
      (assoc probe :outer ((query-formats fmt) (legacy-outer ids probe)))
      (catch Throwable _
        nil))))

(defn- run-probe*
  "[[run-probe]] with the rendered `probe`."
  [{:keys [ids restricted-ids expect] :as ctx} probe-name probe-fn probe]
  (let [user        (run-as :rasta ids probe)
        admin-base  (run-as :crowberto ids probe)
        expected    (if (= expect :unrestricted)
                      admin-base
                      (run-as :crowberto restricted-ids (probe-fn restricted-ids)))
        verdicts    {:differential  (differential-verdict ctx user expected admin-base)
                     :canary        (canary-verdict ctx user)
                     :structural    (structural-verdict ctx probe)
                     :hygiene       (hygiene-verdict ctx user)
                     :metadata-diff (metadata-diff-verdict user expected)}]
    (merge verdicts
           {:config   (:config ctx)
            :probe    probe-name
            :outcome  (outcome verdicts)
            :user     (-> user (dissoc :body) (update :rows #(some->> % (take 5))))
            :expected (-> expected (dissoc :body) (update :rows #(some->> % (take 5))))
            :format   (:format ctx :legacy)
            :query    probe})))

(defn- run-probe
  "Run `probe-fn`, a fn of an ids map returning `{:query inner-query, :parameters ...}`, under `ctx`, sending the
  sandboxed user's query in `(:format ctx)` (default `:legacy`). Returns a result map with every detector's verdict
  and `:outcome`; `:unrenderable` when the probe has no form in that format."
  [{:keys [ids] fmt :format :or {fmt :legacy} :as ctx} probe-name probe-fn]
  (if-let [probe (render-probe fmt ids (probe-fn ids))]
    (run-probe* ctx probe-name probe-fn probe)
    {:config (:config ctx) :probe probe-name :format fmt :outcome :unrenderable}))

;;; ------------------------------------------------ Smoke corpus --------------------------------------------------

(defn- implicit [ids table fk target-table target-col]
  [:field (fid ids target-table target-col) {:source-field (fid ids table fk)}])

(defn- implicit-join-alias
  "The alias the QP gives the implicit join from ORDERS.SHOP_ID to SHOPS."
  [ids]
  (str (mt/format-name "shops") "__via__" (fname ids :orders :shop_id)))

(def ^:private canonical-probes
  "Probe name -> fn of an ids map returning the inner query. One query per role of SHOPS and per source kind, written
  the way the query builder would."
  (array-map
   :shops-source            (fn [ids] {:query {:source-table (tid ids :shops)}})
   :shops-count             (fn [ids] {:query {:source-table (tid ids :shops) :aggregation [[:count]]}})
   :orders-plain            (fn [ids] {:query {:source-table (tid ids :orders)}})
   :orders-fk-only          (fn [ids] {:query {:source-table (tid ids :orders) :fields [[:field (fid ids :orders :shop_id) nil]]}})
   :orders-implicit-price   (fn [ids] {:query {:source-table (tid ids :orders)
                                               :fields       [[:field (fid ids :orders :id) nil]
                                                              (implicit ids :orders :shop_id :shops :price)]}})
   :orders-second-fk        (fn [ids] {:query {:source-table (tid ids :orders)
                                               :fields       [[:field (fid ids :orders :id) nil]
                                                              (implicit ids :orders :return_shop_id :shops :price)]}})
   :orders-both-fks         (fn [ids] {:query {:source-table (tid ids :orders)
                                               :fields       [(implicit ids :orders :shop_id :shops :secret)
                                                              (implicit ids :orders :return_shop_id :shops :secret)]}})
   :orders-explicit-join    (fn [ids] {:query {:source-table (tid ids :orders)
                                               :joins        [{:source-table (tid ids :shops)
                                                               :alias        "S"
                                                               :condition    [:= [:field (fid ids :orders :shop_id) nil]
                                                                              [:field (fid ids :shops :id) {:join-alias "S"}]]
                                                               :fields       :all}]}})
   :items-two-hops          (fn [ids] {:query {:source-table (tid ids :items)
                                               :joins        [{:source-table (tid ids :orders)
                                                               :alias        "O"
                                                               :condition    [:= [:field (fid ids :items :order_id) nil]
                                                                              [:field (fid ids :orders :id) {:join-alias "O"}]]
                                                               :fields       :none}]
                                               :fields       [[:field (fid ids :items :id) nil]
                                                              [:field (fid ids :shops :price) {:source-field            (fid ids :orders :shop_id)
                                                                                               :source-field-join-alias "O"}]]}})
   :shops-self-join         (fn [ids] {:query {:source-table (tid ids :shops)
                                               :fields       [[:field (fid ids :shops :id) nil]
                                                              (implicit ids :shops :parent_shop_id :shops :secret)]}})
   :shops-out-to-regions    (fn [ids] {:query {:source-table (tid ids :shops)
                                               :fields       [[:field (fid ids :shops :id) nil]
                                                              (implicit ids :shops :region_id :regions :secret)]}})
   :regions-into-shops      (fn [ids] {:query {:source-table (tid ids :regions)
                                               :fields       [[:field (fid ids :regions :id) nil]
                                                              (implicit ids :regions :hq_shop_id :shops :secret)]}})
   :card-shops              (fn [ids] {:query {:source-table (card-source ids :card/shops)}})
   :model-shops             (fn [ids] {:query {:source-table (card-source ids :model/shops)}})
   :card-orders-implicit    (fn [ids] {:query {:source-table (card-source ids :card/orders-implicit)}})
   :model-orders-implicit   (fn [ids] {:query {:source-table (card-source ids :model/orders-implicit)}})
   :model-shops-count       (fn [ids] {:query {:source-table (card-source ids :model/shops) :aggregation [[:count]]}})
   :nested-shops            (fn [ids] {:query {:source-query {:source-table (tid ids :shops)}}})
   :nested-shops-name-ref   (fn [ids] {:query {:source-query {:source-table (tid ids :shops)}
                                               :fields       [[:field (fname ids :shops :secret) {:base-type :type/Text}]]}})
   :parameter-on-shops      (fn [ids] {:query      {:source-table (tid ids :orders) :aggregation [[:count]]}
                                       :parameters [{:type   "number/!="
                                                     :target ["dimension" (implicit ids :orders :shop_id :shops :price)]
                                                     :value  [-1]}]})
   :orders-breakout-fk      (fn [ids] {:query {:source-table (tid ids :orders)
                                               :breakout     [[:field (fid ids :orders :shop_id) nil]]
                                               :aggregation  [[:count]]}})
   :orders-order-by-fk      (fn [ids] {:query {:source-table (tid ids :orders)
                                               :order-by     [[:asc [:field (fid ids :orders :shop_id) nil]]]}})
   :items-join-orders-all   (fn [ids] {:query {:source-table (tid ids :items)
                                               :joins        [{:source-table (tid ids :orders)
                                                               :alias        "O"
                                                               :condition    [:= [:field (fid ids :items :order_id) nil]
                                                                              [:field (fid ids :orders :id) {:join-alias "O"}]]
                                                               :fields       :all}]}})
   ;; a ref through RETURN_SHOP_ID that claims the name SHOP_ID gets the implicit join alias of SHOP_ID; the two
   ;; implicit joins collide and one is renamed
   :colliding-implicit-joins (fn [ids] {:query {:source-table (tid ids :orders)
                                                :fields       [[:field (fid ids :orders :id) nil]
                                                               (implicit ids :orders :shop_id :shops :price)
                                                               [:field (fid ids :shops :secret) {:source-field      (fid ids :orders :return_shop_id)
                                                                                                 :source-field-name (fname ids :orders :shop_id)}]]}})
   ;; SHOPS rows bucketed by month through the implicit join; each tier is its own month
   :orders-breakout-shop-month (fn [ids] {:query {:source-table (tid ids :orders)
                                                  :breakout     [[:field (fid ids :shops :opened_on) {:source-field  (fid ids :orders :shop_id)
                                                                                                      :temporal-unit :month}]]
                                                  :aggregation  [[:count]]}})
   ;; the next stage reads the month-bucketed column by name, so its unit is inherited
   :month-bucket-name-ref   (fn [ids] {:query {:source-query {:source-table (tid ids :shops)
                                                              :breakout     [[:field (fid ids :shops :opened_on) {:temporal-unit :month}]]
                                                              :aggregation  [[:sum [:field (fid ids :shops :secret_num) nil]]]}
                                               :fields       [[:field (fname ids :shops :opened_on) {:base-type :type/Date}]
                                                              [:field "sum" {:base-type :type/BigInteger}]]}})
   ;; a name ref "S__SECRET" that points into join S by the joined-column naming convention
   :joined-dunder-name-ref  (fn [ids] {:query {:source-table (tid ids :orders)
                                               :joins        [{:source-table (tid ids :shops)
                                                               :alias        "S"
                                                               :condition    [:= [:field (fid ids :orders :shop_id) nil]
                                                                              [:field (fid ids :shops :id) {:join-alias "S"}]]
                                                               :fields       :all}]
                                               :fields       [[:field (fid ids :orders :id) nil]
                                                              [:field (str "S__" (fname ids :shops :secret)) {:base-type :type/Text}]]}})
   ;; a name ref to an expression over a SHOPS column
   :expression-name-ref     (fn [ids] {:query {:source-table (tid ids :shops)
                                               :expressions  {"secret_copy" [:concat [:field (fid ids :shops :secret) nil] ""]}
                                               :fields       [[:field (fid ids :shops :id) nil]
                                                              [:field "secret_copy" {:base-type :type/Text}]]}})
   ;; NAME, NAME_2 and NAME_3 in one stage; the next stage reads NAME_3 by name
   :deduplicated-name-ref-3 (fn [ids] {:query {:source-query {:source-table (tid ids :orders)
                                                              :fields       [[:field (fid ids :orders :name) nil]
                                                                             (implicit ids :orders :shop_id :shops :name)
                                                                             (implicit ids :orders :return_shop_id :shops :name)]}
                                               :fields       [[:field (str (fname ids :shops :name) "_3") {:base-type :type/Text}]]}})
   ;; an explicit REGIONS join dressed up as the implicit join through ORDERS.SHOP_ID; the implicit ref must not reuse it
   :forged-implicit-join    (fn [ids] {:query {:source-table (tid ids :orders)
                                               :joins        [{:source-table          (tid ids :regions)
                                                               :alias                 (implicit-join-alias ids)
                                                               :condition             [:= [:field (fid ids :orders :shop_id) nil]
                                                                                       [:field (fid ids :regions :id) {:join-alias (implicit-join-alias ids)}]]
                                                               :fields                :none
                                                               :fk-field-id           (fid ids :orders :shop_id)
                                                               :qp/is-implicit-join   true}]
                                               :fields       [[:field (fid ids :orders :id) nil]
                                                              (implicit ids :orders :shop_id :shops :secret)]}})
   ;; the same explicit join without the forged keys: the implicit join's alias collides with it and must be renamed
   :colliding-join-alias    (fn [ids] {:query {:source-table (tid ids :orders)
                                               :joins        [{:source-table (tid ids :regions)
                                                               :alias        (implicit-join-alias ids)
                                                               :condition    [:= [:field (fid ids :orders :shop_id) nil]
                                                                              [:field (fid ids :regions :id) {:join-alias (implicit-join-alias ids)}]]
                                                               :fields       :none}]
                                               :fields       [[:field (fid ids :orders :id) nil]
                                                              (implicit ids :orders :shop_id :shops :secret)]}})
   ;; ORDERS.NAME and SHOPS.NAME in one stage come out as NAME and NAME_2; the next stage reads them by name
   :deduplicated-name-ref   (fn [ids] {:query {:source-query {:source-table (tid ids :orders)
                                                              :fields       [[:field (fid ids :orders :name) nil]
                                                                             (implicit ids :orders :shop_id :shops :name)
                                                                             (implicit ids :orders :shop_id :shops :secret)]}
                                               :fields       [[:field (str (fname ids :shops :name) "_2") {:base-type :type/Text}]
                                                              [:field (fname ids :shops :secret) {:base-type :type/Text}]]}})
   ;; a bogus join alias over a card source fails validation after the card's stage metadata is spliced in, and the
   ;; error text echoes that metadata, fingerprints included
   :model-bogus-join-alias  (fn [ids] {:query {:source-table (card-source ids :model/shops)
                                               :fields       [[:field (fid ids :shops :name) {:join-alias "Nope"}]]}})
   :card-bogus-join-alias   (fn [ids] {:query {:source-table (card-source ids :card/shops)
                                               :fields       [[:field (fid ids :shops :name) {:join-alias "Nope"}]]}})))

(def ^:private saved-sql-probes
  "Probes over a saved SQL question on SHOPS. Sandboxes do not apply to SQL results, by design: see \"You cannot
  secure the rows or columns of SQL results\" in docs/permissions/row-and-column-security.md."
  (array-map
   :native-card-source   (fn [ids] {:query {:source-table (card-source ids :native/shops)}})
   :native-card-name-ref (fn [ids] {:query {:source-table (card-source ids :native/shops)
                                            :fields       [[:field (fname ids :shops :secret) {:base-type :type/Text}]]}})))

(defn run-canonical!
  "Run the canonical probes (or those named by `probe-names`) under one config, or every config. Returns result maps."
  ([]
   (mapcat run-canonical! (keys sandbox-configs)))
  ([config-k]
   (run-canonical! config-k (keys canonical-probes)))
  ([config-k probe-names]
   (do-with-corpus-config!
    config-k
    (fn [ctx]
      (doall (for [probe-name probe-names]
               (run-probe ctx probe-name (canonical-probes probe-name))))))))

(defn run-canonical-formats!
  "Run the canonical probes (or those named by `probe-names`) under `config-k`, once per query format in `fmts`."
  ([config-k fmts]
   (run-canonical-formats! config-k fmts (keys canonical-probes)))
  ([config-k fmts probe-names]
   (do-with-corpus-config!
    config-k
    (fn [ctx]
      (doall (for [fmt fmts, probe-name probe-names]
               (run-probe (assoc ctx :format fmt) probe-name (canonical-probes probe-name))))))))

;;; ----------------------------------------------- Noninterference ------------------------------------------------
;;;
;;; Change only the data a sandbox hides; the sandboxed user's responses must not change.

(def ^:private varied-datasets
  "Restricted dataset -> the varied datasets that change only data it hides."
  {#'corpus/sandbox-corpus-tier1            [#'corpus/sandbox-corpus-varied-tier2]
   #'corpus/sandbox-corpus-tier1-both       [#'corpus/sandbox-corpus-varied-tier2]
   #'corpus/sandbox-corpus-hide-price       [#'corpus/sandbox-corpus-varied-hidden-cols]
   #'corpus/sandbox-corpus-tier1-hide-price [#'corpus/sandbox-corpus-varied-tier2 #'corpus/sandbox-corpus-varied-hidden-cols]})

(def ^:private masked-response-keys
  "Response keys whose values differ between two databases with the same data: ids, timings and the echoed query."
  #{:id :table_id :table-id :fk_target_field_id :fk-target-field-id :fk_field_id :fk-field-id :database_id :database
    :card-id :card_id :field-id :source-field :lib/card-id :lib/original-fk-field-id :source-table :source-card
    :field_ref :json_query :started_at :running_time
    :metabase.query-processor.middleware.add-remaps/original-field-dimension-id
    :metabase.query-processor.middleware.add-remaps/new-field-dimension-id})

(defn- mask-response
  "`body` without [[masked-response-keys]], with the ids in field refs replaced by `_`, and with the database id and
  query hash in SQL comments replaced by `_`."
  [body]
  (walk/postwalk (fn [x]
                   (cond
                     (map? x)                                        (apply dissoc x masked-response-keys)
                     (and (vector? x) (#{:field "field"} (first x))) (mapv #(if (int? %) '_ %) x)
                     (string? x)                                     (str/replace x #"(databaseID|queryHash): \w+" "$1: _")
                     :else                                           x))
                 body))

(defn- sandboxed-responses!
  "Probe name -> the sandboxed user's masked response to each canonical probe under `config-k`, on `dataset`."
  [config-k dataset]
  (do-with-corpus-config!
   config-k dataset
   (fn [{:keys [ids]}]
     (into {} (for [[probe-name probe-fn] canonical-probes]
                [probe-name (mask-response (:body (run-as :rasta ids (probe-fn ids))))])))))

(defn run-noninterference!
  "For `config-k`, each varied dataset paired with its restricted dataset, and each canonical probe, the sandboxed
  user's masked responses on the base and the varied dataset, as `{:config :varied :probe :base :varied-response}`."
  [config-k]
  (let [base (sandboxed-responses! config-k #'corpus/sandbox-corpus)]
    (doall (for [varied                (varied-datasets (:restricted (corpus-config config-k)))
                 :let                  [responses (sandboxed-responses! config-k varied)]
                 [probe-name response] base]
             {:config config-k :varied varied :probe probe-name
              :base response :varied-response (responses probe-name)}))))

(defn summarize
  "Config -> outcome -> count, for reading at the REPL."
  [results]
  (into (sorted-map)
        (map (fn [[config rs]] [config (frequencies (map :outcome rs))]))
        (group-by :config results)))

;;; ------------------------------------------------ Field-ref grid ------------------------------------------------
;;;
;;; Every way to write a ref to a SHOPS column, in every clause position, from every stage that can reach SHOPS.

(def ^:private edges
  "Edge name -> how a stage reaches SHOPS: the source table and the FK column, plus for the two-hop case the explicit
  join that brings the FK's table into scope."
  (array-map
   :orders/shop_id        {:source :orders  :fk-table :orders  :fk :shop_id}
   :orders/return_shop_id {:source :orders  :fk-table :orders  :fk :return_shop_id}
   :regions/hq_shop_id    {:source :regions :fk-table :regions :fk :hq_shop_id}
   :shops/parent_shop_id  {:source :shops   :fk-table :shops   :fk :parent_shop_id}
   :items->orders/shop_id {:source :items   :fk-table :orders  :fk :shop_id :via-join "O"}))

(defn- edge-joins
  "Explicit joins an edge's stage needs before the FK column is in scope."
  [ids {:keys [via-join]}]
  (when via-join
    [{:source-table (tid ids :orders)
      :alias        via-join
      :condition    [:= [:field (fid ids :items :order_id) nil] [:field (fid ids :orders :id) {:join-alias via-join}]]
      :fields       :none}]))

(defn- fk-opts [ids {:keys [fk-table fk via-join]}]
  (cond-> {:source-field (fid ids fk-table fk)}
    via-join (assoc :source-field-join-alias via-join)))

(def ^:private targets
  {:price      {:base-type :type/Integer    :param {:type "number/!=" :value [-1]}}
   :secret     {:base-type :type/Text       :param {:type "string/!=" :value ["__none__"]}}
   :rating     {:base-type :type/Float      :param {:type "number/!=" :value [-1]}}
   ;; the numeric canary: its hidden rows extend the column's range, which binning bounds would reveal
   :secret_num {:base-type :type/BigInteger :param {:type "number/!=" :value [-1]}}})

(def ^:private ref-shapes
  "Ref shape name -> fn of ids, edge and target returning a legacy ref to SHOPS.<target> from the edge's stage."
  (array-map
   :canonical                 (fn [ids e t] [:field (fid ids :shops t) (fk-opts ids e)])
   :legacy-fk->               (fn [ids e t] [:fk-> [:field-id (fid ids (:fk-table e) (:fk e))] [:field-id (fid ids :shops t)]])
   :name-ref+source-field     (fn [ids e t] [:field (fname ids :shops t) (merge {:base-type (get-in targets [t :base-type])} (fk-opts ids e))])
   :source-field-name-real    (fn [ids e t] [:field (fid ids :shops t) (assoc (fk-opts ids e) :source-field-name (fname ids (:fk-table e) (:fk e)))])
   :source-field-name-bogus   (fn [ids e t] [:field (fid ids :shops t) (assoc (fk-opts ids e) :source-field-name "NOPE")])
   :source-field-join-bogus   (fn [ids e t] [:field (fid ids :shops t) (assoc (fk-opts ids e) :source-field-join-alias "Nope")])
   :bare-id                   (fn [ids _ t] [:field (fid ids :shops t) nil])
   :bare-id+base-type         (fn [ids _ t] [:field (fid ids :shops t) {:base-type (get-in targets [t :base-type])}])
   :implicit-alias-no-join    (fn [ids e t] [:field (fid ids :shops t) {:join-alias (str (mt/format-name "shops") "__via__" (fname ids (:fk-table e) (:fk e)))}])
   :field-literal             (fn [ids _ t] [:field-literal (fname ids :shops t) (get-in targets [t :base-type])])
   :source-field-is-pk        (fn [ids e t] [:field (fid ids :shops t) {:source-field (fid ids (:fk-table e) :id)}])
   :source-field-from-shops   (fn [ids _ t] [:field (fid ids :shops t) {:source-field (fid ids :shops :region_id)}])
   ;; only ORDERS has a second FK into SHOPS; nil elsewhere, and the probe is skipped
   :source-field-other-fk     (fn [ids e t] (when (= :orders (:fk-table e))
                                              [:field (fid ids :shops t) {:source-field (fid ids :orders (if (= (:fk e) :shop_id) :return_shop_id :shop_id))}]))))

(defn- lib-binned
  "`r` with default binning, when it is a `:field` ref whose options can carry it."
  [[tag id-or-name opts :as r]]
  (if (and (= tag :field) ((some-fn nil? map?) opts))
    [tag id-or-name (assoc opts :binning {:strategy :default})]
    r))

(defn- grid-positions
  "Position name -> `{:query inner, :parameters ...}` for ids, edge, target and ref. Every query starts from the
  edge's source table with the edge's joins."
  [ids e t r]
  (let [src    (tid ids (:source e))
        base   (cond-> {:source-table src} (edge-joins ids e) (assoc :joins (edge-joins ids e)))
        canon  ((ref-shapes :canonical) ids e :name)
        own-id [:field (fid ids (:source e) :id) nil]]
    (array-map
     :fields            {:query (assoc base :fields [own-id r] :limit 50)}
     :breakout          {:query (assoc base :breakout [r] :aggregation [[:count]])}
     :filter            {:query (assoc base :aggregation [[:count]] :filter [:not-null r])}
     :aggregation       {:query (assoc base :aggregation [[:max r]])}
     :expression        {:query (assoc base :expressions {"x" [:coalesce r r]} :fields [own-id [:expression "x"]] :limit 50)}
     :case-expression   {:query (assoc base :expressions {"x" [:case [[[:not-null r] r]] {:default r}]} :fields [own-id [:expression "x"]] :limit 50)}
     :order-by          {:query (assoc base :fields [own-id r] :order-by [[:desc r]] :limit 50)}
     :mixed-after-canon {:query (assoc base :fields [canon r] :limit 50)}
     :mixed-before-canon {:query (assoc base :fields [r canon] :limit 50)}
     :second-stage      {:query {:source-query base :fields [r] :limit 50}}
     :explicit-join-present
     {:query (update base :joins (fnil conj [])
                     {:source-table (tid ids :shops)
                      :alias        "S"
                      :condition    [:= [:field (fid ids (:fk-table e) (:fk e)) (when (:via-join e) {:join-alias (:via-join e)})]
                                     [:field (fid ids :shops :id) {:join-alias "S"}]]
                      :fields       :none})
      :params-note "ref does not name the join"}
     :right-join-shops-preserved
     {:query (assoc base
                    :joins (conj (vec (:joins base))
                                 {:source-table (tid ids :shops)
                                  :alias        "S"
                                  :strategy     :right-join
                                  :condition    [:= [:field (fid ids (:fk-table e) (:fk e)) (when (:via-join e) {:join-alias (:via-join e)})]
                                                 [:field (fid ids :shops :id) {:join-alias "S"}]]
                                  :fields       :none})
                    :fields [own-id r [:field (fid ids :shops :secret) {:join-alias "S"}]]
                    :limit 50)}
     :join-condition
     {:query (assoc base
                    :joins (conj (vec (:joins base))
                                 {:source-table (tid ids :regions)
                                  :alias        "RG"
                                  :strategy     :inner-join
                                  :condition    [:and [:= [:field (fid ids :regions :id) {:join-alias "RG"}] [:field (fid ids :regions :id) {:join-alias "RG"}]]
                                                 [:not-null r]]
                                  :fields       :none})
                    :aggregation [[:count]])}
     :cumulative        {:query (assoc base :aggregation [[:cum-count]] :breakout [r])}
     :binning           {:query (assoc base :breakout [(if (#{:price :secret_num} t) (lib-binned r) r)] :aggregation [[:count]])}
     :parameter         {:query      (assoc base :aggregation [[:count]])
                         :parameters [(assoc (get-in targets [t :param]) :target [:dimension r])]})))

(defn- grid-probe-fn
  "A probe fn for one grid cell, or nil when the ref shape does not apply to the edge."
  [edge-k shape-k position-k target]
  (when ((ref-shapes shape-k) {:tables {} :fields {} :names {}} (edges edge-k) target)
    (fn [ids]
      (let [e (edges edge-k)
            r ((ref-shapes shape-k) ids e target)]
        (-> (grid-positions ids e target r)
            (get position-k)
            (dissoc :params-note))))))

(def ^:private grid-position-names
  [:fields :breakout :filter :aggregation :expression :case-expression :order-by :mixed-after-canon
   :mixed-before-canon :second-stage :explicit-join-present :right-join-shops-preserved :join-condition :cumulative
   :binning :parameter])

(defn grid-probe-names
  "Every `[edge shape position target]` of the grid, or those matching `selection` (a map of key -> seq)."
  ([] (grid-probe-names {}))
  ([{:keys [edges* shapes positions* targets*]}]
   (for [edge-k     (or edges* (keys edges))
         shape-k    (or shapes (keys ref-shapes))
         position-k (or positions* grid-position-names)
         target     (or targets* [:price :secret :rating])]
     [edge-k shape-k position-k target])))

(defn run-grid!
  "Run the grid probes named by `probe-names` (default: all) under `config-k`, or under every config."
  ([] (mapcat #(run-grid! % (grid-probe-names)) (keys sandbox-configs)))
  ([config-k probe-names]
   (do-with-corpus-config!
    config-k
    (fn [ctx]
      (doall (for [[edge-k shape-k position-k target :as probe-name] probe-names
                   :let [probe-fn (grid-probe-fn edge-k shape-k position-k target)]
                   :when probe-fn]
               (run-probe ctx probe-name probe-fn)))))))

(defn run-grid-formats!
  "[[run-grid!]] once per query format in `fmts`."
  [config-k fmts probe-names]
  (do-with-corpus-config!
   config-k
   (fn [ctx]
     (doall (for [fmt                                              fmts
                  [edge-k shape-k position-k target :as probe-name] probe-names
                  :let                                             [probe-fn (grid-probe-fn edge-k shape-k position-k target)]
                  :when                                            probe-fn]
              (run-probe (assoc ctx :format fmt) probe-name probe-fn))))))

(defn grid-summary
  "Outcome -> count, and the probes per failing outcome."
  [results]
  {:counts   (frequencies (map :outcome results))
   :failing  (->> results
                  (filter (comp failing-outcomes :outcome))
                  (map (juxt :outcome :probe (comp #(u/truncate (str %) 100) :error :user)))
                  sort
                  vec)})

;;; ------------------------------------------- Mutation score of enforcement ---------------------------------------
;;;
;;; Each mutant breaks the sandbox rewrite in one plausible way. Every mutant must be caught by at least one detector
;;; on at least one probe; a surviving mutant is a blind spot in the detectors.

(def ^:private real-apply-sandboxing
  "The OSS entry point of the rewrite, captured before any mutant replaces it. The preprocess pipeline calls this var,
  not the EE one, so mutants must replace it here."
  qp.middleware.enterprise/apply-sandboxing)

(defn- sandbox-only-stages-without-joins
  "A broken rewrite that skips any query with joins, as if the second pass never ran."
  [query]
  (if (some :joins (:stages query))
    query
    (real-apply-sandboxing query)))

(def ^:private mutants
  "Mutant name -> var -> replacement, applied with `with-redefs-fn`. Each is a way the rewrite could fail that would
  let hidden rows through. Left out on purpose: column projection and the wrapper `:fields` filter, whose breakage
  fails closed with a database error that no detector counts as a catch; and `fix-bad-field-id-refs`, which field
  resolution makes equivalent for every probe here."
  (array-map
   :no-sandboxing          {#'qp.middleware.enterprise/apply-sandboxing identity}
   :nothing-enforced       {#'sandbox.api.util/enforced-sandboxes-for-tables (constantly nil)}
   :attribute-filter-lost  {#'sandboxing/sandbox->parameters (constantly [])}
   :joins-never-sandboxed  {#'qp.middleware.enterprise/apply-sandboxing sandbox-only-stages-without-joins}))

(def ^:private mutant-probes
  "Probe name -> probe fn. A small set that between them exercise every part the mutants break: canonical probes,
  plus a bare off-table ref that only `fix-bad-field-id-refs` rescues through the explicit join."
  (delay
    (merge (select-keys canonical-probes [:shops-source :orders-implicit-price :orders-explicit-join :items-two-hops
                                          :nested-shops :card-shops])
           {[:orders/shop_id :bare-id :explicit-join-present :price]
            (grid-probe-fn :orders/shop_id :bare-id :explicit-join-present :price)})))

(def ^:private mutant-configs [:table-tier :card-hide-price :card-join])

(defn- detectors-fired [{:keys [differential canary structural hygiene]}]
  (cond-> #{}
    (#{:extra-rows :missing-rows :over-restricted} differential) (conj :differential)
    (= :leak (:verdict canary))                                  (conj :canary)
    (= :raw-ref (:verdict structural))                           (conj :structural)
    (= :dirty (:verdict hygiene))                                (conj :hygiene)))

(defn mutation-score!
  "Run the mutant probes under each mutant and config. Returns mutant -> set of detectors that caught it, over all
  configs and probes, plus `:probes` with the per-probe detail."
  []
  (into (sorted-map)
        (for [[mutant-k redefs] mutants]
          [mutant-k
           (let [results (with-redefs-fn redefs
                           (fn []
                             (doall
                              (for [config-k mutant-configs
                                    r        (do-with-corpus-config!
                                              config-k
                                              (fn [ctx]
                                                (doall (for [[probe-name probe-fn] @mutant-probes]
                                                         (run-probe ctx probe-name probe-fn)))))]
                                r))))]
             {:caught-by (reduce into #{} (map detectors-fired results))
              :probes    (mapv (fn [r] [(:config r) (:probe r) (:outcome r)]) results)})])))

;;; ------------------------------------------------ Endpoint scan -------------------------------------------------
;;;
;;; Everything the sandboxed user can reach that reads SHOPS data or metadata, outside `POST /api/dataset`: scanned
;;; for canaries of hidden rows and for hygiene. The base database gets one full sync first, so fingerprints and
;;; field values exist as they would in a real instance.

(defn- do-with-corpus-dashboard!
  "Create a dashboard showing the SHOPS card and the ORDERS card with implicit SHOPS columns, with `secret` and `tier`
  parameters wired to SHOPS directly and through the implicit join, and call `(f dashboard)`, where `dashboard` has
  `:id` and `:dashcards`, card key -> `[dashcard-id card-id]`."
  [ids f]
  (let [orders-card (get-in ids [:cards :card/orders-implicit])
        shops-card  (get-in ids [:cards :card/shops])
        mappings    (fn [card-id field-ref-fn]
                      (for [param ["secret" "tier"]]
                        {:parameter_id param :card_id card-id :target ["dimension" (field-ref-fn (keyword param))]}))]
    (mt/with-temp [:model/Dashboard {dash-id :id} {:parameters [{:id "secret" :slug "secret" :name "secret" :type "string/="}
                                                                {:id "tier" :slug "tier" :name "tier" :type "number/="}]}
                   :model/DashboardCard {orders-dc :id} {:dashboard_id       dash-id
                                                         :card_id            orders-card
                                                         :parameter_mappings (mappings orders-card #(implicit ids :orders :shop_id :shops %))}
                   :model/DashboardCard {shops-dc :id} {:dashboard_id       dash-id
                                                        :card_id            shops-card
                                                        :parameter_mappings (mappings shops-card #(vector :field (fid ids :shops %) nil))}]
      (f {:id dash-id :dashcards {:card/orders-implicit [orders-dc orders-card] :card/shops [shops-dc shops-card]}}))))

(defn- dashboard-requests
  "Endpoint name -> request for the dashboard of [[do-with-corpus-dashboard!]]. Tier 2 is hidden by the row sandboxes."
  [ids {dash :id :keys [dashcards]}]
  (let [run-dashcard (fn [card-k params]
                       (let [[dashcard card] (dashcards card-k)]
                         [:post (format "dashboard/%d/dashcard/%d/card/%d/query" dash dashcard card) {:parameters params}]))
        hidden-tier  [{:id "tier" :type "number/=" :value [2]}]]
    (array-map
     :dashboard-get                [:get (format "dashboard/%d" dash)]
     :dashboard-query-metadata     [:get (format "dashboard/%d/query_metadata" dash)]
     :dashboard-param-values       [:get (format "dashboard/%d/params/secret/values" dash)]
     :dashboard-param-search       [:get (format "dashboard/%d/params/secret/search/CANARY" dash)]
     :dashboard-chain-filter       [:get (format "dashboard/%d/params/secret/values?tier=2" dash)]
     :dashboard-chain-filter-tier  [:get (format "dashboard/%d/params/tier/values?secret=%s" dash
                                                 (corpus/canary-text :shops 2))]
     :dashcard-orders-hidden-tier  (run-dashcard :card/orders-implicit hidden-tier)
     :dashcard-shops-hidden-tier   (run-dashcard :card/shops hidden-tier)
     :xray-table                   [:get (format "automagic-dashboards/table/%d" (tid ids :shops))]
     :xray-field-secret-num        [:get (format "automagic-dashboards/field/%d" (fid ids :shops :secret_num))])))

(defn- endpoint-requests
  "Endpoint name -> `[method path body?]` for the current (base copy) database."
  [ids]
  (let [shops      (tid ids :shops)
        secret     (fid ids :shops :secret)
        secret-num (fid ids :shops :secret_num)
        price      (fid ids :shops :price)
        model      (get-in ids [:cards :model/shops])
        card       (get-in ids [:cards :card/shops])
        canonical  (implicit ids :orders :shop_id :shops :secret)
        orders-q   {:database (:db ids) :type "query"
                    :query    {:source-table (tid ids :orders) :fields [[:field (fid ids :orders :id) nil] canonical]}}]
    (array-map
     :field-secret-num         [:get (format "field/%d" secret-num)]
     :field-secret             [:get (format "field/%d" secret)]
     :field-values-secret      [:get (format "field/%d/values" secret)]
     :field-values-secret-num  [:get (format "field/%d/values" secret-num)]
     :field-search-secret      [:get (format "field/%d/search/%d?value=CANARY&limit=1000" secret secret)]
     :field-summary-secret-num [:get (format "field/%d/summary" secret-num)]
     :field-related            [:get (format "field/%d/related" secret)]
     :table-query-metadata     [:get (format "table/%d/query_metadata" shops)]
     :table                    [:get (format "table/%d" shops)]
     :table-fks                [:get (format "table/%d/fks" shops)]
     :database-metadata        [:get (format "database/%d/metadata" (:db ids))]
     :database-fields          [:get (format "database/%d/fields" (:db ids))]
     :database-idfields        [:get (format "database/%d/idfields" (:db ids))]
     :parameter-values         [:post "dataset/parameter/values"
                                {:parameter {:id "p" :type "string/=" :slug "s"} :field_ids [secret]}]
     :parameter-search         [:post "dataset/parameter/search/CANARY"
                                {:parameter {:id "p" :type "string/=" :slug "s"} :field_ids [secret]}]
     :pivot                    [:post "dataset/pivot"
                                {:database (:db ids) :type "query"
                                 :query    {:source-table (tid ids :orders) :breakout [canonical] :aggregation [["count"]]}}]
     :pivot-secret-num         [:post "dataset/pivot"
                                {:database (:db ids) :type "query"
                                 :query    {:source-table (tid ids :orders)
                                            :breakout     [(implicit ids :orders :shop_id :shops :secret_num)]
                                            :aggregation  [["count"]]}}]
     :download-csv             [:post "dataset/csv" {:query (json/encode orders-q)}]
     :download-json            [:post "dataset/json" {:query (json/encode orders-q)}]
     :card-get-model           [:get (format "card/%d" model)]
     :card-get                 [:get (format "card/%d" card)]
     :card-query-metadata      [:get (format "card/%d/query_metadata" model)]
     :card-run-model           [:post (format "card/%d/query" model)]
     :card-run-model-csv       [:post (format "card/%d/query/csv" model)]
     :table-card-metadata      [:get (format "table/card__%d/query_metadata" model)]
     :binning-breakout         [:post "dataset"
                                {:database (:db ids) :type "query"
                                 :query    {:source-table shops
                                            :breakout     [[:field price {:binning {:strategy :default}}]]
                                            :aggregation  [["count"]]}}]
     :binning-secret-num       [:post "dataset"
                                {:database (:db ids) :type "query"
                                 :query    {:source-table shops
                                            :breakout     [[:field secret-num {:binning {:strategy :default}}]]
                                            :aggregation  [["count"]]}}])))

(defn- request-as [user [method path body]]
  (let [{:keys [status] response :body} (if body
                                          (mt/user-http-request-full-response user method path body)
                                          (mt/user-http-request-full-response user method path))]
    {:status status :body response}))

(defn run-canary-endpoints!
  "Call every endpoint of [[endpoint-requests]] as the sandboxed user under `config-k`, after a full sync of the base
  copy. Returns endpoint -> `{:status, :leaked-venue-ids, :hygiene}`."
  [config-k]
  (do-with-corpus-config!
   config-k
   (fn [{:keys [ids allowed] :as ctx}]
     (do-with-corpus-dashboard!
      ids
      (fn [dashboard]
        (into (array-map)
              (for [[endpoint request] (concat (endpoint-requests ids) (dashboard-requests ids dashboard))]
                (let [{:keys [status body]} (request-as :rasta request)
                      user                  {:body body}]
                  [endpoint {:status  status
                             :leaked  (not-empty (into (sorted-set) (remove allowed) (corpus/canary-ids body)))
                             :hygiene (hygiene-verdict ctx user)}]))))))))

(deftest ^:synchronized endpoints-reveal-nothing-outside-the-sandbox-test
  (mt/test-drivers #{:h2}
    (doseq [config-k [:table-tier :card-hide-price :table-tier+value-remap]
            [endpoint {:keys [status leaked hygiene]}] (run-canary-endpoints! config-k)]
      (testing (format "%s under %s (HTTP %s)" endpoint config-k status)
        (testing "no canary of a hidden row"
          (is (nil? leaked)))
        (testing "nothing internal in the response"
          (is (= :clean (:verdict hygiene)) (pr-str hygiene)))))))

;;; --------------------------------------------- Generated queries -----------------------------------------------
;;;
;;; Random valid queries from the lib generator, started from each source kind, converted to legacy MBQL the way a
;;; client would send them, then (step 6) rewritten into non-standard ref encodings. test.check shrinks a failing
;;; case to the shortest walk and fewest mutations that still fail.

(defn- translate-query
  "`legacy-query`, built against database `from`, rewritten to name the same tables, fields and cards in database
  `to`. Only ids in table, field and card positions are touched."
  [legacy-query from to]
  (let [tbl     (into {} (for [t tables] [(tid from t) (tid to t)]))
        fld     (into {} (for [[t cols] columns, c cols] [(fid from t c) (fid to t c)]))
        crd     (into {} (for [k (keys (:cards from))] [(card-source from k) (card-source to k)]))
        xf-opts (fn [opts] (cond-> opts (and (map? opts) (int? (:source-field opts))) (update :source-field #(fld % %))))]
    (-> (walk/postwalk
         (fn [x]
           (cond
             (and (map? x) (contains? x :source-table))
             (update x :source-table #(or (tbl %) (crd %) %))

             (and (vector? x) (= :field (first x)) (int? (second x)))
             [:field (fld (second x) (second x)) (xf-opts (nth x 2 nil))]

             (and (vector? x) (= :field-id (first x)) (int? (second x)))
             [:field-id (fld (second x) (second x))]

             :else x))
         legacy-query)
        (assoc :database (:db to)))))

(def ^:private source-kinds
  [:table/shops :table/orders :table/items :table/regions :card/shops :model/shops :card/orders-implicit :nested/shops])

(defn- start-query
  "A lib query over the current database to start a random walk from."
  [ids source]
  (let [mp    (mt/metadata-provider)
        table (fn [t] (lib/query mp (lib.metadata/table mp (tid ids t))))
        card  (fn [k] (lib/query mp (lib.metadata/card mp (get-in ids [:cards k]))))]
    (case source
      :table/shops          (table :shops)
      :table/orders         (table :orders)
      :table/items          (table :items)
      :table/regions        (table :regions)
      :card/shops           (card :card/shops)
      :model/shops          (card :model/shops)
      :card/orders-implicit (card :card/orders-implicit)
      :nested/shops         (lib/append-stage (table :shops)))))

(defn- generate-legacy-query
  "A random walk of `n-steps` from `source`, seeded, as a legacy query over the current database."
  [ids {:keys [source seed n-steps]}]
  (let [mp (mt/metadata-provider)]
    (binding [tu.rng/*generator*             (java.util.Random. ^long seed)
              lib.tu.gen/*safe-for-old-refs* false
              ;; SQL question results are not sandboxed, by design; see [[saved-sql-probes]]
              lib.tu.gen/*available-cards*   (mapv #(lib.metadata/card mp %) (vals (dissoc (:cards ids) :native/shops)))]
      (-> (lib.tu.gen/random-queries-from (start-query ids source) n-steps)
          last
          lib/->legacy-MBQL))))

(defn- field-ref? [x]
  (and (vector? x) (= :field (first x)) (or (int? (second x)) (string? (second x)))))

(defn- field-id->column
  "Field id -> `[table column]` for the ids map."
  [ids]
  (into {} (map (fn [[tc id]] [id tc])) (:fields ids)))

(def ^:private column-base-types
  {:id :type/Integer :name :type/Text :tier :type/Integer :price :type/Integer :amount :type/Integer :qty :type/Integer
   :region_id :type/Integer :parent_shop_id :type/Integer :hq_shop_id :type/Integer :shop_id :type/Integer
   :return_shop_id :type/Integer :order_id :type/Integer :secret :type/Text :secret_num :type/BigInteger
   :rating :type/Float :opened_on :type/Date})

(defn- ref-column
  "`[table column]` a field ref names, when it names one by id."
  [ids [_ id-or-name]]
  (when (int? id-or-name) ((field-id->column ids) id-or-name)))

(defn- nth-field-ref
  "The k-th (mod count) field ref of `x` in traversal order, refs to SHOPS first; nil when there are none."
  [ids x k]
  (let [refs  (atom [])
        _     (walk/postwalk (fn [v] (when (field-ref? v) (swap! refs conj v)) v) x)
        shops (filter #(= :shops (first (ref-column ids %))) @refs)
        all   (concat shops (remove #(= :shops (first (ref-column ids %))) @refs))]
    (when (seq all)
      (nth all (mod k (count all))))))

(defn- replace-ref
  "`x` with every occurrence of field ref `r` replaced by `(f r)`."
  [x r f]
  (walk/postwalk (fn [v] (if (= v r) (f v) v)) x))

(defn- name-ref
  "`r` rewritten as a name ref, with or without its base type."
  [ids [_ _ opts :as r] base-type?]
  (if-let [[table column] (ref-column ids r)]
    [:field (fname ids table column) (cond-> (or opts {})
                                       true       (dissoc :source-field :source-field-name :source-field-join-alias :join-alias)
                                       base-type? (assoc :base-type (column-base-types column)))]
    r))

(defn- fk-column-name [ids opts]
  (when-let [[table column] (ref-column ids [:field (:source-field opts) nil])]
    (fname ids table column)))

(def ^:private mutation-kinds
  "Mutation name -> fn of ids, the outer legacy query and the k-th ref, returning the mutated outer query. Each is a
  way a client could encode a ref that the query builder never produces."
  (array-map
   :id->name                   (fn [ids q r] (replace-ref q r #(name-ref ids % true)))
   :id->name-no-base-type      (fn [ids q r] (replace-ref q r #(name-ref ids % false)))
   :source-field-name          (fn [ids q [_ id opts :as r]]
                                 (if-let [n (and (:source-field opts) (fk-column-name ids opts))]
                                   (replace-ref q r (constantly [:field id (assoc opts :source-field-name n)]))
                                   q))
   :source-field-name-bogus    (fn [_ q [_ id opts :as r]]
                                 (if (:source-field opts)
                                   (replace-ref q r (constantly [:field id (assoc opts :source-field-name "NOPE")]))
                                   q))
   :source-field-join-bogus    (fn [_ q [_ id opts :as r]]
                                 (if (:source-field opts)
                                   (replace-ref q r (constantly [:field id (assoc opts :source-field-join-alias "Nope")]))
                                   q))
   :fk->                       (fn [_ q [_ id opts :as r]]
                                 (if (and (int? id) (:source-field opts))
                                   (replace-ref q r (constantly [:fk-> [:field-id (:source-field opts)] [:field-id id]]))
                                   q))
   :field-literal              (fn [ids q r]
                                 (if-let [[table column] (ref-column ids r)]
                                   (replace-ref q r (constantly [:field-literal (fname ids table column) (column-base-types column)]))
                                   q))
   :joined-field               (fn [_ q [_ id opts :as r]]
                                 (if (and (int? id) (:join-alias opts))
                                   (replace-ref q r (constantly [:joined-field (:join-alias opts) [:field-id id]]))
                                   q))
   :drop-join-alias            (fn [_ q [_ id opts :as r]]
                                 (if (:join-alias opts)
                                   (replace-ref q r (constantly [:field id (not-empty (dissoc opts :join-alias))]))
                                   q))
   :bogus-join-alias           (fn [_ q [_ id opts :as r]]
                                 (replace-ref q r (constantly [:field id (assoc opts :join-alias "Nope")])))
   :drop-source-field          (fn [_ q [_ id opts :as r]]
                                 (if (:source-field opts)
                                   (replace-ref q r (constantly [:field id (not-empty (dissoc opts :source-field :source-field-name :source-field-join-alias))]))
                                   q))
   :duplicate-into-filter      (fn [_ q r]
                                 (update-in q [:query :filter] #(if % [:and % [:not-null r]] [:not-null r])))
   :outer-stage                (fn [_ q r]
                                 (assoc q :query {:source-query (:query q) :fields [r]}))
   :outer-stage-name-ref       (fn [ids q r]
                                 (assoc q :query {:source-query (:query q) :fields [(name-ref ids r true)]}))
   :parameter                  (fn [ids q r]
                                 (if-let [[_ column] (ref-column ids r)]
                                   (let [text? (= :type/Text (column-base-types column))]
                                     (update q :parameters (fnil conj [])
                                             {:type   (if text? "string/!=" "number/!=")
                                              :value  [(if text? "__none__" -424242)]
                                              :target [:dimension r]}))
                                   q))))

(defn- apply-mutations
  "Rewrite refs of `legacy-query` per `mutations`, each `[kind k]`: the k-th field ref (SHOPS refs first) gets the
  kind's encoding. Mutations that do not apply to the chosen ref leave the query unchanged."
  [ids legacy-query mutations]
  (reduce (fn [q [kind k]]
            (if-let [r (nth-field-ref ids (:query q) k)]
              ((mutation-kinds kind) ids q r)
              q))
          legacy-query
          mutations))

(defn- run-case!
  "Generate, mutate and run one case under `ctx`. A generator failure is recorded as `:generator-error`, not a
  sandbox failure."
  [{:keys [ids restricted-ids] :as ctx} {:keys [source seed n-steps mutations format] :or {format :legacy} :as case}]
  (let [generated (try (apply-mutations ids (generate-legacy-query ids case) mutations)
                       (catch Throwable e {::generator-error (ex-message e)}))]
    (if-let [error (::generator-error generated)]
      {:outcome :generator-error :case case :error (u/truncate error 300) :config (:config ctx)}
      (let [q-base       (select-keys generated [:query :parameters])
            q-restricted (select-keys (translate-query generated ids restricted-ids) [:query :parameters])
            probe-fn     (fn [i] (if (= (:db i) (:db ids)) q-base q-restricted))]
        (assoc (run-probe (assoc ctx :format format) [:generated source seed n-steps mutations format] probe-fn) :case case)))))

(def ^:private gen-case
  (gen/hash-map :source    (gen/elements source-kinds)
                :seed      (gen/no-shrink gen/large-integer)
                :n-steps   (gen/choose 1 12)
                :mutations (gen/vector (gen/tuple (gen/elements (keys mutation-kinds)) gen/nat) 0 3)
                ;; shrinks toward `:legacy`, the first format
                :format    (gen/elements (keys query-formats))))

(defn- env-long [var-name default]
  (or (some-> (System/getenv var-name) parse-long) default))

(defn- generated-cases-per-config [] (env-long "MB_SANDBOX_GEN_CASES" 40))
(defn- generated-seed [] (env-long "MB_SANDBOX_GEN_SEED" 20261026))

(defn run-generated!
  "Run `n` generated cases under `config-k` with test.check seed `seed`. Returns the quick-check result and every
  case result, including those run while shrinking."
  [config-k n seed]
  (do-with-corpus-config!
   config-k
   (fn [ctx]
     (let [outcomes (atom [])
           property (prop/for-all [c gen-case]
                      (let [r (run-case! ctx c)]
                        (swap! outcomes conj r)
                        (not (failing-outcomes (:outcome r)))))
           ;; the property needs the expensive harness around it, so it cannot be a top-level `defspec`
           result   #_{:clj-kondo/ignore [:discouraged-var]} (tc/quick-check n property :seed seed)]
       {:result result :outcomes @outcomes}))))

(defn- meaningful?
  "At least some of the cases ran for both users and compared rows; a run that only ever errors proves nothing."
  [outcomes]
  (let [counts (frequencies (map :outcome outcomes))
        total  (count outcomes)]
    (and (pos? total)
         (>= (+ (get counts :consistent 0) (get counts :vacuous 0)) (* 0.3 total)))))

(defn repro-report
  "The smallest failing case as a reproduction: its generator inputs, the legacy query as JSON, and every verdict."
  [{:keys [shrunk fail] :as result} outcomes]
  (when-not (:pass? result)
    (let [smallest  (first (or (:smallest shrunk) fail))
          r         (last (filter #(= smallest (:case %)) outcomes))
          ;; a property that threw has no result record; test.check keeps the exception
          exception (some-> (or (:result shrunk) (:result result)) ex-message)]
      (str/join "\n"
                ["Smallest failing case:" (pr-str smallest)
                 "Outcome:" (pr-str (select-keys r [:outcome :differential :canary :structural :hygiene :error]))
                 "Exception:" (pr-str exception)
                 "Legacy query (base copy ids):" (json/encode (:query r))
                 "Sandboxed user got:" (pr-str (:user r))
                 "Expected:" (pr-str (:expected r))]))))

;;; --------------------------------------------------- Tests ------------------------------------------------------

(defn- corpus-drivers []
  (mt/normal-drivers-with-feature :nested-queries :left-join))

(defn- run-as-admin [inner-query]
  (mt/user-http-request :crowberto :post 202 "dataset" {:database (mt/id) :type "query" :query inner-query}))

(defn- canary-ids-of [table body]
  (into (sorted-set) (keep (fn [[t id]] (when (= t table) id))) (corpus/canary-ids body)))

(def ^:private expected-variants
  "Dataset variant -> the SHOPS and REGIONS rows whose canaries an admin sees in it, and the SHOPS row count."
  {#'corpus/sandbox-corpus                  {:shops #{1 2 3 4 5 6 7 8} :regions #{1 2 3 4} :shop-rows 8}
   #'corpus/sandbox-corpus-tier1            {:shops #{1 3 5 7}         :regions #{1 2 3 4} :shop-rows 4}
   #'corpus/sandbox-corpus-tier1-both       {:shops #{1 3 5 7}         :regions #{1 3}     :shop-rows 4}
   #'corpus/sandbox-corpus-region-tier1     {:shops #{1 2 5 6}         :regions #{1 2 3 4} :shop-rows 4}
   #'corpus/sandbox-corpus-hide-price       {:shops #{}                :regions #{1 2 3 4} :shop-rows 8}
   #'corpus/sandbox-corpus-tier1-hide-price {:shops #{}                :regions #{1 2 3 4} :shop-rows 4}
   #'corpus/sandbox-corpus-no-shops         {:shops #{}                :regions #{1 2 3 4} :shop-rows 0}})

(deftest ^:synchronized corpus-dataset-loads-as-designed-test
  (mt/test-drivers (corpus-drivers)
    (doseq [[dataset-var {:keys [shops regions shop-rows]}] expected-variants]
      (testing (:name (meta dataset-var))
        (mt/dataset (var-get dataset-var)
          (corpus/ensure-fk-metadata!)
          (testing "every FK is marked in the metadata"
            (doseq [[from-table from-col to-table to-col] corpus/fks]
              (is (= (mt/id to-table to-col)
                     (t2/select-one-fn :fk_target_field_id :model/Field :id (mt/id from-table from-col))))))
          (let [shops-body (run-as-admin {:source-table (mt/id :shops)})]
            (testing "SHOPS rows and canaries"
              (is (= shop-rows (count (get-in shops-body [:data :rows]))))
              (is (= shops (canary-ids-of :shops shops-body)))))
          (testing "REGIONS canaries"
            (is (= regions (canary-ids-of :regions (run-as-admin {:source-table (mt/id :regions)})))))
          (testing "both FKs from ORDERS into SHOPS resolve as implicit joins"
            (let [body (run-as-admin {:source-table (mt/id :orders)
                                      :fields       [["field" (mt/id :shops :name) {"source-field" (mt/id :orders :shop_id)}]
                                                     ["field" (mt/id :shops :name) {"source-field" (mt/id :orders :return_shop_id)}]]
                                      :limit        2})]
              (is (= "completed" (:status body)) (:error body))
              (is (= 2 (count (first (get-in body [:data :rows]))))))))))))

(deftest ^:synchronized canonical-probes-are-enforced-under-every-sandbox-test
  (mt/test-drivers (corpus-drivers)
    (doseq [[mode run-in-mode] schema-check-modes
            config-k           (keys sandbox-configs)]
      (when (mt/dataset corpus/sandbox-corpus (config-supported? config-k))
        (doseq [{:keys [probe outcome] :as result} (run-in-mode #(run-canonical! config-k))]
          (testing (format "%s under %s in %s" probe config-k mode)
            (is (not (failing-outcomes outcome))
                (pr-str (select-keys result [:outcome :differential :canary :structural :hygiene :metadata-diff
                                             :user :expected :query])))))))))

(deftest ^:synchronized saved-sql-results-are-not-sandboxed-as-documented-test
  (testing "A sandboxed user who can see a saved SQL question gets its unsandboxed results, as the docs say"
    (mt/test-drivers (corpus-drivers)
      (do-with-corpus-config!
       :table-tier
       (fn [ctx]
         (doseq [[probe-name probe-fn] saved-sql-probes
                 :let [{:keys [differential]} (run-probe (assoc ctx :expect :unrestricted) probe-name probe-fn)]]
           (testing probe-name
             (is (= :consistent differential)))))))))

(deftest ^:synchronized attribute-values-are-enforced-or-rejected-test
  (testing "Every login attribute value either filters the sandboxed table or fails the query, with and without schema checks"
    (mt/test-drivers (corpus-drivers)
      (doseq [[mode run-in-mode] schema-check-modes
              config-k           (keys attribute-variants)
              :when              (mt/dataset corpus/sandbox-corpus (config-supported? config-k))
              ;; the bogus-join-alias probes are covered by the canonical test
              {:keys [probe outcome] :as result} (run-in-mode
                                                  #(run-canonical! config-k (remove #{:model-bogus-join-alias :card-bogus-join-alias}
                                                                                    (keys canonical-probes))))]
        (testing (format "%s under %s in %s" probe config-k mode)
          (is (not (failing-outcomes outcome))
              (pr-str (select-keys result [:outcome :differential :canary :user :expected]))))))))

(deftest ^:synchronized sandboxed-user-responses-are-hygienic-test
  (testing "Nothing in a sandboxed user's response reveals the sandbox or the raw table: no fingerprints, no native or preprocessed forms, no SQL text"
    (mt/test-drivers (corpus-drivers)
      (doseq [{:keys [probe config hygiene]} (mapcat run-canonical! [:table-tier :card-hide-price])]
        (testing (format "%s under %s" probe config)
          (is (= :clean (:verdict hygiene)) (pr-str hygiene)))))))

(def ^:private forged-stage-keys
  "Keys the rewrite or the permission walk set on stages, which a client must not be able to supply."
  {:sandbox?-flag    (fn [_table-id] {"metabase-enterprise.sandbox.query-processor.middleware.sandboxing/sandbox?" true})
   :marker           (fn [table-id] {"query-permissions/sandboxed-table" table-id})
   :flag-and-marker  (fn [table-id] {"metabase-enterprise.sandbox.query-processor.middleware.sandboxing/sandbox?" true
                                     "query-permissions/sandboxed-table" table-id})
   :from-source-card (fn [_table-id] {"qp/stage-is-from-source-card" 1})})

(deftest ^:synchronized forged-internal-stage-keys-do-not-skip-sandbox-test
  (testing "Client-supplied internal stage keys, in legacy and MBQL 5 form, do not make the rewrite skip a stage"
    (mt/test-drivers (corpus-drivers)
      (do-with-corpus-config!
       :table-tier
       (fn [{:keys [ids allowed]}]
         (doseq [[k extra-fn] forged-stage-keys
                 :let         [extra (extra-fn (tid ids :shops))
                               field ["field" (fid ids :shops :secret) nil]]
                 [fmt query]  {:legacy {:database (:db ids)
                                        :type     "query"
                                        :query    (merge {:source-table (tid ids :shops) :fields [field]} extra)}
                               :mbql5  {:database (:db ids)
                                        :lib/type "mbql/query"
                                        :stages   [(merge {:lib/type     "mbql.stage/mbql"
                                                           :source-table (tid ids :shops)
                                                           :fields       [["field" {:lib/uuid (str (random-uuid))} (fid ids :shops :secret)]]}
                                                          extra)]}}]
           (testing (format "%s as %s" k fmt)
             (let [{:keys [body]} (mt/user-http-request-full-response :rasta :post "dataset" query)]
               (is (= "completed" (:status body)) (:error body))
               (is (empty? (remove allowed (corpus/canary-ids body)))
                   "the sandboxed user saw a hidden row's canary")))))))))

(deftest ^:synchronized detectors-agree-with-each-other-test
  (testing "No probe makes two detectors contradict each other; a disagreement is a detector bug"
    (mt/test-drivers (corpus-drivers)
      (is (empty? (detector-disagreements (mapcat run-canonical! [:table-tier :card-hide-price :two-tables])))))))

(deftest ^:synchronized every-mutant-of-the-rewrite-is-caught-test
  (testing "Each plausible way of breaking the sandbox rewrite is caught by at least one detector"
    (mt/test-drivers #{:h2}
      (doseq [[mutant-k {:keys [caught-by probes]}] (mutation-score!)]
        (testing (name mutant-k)
          (is (seq caught-by) (pr-str probes)))))))

(def ^:private ci-grid-selection
  "The slice of the grid that runs in CI: every edge and ref shape, in the positions that take different code paths,
  for the numeric target."
  {:positions* [:fields :join-condition :second-stage :parameter]
   :targets*   [:price]})

(deftest ^:synchronized field-ref-grid-slice-cannot-escape-sandbox-test
  (mt/test-drivers (corpus-drivers)
    (doseq [[mode run-in-mode]                  schema-check-modes
            config-k                            [:table-tier :card-hide-price]
            {:keys [probe outcome] :as result} (run-in-mode #(run-grid! config-k (grid-probe-names ci-grid-selection)))]
      (testing (format "%s under %s in %s" (pr-str probe) config-k mode)
        (is (not (failing-outcomes outcome))
            (pr-str (select-keys result [:outcome :differential :canary :structural :hygiene :user :query])))))))

(deftest ^:synchronized query-formats-are-sandboxed-alike-test
  (testing "A probe gets the same outcome sent as legacy MBQL, as MBQL 5, and as MBQL 5 with legacy-ordered refs"
    (mt/test-drivers (corpus-drivers)
      (doseq [[mode run-in-mode] schema-check-modes
              config-k           [:table-tier :card-hide-price]
              [probe results]    (group-by :probe
                                           (run-in-mode
                                            #(concat (run-canonical-formats! config-k (keys query-formats))
                                                     (when (= config-k :table-tier)
                                                       (run-grid-formats! config-k (keys query-formats)
                                                                          (grid-probe-names ci-grid-selection))))))]
        (testing (format "%s under %s in %s" (pr-str probe) config-k mode)
          (is (= 1 (count (into #{} (map :outcome) results)))
              (pr-str (map (juxt :format :outcome) results))))))))

(deftest ^:synchronized hidden-data-does-not-change-sandboxed-responses-test
  (testing "Changing only the rows or columns a sandbox hides leaves every sandboxed response the same"
    (mt/test-drivers (corpus-drivers)
      (doseq [[mode run-in-mode] schema-check-modes
              [config-k config]  sandbox-configs
              :when              (and (varied-datasets (:restricted config))
                                      (not (#{:error :unrestricted} (:expect config)))
                                      (mt/dataset corpus/sandbox-corpus (config-supported? config-k)))
              {:keys [probe varied base varied-response]} (run-in-mode #(run-noninterference! config-k))]
        (testing (format "%s under %s on %s in %s" probe config-k (:name (meta varied)) mode)
          (is (= base varied-response)
              (u/truncate (pr-str (take 2 (data/diff base varied-response))) 2000)))))))

(deftest ^:synchronized generated-queries-cannot-escape-sandbox-test
  (mt/test-drivers (corpus-drivers)
    (doseq [[mode run-in-mode] schema-check-modes
            config-k           [:table-tier :card-hide-price :two-tables]]
      (testing (format "%s in %s" config-k mode)
        (let [{:keys [result outcomes]} (run-in-mode #(run-generated! config-k (generated-cases-per-config) (generated-seed)))]
          (is (true? (:pass? result)) (repro-report result outcomes))
          (is (meaningful? outcomes) (pr-str (frequencies (map :outcome outcomes)))))))))

(def ^:private fake-ids
  "An ids map with made-up ids, for testing the mutation catalogue without a database."
  {:db     1
   :tables (zipmap tables (range 100 104))
   :fields (into {} (map-indexed (fn [i tc] [tc (+ 200 i)])) (for [[t cols] columns, c cols] [t c]))
   :names  (into {} (for [[t cols] columns, c cols] [[t c] (u/upper-case-en (name c))]))
   :cards  {:card/shops 5 :model/shops 6 :card/orders-implicit 7}})

(deftest ^:parallel mutation-kinds-rewrite-refs-test
  (let [f  (partial fid fake-ids)
        q  {:database 1
            :type     :query
            :query    {:source-table (tid fake-ids :orders)
                       :joins        [{:source-table (tid fake-ids :shops) :alias "S"
                                       :condition    [:= [:field (f :orders :shop_id) nil] [:field (f :shops :id) {:join-alias "S"}]]
                                       :fields       :none}]
                       :fields       [[:field (f :orders :id) nil]
                                      [:field (f :shops :price) {:source-field (f :orders :shop_id)}]
                                      [:field (f :shops :secret) {:join-alias "S"}]]}}
        at (fn [kind k] (apply-mutations fake-ids q [[kind k]]))]
    (testing "SHOPS refs come first in walk order: the join condition's SHOPS.ID, then PRICE, then SECRET"
      (is (= [:field "ID" {:base-type :type/Integer}]
             (get-in (at :id->name 0) [:query :joins 0 :condition 2])))
      (is (= [:field "PRICE" {:base-type :type/Integer}]
             (get-in (at :id->name 1) [:query :fields 1])))
      (is (= [:field "SECRET" {:base-type :type/Text}]
             (get-in (at :id->name 2) [:query :fields 2])))
      (is (= [:field "SECRET" {}]
             (get-in (at :id->name-no-base-type 2) [:query :fields 2]))))
    (testing "source-field variants"
      (is (= [:field (f :shops :price) {:source-field (f :orders :shop_id) :source-field-name "SHOP_ID"}]
             (get-in (at :source-field-name 1) [:query :fields 1])))
      (is (= [:field (f :shops :price) {:source-field (f :orders :shop_id) :source-field-name "NOPE"}]
             (get-in (at :source-field-name-bogus 1) [:query :fields 1])))
      (is (= [:fk-> [:field-id (f :orders :shop_id)] [:field-id (f :shops :price)]]
             (get-in (at :fk-> 1) [:query :fields 1])))
      (is (= [:field (f :shops :price) nil]
             (get-in (at :drop-source-field 1) [:query :fields 1])))
      (testing "do not apply to a ref without :source-field"
        (is (= q (at :fk-> 2)))))
    (testing "join-alias variants"
      (is (= [:joined-field "S" [:field-id (f :shops :secret)]]
             (get-in (at :joined-field 2) [:query :fields 2])))
      (is (= [:field (f :shops :secret) nil]
             (get-in (at :drop-join-alias 2) [:query :fields 2])))
      (is (= [:field (f :shops :secret) {:join-alias "Nope"}]
             (get-in (at :bogus-join-alias 2) [:query :fields 2]))))
    (testing "wrapping and parameters"
      (is (= [:field-literal "PRICE" :type/Integer]
             (get-in (at :field-literal 1) [:query :fields 1])))
      (is (= [:not-null [:field (f :shops :price) {:source-field (f :orders :shop_id)}]]
             (get-in (at :duplicate-into-filter 1) [:query :filter])))
      (is (= {:source-query (:query q) :fields [[:field (f :shops :price) {:source-field (f :orders :shop_id)}]]}
             (:query (at :outer-stage 1))))
      (is (= [[:field "PRICE" {:base-type :type/Integer}]]
             (get-in (at :outer-stage-name-ref 1) [:query :fields])))
      (is (= [{:type "number/!=" :value [-424242] :target [:dimension [:field (f :shops :price) {:source-field (f :orders :shop_id)}]]}]
             (:parameters (at :parameter 1))))
      (is (= "string/!=" (-> (at :parameter 2) :parameters first :type))))
    (testing "k wraps around the five candidate refs"
      (is (= (at :id->name 1) (at :id->name 6))))))

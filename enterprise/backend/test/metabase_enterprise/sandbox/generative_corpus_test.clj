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
   [clojure.test :refer :all]
   [metabase-enterprise.sandbox.corpus-test-dataset :as corpus]
   [metabase-enterprise.sandbox.test-util :as met]
   [metabase.driver :as driver]
   [metabase.driver.util :as driver.u]
   [metabase.lib.walk :as lib.walk]
   [metabase.permissions.core :as perms]
   [metabase.permissions.models.data-permissions :as data-perms]
   [metabase.query-processor.preprocess :as qp.preprocess]
   [metabase.query-processor.setup :as qp.setup]
   [metabase.query-processor.util.add-alias-info :as add]
   [metabase.test :as mt]
   [metabase.util :as u]
   [metabase.util.json :as json]
   [metabase.util.match :as match]
   [toucan2.core :as t2]))

(set! *warn-on-reflection* true)

;;; ---------------------------------------------------- Ids -------------------------------------------------------

(def ^:private tables [:regions :shops :orders :items])

(def ^:private columns
  {:regions [:id :name :tier :hq_shop_id :secret :secret_num]
   :shops   [:id :name :tier :price :region_id :parent_shop_id :secret :secret_num]
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
                          :fields [$id $name $tier $price $region_id $parent_shop_id $secret $secret_num]})
           :remappings {"tier" ["dimension" [:field (mt/id :regions :tier) {:join-alias "R"}]]}}})

(defn- shops-hide-price-gtap []
  {:shops {:query (mt/mbql-query shops {:fields [$id $name $tier $region_id $parent_shop_id]})}})

(defn- shops-rows+hide-price-gtap []
  {:shops {:query      (mt/mbql-query shops {:fields [$id $name $tier $region_id $parent_shop_id]})
           :remappings (tier-dimension-remapping :shops)}})

(defn- shops-native-gtap
  "A native sandbox card with a `{{tier}}` template tag. Unquoted identifiers, which every supported driver resolves
  in its own case."
  []
  {:shops {:query      (mt/native-query {:query         (format "SELECT * FROM %s WHERE %s = {{tier}}"
                                                                (mt/format-name "shops")
                                                                (mt/format-name "tier"))
                                         :template-tags {"tier" {:name         "tier"
                                                                 :display-name "tier"
                                                                 :type         :number
                                                                 :required     true}}})
           :remappings {"tier" ["variable" ["template-tag" "tier"]]}}})

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

(def ^:private sandbox-configs
  "Config name -> how VENUES-style sandboxing is set up on SHOPS (and REGIONS):

  * `:gtaps`       fn returning the `met/with-gtaps!` map; evaluated inside the temp copy of the base database
  * `:attributes`  the user's login attributes
  * `:restricted`  the dataset variant that equals the sandbox applied by hand
  * `:expect`      `:restricted` (user == admin on restricted), `:unrestricted` (user == admin on base) or `:error`
  * `:hidden-cols` SHOPS columns the sandbox card leaves out
  * `:sandboxed`   tables with an enforced sandbox, for the structural detector
  * `:setup`       fn run inside `with-gtaps!` for extra arrangements (second groups, remaps)
  * `:feature`     driver feature the config needs"
  (array-map
   :table-tier      {:gtaps shops-table-gtap           :attributes {"tier" "1"} :restricted #'corpus/sandbox-corpus-tier1}
   :card-rows       {:gtaps shops-card-gtap            :attributes {"tier" "1"} :restricted #'corpus/sandbox-corpus-tier1}
   :card-join       {:gtaps shops-card-join-gtap       :attributes {"tier" "1"} :restricted #'corpus/sandbox-corpus-region-tier1}
   :card-hide-price {:gtaps shops-hide-price-gtap      :attributes {}           :restricted #'corpus/sandbox-corpus-hide-price
                     :hidden-cols #{:price :secret :secret_num}}
   :card-rows+hide  {:gtaps shops-rows+hide-price-gtap :attributes {"tier" "1"} :restricted #'corpus/sandbox-corpus-tier1-hide-price
                     :hidden-cols #{:price :secret :secret_num}}
   :native          {:gtaps shops-native-gtap          :attributes {"tier" "1"} :restricted #'corpus/sandbox-corpus-tier1
                     :feature :native-parameters}
   :two-tables      {:gtaps (fn [] (merge (shops-table-gtap) {:regions {:remappings (tier-remapping :regions)}}))
                     :attributes {"tier" "1"} :restricted #'corpus/sandbox-corpus-tier1-both :sandboxed #{:shops :regions}}
   ;; another group grants unrestricted access, so the sandbox is not enforced (by design)
   :cancelled       {:gtaps shops-table-gtap :attributes {"tier" "1"} :restricted #'corpus/sandbox-corpus :expect :unrestricted
                     :sandboxed #{} :setup #(add-rasta-to-group-with-view-data! :unrestricted)}
   ;; another group blocks the database; the sandbox must still apply
   :plus-blocked    {:gtaps shops-table-gtap :attributes {"tier" "1"} :restricted #'corpus/sandbox-corpus-tier1
                     :setup #(add-rasta-to-group-with-view-data! :blocked)}
   ;; two sandboxed groups on one table is an error, and must stay one
   :two-sandboxes   {:gtaps shops-table-gtap :attributes {"tier" "1"} :restricted #'corpus/sandbox-corpus-tier1 :expect :error
                     :setup (fn []
                              (let [group (add-rasta-to-group-with-view-data! :unrestricted)]
                                (t2/insert! :model/Sandbox {:group_id             (:id group)
                                                            :table_id             (mt/id :shops)
                                                            :attribute_remappings (tier-remapping :shops)})))}))

(defn- config-sandboxed-tables [config-k]
  (let [{:keys [sandboxed]} (sandbox-configs config-k)]
    (if (contains? (sandbox-configs config-k) :sandboxed) sandboxed #{:shops})))

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
    (mt/with-temp [:model/Card {shops-card :id}  {:dataset_query shops-q}
                   :model/Card {shops-model :id} {:dataset_query shops-q :type :model :result_metadata (model-metadata shops-q)}
                   :model/Card {orders-card :id} {:dataset_query orders-q}]
      (f {:card/shops shops-card :model/shops shops-model :card/orders-implicit orders-card}))))

(defn- allowed-canary-ids
  "The canaries an admin can see in the current (restricted) database."
  []
  (into #{}
        (mapcat (fn [table]
                  (corpus/canary-ids
                   (mt/user-http-request :crowberto :post 202 "dataset"
                                         {:database (mt/id) :type "query" :query {:source-table (mt/id table)}}))))
        [:shops :regions]))

(defn- config-supported? [config-k]
  (if-let [feature (get-in sandbox-configs [config-k :feature])]
    (driver.u/supports? driver/*driver* feature (mt/db))
    true))

(defn- do-with-corpus-config!
  "Load the base dataset and the config's restricted variant, create the cards in both, set up the sandbox on a temp
  copy of the base database as `:rasta`, and call `(f ctx)` inside it. `ctx` has `:ids` for the base copy,
  `:restricted-ids` for the restricted database, `:allowed` canaries, and the config's fields."
  [config-k f]
  (let [{:keys [gtaps attributes restricted setup expect hidden-cols] :or {expect :restricted}} (sandbox-configs config-k)]
    (mt/dataset corpus/sandbox-corpus
      (corpus/ensure-fk-metadata!)
      (let [base (mt/db)]
        (mt/dataset (var-get restricted)
          (corpus/ensure-fk-metadata!)
          (do-with-cards!
           (fn [restricted-cards]
             (let [restricted-ids (ids-map restricted-cards)
                   allowed        (allowed-canary-ids)]
               (mt/with-db base
                 (met/with-gtaps! {:gtaps (gtaps) :attributes attributes}
                   (doseq [table tables]
                     (data-perms/set-table-permission! &group (mt/id table) :perms/create-queries :query-builder))
                   (when setup (setup))
                   (do-with-cards!
                    (fn [cards]
                      (f {:config         config-k
                          :expect         expect
                          :hidden-cols    (or hidden-cols #{})
                          :sandboxed      (config-sandboxed-tables config-k)
                          :ids            (ids-map cards)
                          :restricted-ids restricted-ids
                          :allowed        allowed})))))))))))))

;;; -------------------------------------------------- Detectors ---------------------------------------------------

(defn- run-as
  "Run `inner-query` (with optional `parameters`) through the dataset API as `user` against `ids`' database.
  Returns `:rows`, `:cols` (names) and `:body`, or `:error` and `:body`."
  [user ids {:keys [query parameters]}]
  (let [outer                 (cond-> {:database (:db ids) :type "query" :query query}
                                parameters (assoc :parameters parameters))
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
  [{:keys [expect hidden-cols]} user expected admin-base]
  (let [hidden-names (set (map name hidden-cols))]
    (cond
      (= expect :error)                   (if (:error user) :user-error :missing-error)
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
  [ids {:keys [query parameters]} table-ids]
  (let [outer (json/decode+kw (json/encode (cond-> {:database (:db ids) :type "query" :query query}
                                             parameters (assoc :parameters parameters))))
        acc   (volatile! [])]
    (try
      (qp.setup/with-qp-setup [outer outer]
        (lib.walk/walk-stages
         (-> outer qp.preprocess/preprocess add/add-alias-info)
         (fn [_query path stage]
           (when-not (:query-permissions/sandboxed-table stage)
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

(defn- outcome
  "One keyword for a result, worst first, from its separate verdicts."
  [{:keys [differential canary structural]}]
  (cond
    (= :leak (:verdict canary))         :canary-leak
    (= :raw-ref (:verdict structural))  :raw-ref
    :else                               differential))

(def ^:private failing-outcomes #{:canary-leak :raw-ref :extra-rows :missing-rows :missing-error :over-restricted})

(defn- run-probe
  "Run `probe-fn`, a fn of an ids map returning `{:query inner-query, :parameters ...}`, under `ctx`. Returns a result
  map with every detector's verdict and `:outcome`."
  [{:keys [ids restricted-ids expect] :as ctx} probe-name probe-fn]
  (let [probe       (probe-fn ids)
        user        (run-as :rasta ids probe)
        admin-base  (run-as :crowberto ids probe)
        expected    (if (= expect :unrestricted)
                      admin-base
                      (run-as :crowberto restricted-ids (probe-fn restricted-ids)))
        verdicts    {:differential (differential-verdict ctx user expected admin-base)
                     :canary       (canary-verdict ctx user)
                     :structural   (structural-verdict ctx probe)}]
    (merge verdicts
           {:config   (:config ctx)
            :probe    probe-name
            :outcome  (outcome verdicts)
            :user     (-> user (dissoc :body) (update :rows #(some->> % (take 5))))
            :expected (-> expected (dissoc :body) (update :rows #(some->> % (take 5))))
            :query    probe})))

;;; ------------------------------------------------ Smoke corpus --------------------------------------------------

(defn- implicit [ids table fk target-table target-col]
  [:field (fid ids target-table target-col) {:source-field (fid ids table fk)}])

(def ^:private canonical-probes
  "Probe name -> fn of an ids map returning the inner query. One query per role of SHOPS and per source kind, written
  the way the query builder would."
  (array-map
   :shops-source            (fn [ids] {:query {:source-table (tid ids :shops)}})
   :shops-count             (fn [ids] {:query {:source-table (tid ids :shops) :aggregation [[:count]]}})
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
   :nested-shops            (fn [ids] {:query {:source-query {:source-table (tid ids :shops)}}})
   :nested-shops-name-ref   (fn [ids] {:query {:source-query {:source-table (tid ids :shops)}
                                               :fields       [[:field (fname ids :shops :secret) {:base-type :type/Text}]]}})
   :parameter-on-shops      (fn [ids] {:query      {:source-table (tid ids :orders) :aggregation [[:count]]}
                                       :parameters [{:type   "number/!="
                                                     :target ["dimension" (implicit ids :orders :shop_id :shops :price)]
                                                     :value  [-1]}]})))

(defn run-canonical!
  "Run the canonical probes under one config, or every config. Returns result maps."
  ([]
   (mapcat run-canonical! (keys sandbox-configs)))
  ([config-k]
   (do-with-corpus-config!
    config-k
    (fn [ctx]
      (doall (for [[probe-name probe-fn] canonical-probes]
               (run-probe ctx probe-name probe-fn)))))))

(defn summarize
  "Config -> outcome -> count, for reading at the REPL."
  [results]
  (into (sorted-map)
        (map (fn [[config rs]] [config (frequencies (map :outcome rs))]))
        (group-by :config results)))

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
   #'corpus/sandbox-corpus-tier1-hide-price {:shops #{}                :regions #{1 2 3 4} :shop-rows 4}})

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
    (doseq [config-k (keys sandbox-configs)]
      (when (mt/dataset corpus/sandbox-corpus (config-supported? config-k))
        (doseq [{:keys [probe outcome] :as result} (run-canonical! config-k)]
          (testing (format "%s under %s" probe config-k)
            (is (not (failing-outcomes outcome))
                (pr-str (select-keys result [:outcome :differential :canary :structural :user :expected :query])))))))))

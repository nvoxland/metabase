(ns metabase-enterprise.sandbox.enforcement-test.policies
  "The ways a sandbox can be set up on SHOPS (and REGIONS), each paired with the restricted dataset variant that equals
  it applied by hand. Besides these hand-written policies, a policy key can be a generated spec, whose restricted
  dataset is derived from it (see `policies.generated`). [[all]] is every policy, with its keys documented there; [[attribute-variants]] reruns a policy
  with other login attribute values; [[varied-datasets]] pairs a restricted variant with datasets that change only what
  it hides. The gtap builders at the top are evaluated inside the harness's temporary copy of the base database."
  (:require
   [metabase-enterprise.sandbox.enforcement-test.dataset :as dataset]
   [metabase-enterprise.sandbox.enforcement-test.policies.generated :as generated]
   [metabase.driver :as driver]
   [metabase.driver.sql :as driver.sql]
   [metabase.driver.util :as driver.u]
   [metabase.permissions.core :as perms]
   [metabase.permissions.models.data-permissions :as data-perms]
   [metabase.test :as mt]
   [toucan2.core :as t2]))

(set! *warn-on-reflection* true)

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

;;; The column sandboxes leave out exactly the columns `dataset/shops-hide-price` nulls: PRICE and the two canary
;;; columns. Every other SHOPS column stays, so the restricted dataset and the sandbox card describe one policy.

(defn- shops-hide-price-gtap []
  {:shops {:query (mt/mbql-query shops {:fields [$id $name $tier $region_id $parent_shop_id $rating $opened_on]})}})

(defn- shops-rows+hide-price-gtap []
  {:shops {:query      (mt/mbql-query shops {:fields [$id $name $tier $region_id $parent_shop_id $rating $opened_on]})
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
  (native-shops-gtap "SELECT %s, %s, %s, %s, %s, %s, %s FROM %s WHERE %s = {{tier}}"
                     "id" "name" "tier" "region_id" "parent_shop_id" "rating" "opened_on" "shops" "tier"))

(def ^:private schema-check-rejections
  "A `mu/defn` check that only runs with schema checks on; in production the same queries succeed."
  [#"^500 Invalid (input|output): \["])

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
  (into {} (for [id (range 1 9)] [(dataset/canary-num :shops id) (str "label " (dataset/canary-text :shops id))])))

(def all
  "Config name -> how VENUES-style sandboxing is set up on SHOPS (and REGIONS):

  * `:gtaps`       fn returning the `met/with-gtaps!` map; evaluated inside the temp copy of the base database
  * `:attributes`  the user's login attributes
  * `:restricted`  the dataset variant that equals the sandbox applied by hand
  * `:expect`      `:restricted` (user == admin on restricted), `:unrestricted` (user == admin on base) or `:error`
  * `:hidden`      table -> the columns the sandbox card leaves out
  * `:spec`        the same policy as a generated-policy spec (see `policies.generated`), when it is expressible as one;
    `policies.generated-test` checks that the spec derives `:restricted`
  * `:accepted-rejections` patterns of the errors that are a known way for the sandbox to fail closed (a policy
    with `:hidden-cols` must instead refuse on purpose: see `invariants/differential`); any other
    failure of the user's query alone is a finding
  * `:sandboxed`   tables with an enforced sandbox, for the structural detector
  * `:setup`       fn run inside `with-gtaps!` for extra arrangements (second groups)
  * `:remaps`      display remappings, set up in both databases; see [[do-with-remaps!]]
  * `:feature`     driver feature the config needs
  * `:persist?`    persist every model as an admin before probing
  * `:premium-features` features granted besides `:sandboxes`"
  (array-map
   :table-tier      {:gtaps shops-table-gtap           :attributes {"tier" "1"} :restricted #'dataset/shops-tier1
                     :spec {:shops {:kind :table :filters [{:column :tier :op := :value "1" :via :attribute}] :hidden #{}}}}
   :card-rows       {:gtaps shops-card-gtap            :attributes {"tier" "1"} :restricted #'dataset/shops-tier1
                     :spec {:shops {:kind :card :filters [{:column :tier :op := :value "1" :via :attribute}] :hidden #{}}}}
   :card-join       {:gtaps shops-card-join-gtap       :attributes {"tier" "1"} :restricted #'dataset/shops-in-tier1-regions
                     :spec {:shops {:kind :card-join :filters [{:column :tier :op := :value "1" :via :attribute}] :hidden #{}}}}
   ;; ratings are tier + 0.5, so rating 1.5 selects the tier 1 rows
   :table-rating    {:gtaps shops-rating-gtap :attributes {"rating" "1.5"} :restricted #'dataset/shops-tier1
                     :spec {:shops {:kind :table :filters [{:column :rating :op := :value "1.5" :via :attribute}] :hidden #{}}}}
   ;; the sandbox card itself is a persisted model: its cache holds every row
   :card-rows+persisted-sandbox {:gtaps shops-card-gtap :attributes {"tier" "1"} :restricted #'dataset/shops-tier1
                                 :setup persist-sandbox-card! :persist? true :feature :persist-models}
   :card-extra-column {:gtaps shops-card-extra-column-gtap :attributes {"tier" "1"} :restricted #'dataset/shops-tier1}
   :card-hide-price {:gtaps shops-hide-price-gtap      :attributes {}           :restricted #'dataset/shops-hide-price
                     :hidden {:shops #{:price :secret :secret_num}}
                     :spec {:shops {:kind :card :filters [] :hidden #{:price :secret :secret_num}}}}
   :card-rows+hide  {:gtaps shops-rows+hide-price-gtap :attributes {"tier" "1"} :restricted #'dataset/shops-tier1-hide-price
                     :hidden {:shops #{:price :secret :secret_num}}
                     :spec {:shops {:kind :card :filters [{:column :tier :op := :value "1" :via :attribute}]
                                    :hidden #{:price :secret :secret_num}}}}
   :native          {:gtaps shops-native-gtap          :attributes {"tier" "1"} :restricted #'dataset/shops-tier1
                     :feature :native-parameters
                     :spec {:shops {:kind :native :filters [{:column :tier :op := :value "1" :via :attribute}] :hidden #{}}}}
   :native-extra-column {:gtaps shops-native-extra-column-gtap :attributes {"tier" "1"}
                         :restricted #'dataset/shops-tier1 :feature :native-parameters
                         :accepted-rejections schema-check-rejections}
   :native-hide-price {:gtaps shops-native-hide-price-gtap :attributes {"tier" "1"}
                       :restricted #'dataset/shops-tier1-hide-price :feature :native-parameters
                       :hidden {:shops #{:price :secret :secret_num}}
                       :spec {:shops {:kind :native :filters [{:column :tier :op := :value "1" :via :attribute}]
                                      :hidden #{:price :secret :secret_num}}}}
   :two-tables      {:gtaps (fn [] (merge (shops-table-gtap) {:regions {:remappings (tier-remapping :regions)}}))
                     :attributes {"tier" "1"} :restricted #'dataset/shops-tier1-regions-tier1 :sandboxed #{:shops :regions}
                     :spec {:shops   {:kind :table :filters [{:column :tier :op := :value "1" :via :attribute}] :hidden #{}}
                            :regions {:kind :table :filters [{:column :tier :op := :value "1" :via :attribute}] :hidden #{}}}}
   ;; ORDERS.SHOP_ID displays SHOPS.NAME: remaps add implicit joins to the sandboxed table in the outer query
   :table-tier+remap {:gtaps shops-table-gtap :attributes {"tier" "1"} :restricted #'dataset/shops-tier1
                      :remaps [[:orders :shop_id :shops :name]]}
   ;; every model persisted by an admin: the cache tables hold every row and column
   :table-tier+persisted {:gtaps shops-table-gtap :attributes {"tier" "1"} :restricted #'dataset/shops-tier1
                          :persist? true :feature :persist-models}
   :card-hide-price+persisted {:gtaps shops-hide-price-gtap :attributes {} :restricted #'dataset/shops-hide-price
                               :hidden {:shops #{:price :secret :secret_num}}
                               :persist? true :feature :persist-models}
   ;; SHOPS.SECRET_NUM displays admin-entered labels, one per row, from FieldValues of the whole table
   :table-tier+value-remap {:gtaps shops-table-gtap :attributes {"tier" "1"} :restricted #'dataset/shops-tier1
                            :remaps [[:shops :secret_num secret-num-labels]]}
   ;; another group grants unrestricted access, so the sandbox is not enforced (by design)
   :cancelled       {:gtaps shops-table-gtap :attributes {"tier" "1"} :restricted #'dataset/shops :expect :unrestricted
                     :sandboxed #{} :setup #(add-rasta-to-group-with-view-data! :unrestricted)}
   ;; another group blocks the database; the sandbox must still apply
   :plus-blocked    {:gtaps shops-table-gtap :attributes {"tier" "1"} :restricted #'dataset/shops-tier1
                     :setup #(add-rasta-to-group-with-view-data! :blocked)}
   ;; another group impersonates a database role: sandbox and impersonation on one database conflict, an error
   :plus-impersonation {:gtaps shops-table-gtap :attributes {"tier" "1" "role" "corpus_role"}
                        :restricted #'dataset/shops-tier1 :expect :error
                        :feature :connection-impersonation :premium-features #{:advanced-permissions}
                        :setup (fn []
                                 (let [group (add-rasta-to-group-with-view-data! :unrestricted)]
                                   (t2/insert! :model/ConnectionImpersonation {:db_id     (mt/id)
                                                                               :group_id  (:id group)
                                                                               :attribute "role"})))}
   ;; two sandboxed groups on one table is an error, and must stay one
   :two-sandboxes   {:gtaps shops-table-gtap :attributes {"tier" "1"} :restricted #'dataset/shops-tier1 :expect :error
                     :setup (fn []
                              (let [group (add-rasta-to-group-with-view-data! :unrestricted)]
                                (t2/insert! :model/Sandbox {:group_id             (:id group)
                                                            :table_id             (mt/id :shops)
                                                            :attribute_remappings (tier-remapping :shops)})))}))

(def attribute-variants
  "Variant name -> a base config from [[policies/all]] run with other login attributes. Attribute values reach the
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
   :native-list       {:base :native     :attributes {"tier" ["3"]}      :restricted #'dataset/shops-empty
                       :accepted-rejections schema-check-rejections}
   :rating-integer    {:base :table-rating :attributes {"rating" "1"}   :restricted #'dataset/shops-empty
                       :spec {:shops {:kind :table :filters [{:column :rating :op := :value "1" :via :attribute}] :hidden #{}}}}
   :rating-junk       {:base :table-rating :attributes {"rating" "1.5x"} :expect :error}
   :rating-list       {:base :table-rating :attributes {"rating" ["2.5"]} :expect :error}
   :native-sql-like   {:base :native     :attributes {"tier" "1 OR 1=1"} :expect :error}
   :native-missing    {:base :native     :attributes {}                  :expect :error}))

(defn policy
  "The config named `policy-k`: a key of [[policies/all]] or [[attribute-variants]], or a generated-policy spec (a map;
  see `policies.generated`)."
  [policy-k]
  (cond
    (map? policy-k) (generated/policy policy-k)
    (all policy-k)  (all policy-k)
    :else           (let [{:keys [base] :as variant} (attribute-variants policy-k)]
                      ;; a variant's spec, if any, is its own: the base's describes other attributes
                      (merge (dissoc (all base) :spec) (dissoc variant :base)))))

(defn sandboxed-tables
  "The tables with an enforced sandbox under `policy-k`, for the structural invariant."
  [policy-k]
  (let [config (policy policy-k)]
    (if (contains? config :sandboxed) (:sandboxed config) #{:shops})))

(defn supported?
  "Does the current driver support the feature `policy-k` needs? Impersonation also needs a default database role,
  which MySQL and MariaDB only have when the connection details name one."
  [policy-k]
  (let [feature (:feature (policy policy-k))]
    (and (or (nil? feature) (driver.u/supports? driver/*driver* feature (mt/db)))
         (or (not= feature :connection-impersonation)
             (some? (driver.sql/default-database-role driver/*driver* (mt/db)))))))

(def varied-datasets
  "Restricted dataset -> the varied datasets that change only data it hides."
  {#'dataset/shops-tier1            [#'dataset/shops-tier2-varied]
   #'dataset/shops-tier1-regions-tier1       [#'dataset/shops-tier2-varied]
   #'dataset/shops-hide-price       [#'dataset/shops-hidden-cols-varied]
   #'dataset/shops-tier1-hide-price [#'dataset/shops-tier2-varied #'dataset/shops-hidden-cols-varied]})

(defn varied
  "The varied datasets for `policy-k`: derived from its spec for a generated policy, else from [[varied-datasets]]."
  [policy-k]
  (let [{:keys [restricted] varied-dataset :varied} (policy policy-k)]
    (if varied-dataset [varied-dataset] (varied-datasets restricted))))

;;; --------------------------------------------------- Scopes -----------------------------------------------------

(defn full-run?
  "Run both schema-check modes on every policy, as `MB_SANDBOX_FULL=true` asks for locally. CI runs production mode on
  every policy and test mode on two representative ones: schema checks quadruple a query's cost, and production has
  none."
  []
  (= "true" (System/getenv "MB_SANDBOX_FULL")))

(def scopes
  "Scope name -> the modes and policies a processor test runs under; see [[scope]]."
  {:ci         {:modes [:production] :policies (keys all)}
   :test-mode  {:modes [:test] :policies [:table-tier :card-hide-price]}
   :attributes {:modes [:production] :policies (keys attribute-variants)}
   ;; every kind of row sandbox and every column sandbox, for the content the sandboxed user writes
   :authored   {:modes [:production] :policies [:table-tier :table-tier+remap :card-rows :card-join :native
                                                :card-hide-price :card-rows+hide :native-hide-price]}
   ;; the saved-question policies, whose sandbox card has metadata to forge or to edit
   :card-policies {:modes [:production] :policies [:card-rows :card-hide-price :native :native-hide-price]}})

(defn scope
  "The modes and policies for scope `k`, widened to both modes on every policy for a full run. `:generated-policies`
  is the sample of generated specs, drawn when asked so the environment knobs apply."
  [k]
  (cond-> (if (= k :generated-policies)
            {:modes [:production] :policies (generated/in-scope)}
            (scopes k))
    (full-run?) (assoc :modes [:production :test])))

;;; ------------------------------------------------ Several users -------------------------------------------------

(def second-users
  "Policy -> the second sandboxed user who runs alongside `:rasta` under the same policy: a test user, their login
  attributes, and the restricted variant that is their specification. Both users' sandboxes are installed on the same
  copy of the database, in separate groups, so state either one leaves behind (caches, recorded metadata, persisted
  models, field values) can reach the other."
  {:table-tier           {:user :lucky :attributes {"tier" "2"} :restricted #'dataset/shops-tier2}
   :card-rows            {:user :lucky :attributes {"tier" "2"} :restricted #'dataset/shops-tier2}
   :native               {:user :lucky :attributes {"tier" "2"} :restricted #'dataset/shops-tier2}
   :card-rows+hide       {:user :lucky :attributes {"tier" "2"} :restricted #'dataset/shops-tier2-hide-price}
   :table-tier+persisted {:user :lucky :attributes {"tier" "2"} :restricted #'dataset/shops-tier2}})

(ns ^:mb/driver-tests metabase-enterprise.sandbox.enforcement-test.entrypoints-test
  "Sandbox enforcement at the entry points, the hypothesis of the processor theorem: every door the sandboxed user can
  take either leads to the processor with the right context, or is checked on its own. [[entry-points]] classifies
  each one:

  * `:routed` -- hands the processor a context it builds (a card, a dashcard, a download, a pivot). Tested by sending
    a probe through it and requiring the response to equal the in-process result ([[invariants/http-equivalence]]),
    or, where the response is not a result set, by scanning it for canaries and hygiene.
  * `:bypassing` -- reads data or metadata without the processor (field values and search, table and database
    metadata, parameter values, x-rays). Tested by scanning its response for canaries of hidden rows and for hygiene.
  * `:setup` -- produces the enforced sandboxes the processor sees (group combinations, login attribute coercion).
    Tested by the processor tests under those policies; the test here asserts they are in scope.

  [[untested-entry-points]] names the doors this suite does not open, and why. A test keeps the two lists complete
  and disjoint. The base database gets one full sync first, so fingerprints and field values exist as they would in a
  real instance."
  (:require
   [clojure.set :as set]
   [clojure.test :refer :all]
   [metabase-enterprise.sandbox.enforcement-test.dataset :as dataset]
   [metabase-enterprise.sandbox.enforcement-test.harness :as harness]
   [metabase-enterprise.sandbox.enforcement-test.invariants :as invariants]
   [metabase-enterprise.sandbox.enforcement-test.policies :as policies]
   [metabase-enterprise.sandbox.enforcement-test.queries.hand-written :as hand-written]
   [metabase.permissions.core :as perms]
   [metabase.test :as mt]
   [metabase.util.json :as json]
   [toucan2.core :as t2]))

(set! *warn-on-reflection* true)

;;; ---------------------------------------------- Requests per entry point ----------------------------------------

(defn- do-with-dashboard!
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
                                                         :parameter_mappings (mappings orders-card #(hand-written/implicit ids :orders :shop_id :shops %))}
                   :model/DashboardCard {shops-dc :id} {:dashboard_id       dash-id
                                                        :card_id            shops-card
                                                        :parameter_mappings (mappings shops-card #(vector :field (dataset/fid ids :shops %) nil))}]
      (f {:id dash-id :dashcards {:card/orders-implicit [orders-dc orders-card] :card/shops [shops-dc shops-card]}}))))

(defn- request
  "`[method path body?]` for entry point `k`, against the base copy `ids` and the dashboard of [[do-with-dashboard!]].
  Tier 2 is hidden by the row sandboxes."
  [k ids {dash :id :keys [dashcards]}]
  (let [shops        (dataset/tid ids :shops)
        secret       (dataset/fid ids :shops :secret)
        secret-num   (dataset/fid ids :shops :secret_num)
        price        (dataset/fid ids :shops :price)
        model        (get-in ids [:cards :model/shops])
        card         (get-in ids [:cards :card/shops])
        canonical    (hand-written/implicit ids :orders :shop_id :shops :secret)
        orders-q     {:database (:db ids) :type "query"
                      :query    {:source-table (dataset/tid ids :orders) :fields [[:field (dataset/fid ids :orders :id) nil] canonical]}}
        run-dashcard (fn [card-k params]
                       (let [[dashcard card] (dashcards card-k)]
                         [:post (format "dashboard/%d/dashcard/%d/card/%d/query" dash dashcard card) {:parameters params}]))
        hidden-tier  [{:id "tier" :type "number/=" :value [2]}]]
    (case k
      :download-csv             [:post "dataset/csv" {:query (json/encode orders-q)}]
      :download-json            [:post "dataset/json" {:query (json/encode orders-q)}]
      :pivot                    [:post "dataset/pivot"
                                 {:database (:db ids) :type "query"
                                  :query    {:source-table (dataset/tid ids :orders) :breakout [canonical] :aggregation [["count"]]}}]
      :pivot-secret-num         [:post "dataset/pivot"
                                 {:database (:db ids) :type "query"
                                  :query    {:source-table (dataset/tid ids :orders)
                                             :breakout     [(hand-written/implicit ids :orders :shop_id :shops :secret_num)]
                                             :aggregation  [["count"]]}}]
      :card-run-model           [:post (format "card/%d/query" model)]
      :card-run-model-csv       [:post (format "card/%d/query/csv" model)]
      :dashcard-orders-hidden-tier (run-dashcard :card/orders-implicit hidden-tier)
      :dashcard-shops-hidden-tier  (run-dashcard :card/shops hidden-tier)
      :binning-breakout         [:post "dataset"
                                 {:database (:db ids) :type "query"
                                  :query    {:source-table shops
                                             :breakout     [[:field price {:binning {:strategy :default}}]]
                                             :aggregation  [["count"]]}}]
      :binning-secret-num       [:post "dataset"
                                 {:database (:db ids) :type "query"
                                  :query    {:source-table shops
                                             :breakout     [[:field secret-num {:binning {:strategy :default}}]]
                                             :aggregation  [["count"]]}}]
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
      :card-get-model           [:get (format "card/%d" model)]
      :card-get                 [:get (format "card/%d" card)]
      :card-query-metadata      [:get (format "card/%d/query_metadata" model)]
      :table-card-metadata      [:get (format "table/card__%d/query_metadata" model)]
      :dashboard-get            [:get (format "dashboard/%d" dash)]
      :dashboard-query-metadata [:get (format "dashboard/%d/query_metadata" dash)]
      :dashboard-param-values   [:get (format "dashboard/%d/params/secret/values" dash)]
      :dashboard-param-search   [:get (format "dashboard/%d/params/secret/search/CANARY" dash)]
      :dashboard-chain-filter   [:get (format "dashboard/%d/params/secret/values?tier=2" dash)]
      :dashboard-chain-filter-tier [:get (format "dashboard/%d/params/tier/values?secret=%s" dash (dataset/canary-text :shops 2))]
      :xray-table               [:get (format "automagic-dashboards/table/%d" (dataset/tid ids :shops))]
      :xray-field-secret-num    [:get (format "automagic-dashboards/field/%d" (dataset/fid ids :shops :secret_num))])))

;;; --------------------------------------------- The entry point table --------------------------------------------

(def ^:private routed-by-transport
  "Routed entry points that return a result set, tested by equivalence with the in-process result: entry point ->
  the harness transport that sends a probe through it."
  {:dataset        [:http :dataset]
   :card-query     [:http :card]
   :dashcard-query [:http :dashcard]})

(def ^:private routed-by-scan
  "Routed entry points whose response is not a plain result set (a file, a pivot, a model run), or that fix the query
  themselves; tested by scanning the response. Each is a key of [[request]]."
  #{:download-csv :download-json :pivot :pivot-secret-num :card-run-model :card-run-model-csv
    :dashcard-orders-hidden-tier :dashcard-shops-hidden-tier :binning-breakout :binning-secret-num})

(def ^:private bypassing
  "Entry points that read data or metadata without the processor. Each is a key of [[request]]."
  #{:field-secret-num :field-secret :field-values-secret :field-values-secret-num :field-search-secret
    :field-summary-secret-num :field-related :table-query-metadata :table :table-fks :database-metadata
    :database-fields :database-idfields :parameter-values :parameter-search :card-get-model :card-get
    :card-query-metadata :table-card-metadata :dashboard-get :dashboard-query-metadata :dashboard-param-values
    :dashboard-param-search :dashboard-chain-filter :dashboard-chain-filter-tier :xray-table :xray-field-secret-num})

(def ^:private setup
  "Entry points that decide which sandboxes the processor sees, and the policies the processor tests cover them with.
  `:sandbox-card-edits` is `PUT /api/card/:id` on a sandbox card by a sandboxed user who curates its collection; its
  own test is [[sandbox-card-edits-by-a-curator-test]]."
  {:group-combinations  {:policies [:cancelled :plus-blocked :plus-impersonation :two-sandboxes]}
   :attribute-coercion  {:policies (keys policies/attribute-variants)}
   :sandbox-card-edits  {:policies [:card-rows :card-hide-price :native :native-hide-price]}})

(def entry-points
  "Every entry point this suite tests: name -> `{:kind :routed|:bypassing|:setup}` plus how it is tested (`:transport`,
  `:request`, or `:policies`)."
  (merge (into {} (for [[k t] routed-by-transport] [k {:kind :routed :transport t}]))
         (into {} (for [k routed-by-scan] [k {:kind :routed :request true}]))
         (into {} (for [k bypassing] [k {:kind :bypassing :request true}]))
         (into {} (for [[k m] setup] [k (assoc m :kind :setup)]))))

(def untested-entry-points
  "Entry points this suite does not open, and why."
  {:mcp-and-metabot      "no HTTP contract to replay; they reach the processor through the card and ad-hoc paths tested here"
   :api-keys             "authentication, not data access; the same handlers run"
   :public-and-embed     "run as the card's creator by design, so no sandbox applies; documented, with its own suite"
   :subscriptions-alerts "rendered as the recipient out of band; metabase-enterprise.sandbox.pulse-test owns them"
   :actions              "the write path; metabase-enterprise.sandbox.actions-test owns it"
   :search               "returns metadata, not rows, from an index built as an admin: a separate design question"
   :documents            "no endpoint that reads table data"
   :database-routing     "routes a whole database; orthogonal to row and column rules, with its own suite"})

;;; ---------------------------------------------------- Scans -----------------------------------------------------

(defn- request-as [user [method path body]]
  (let [{:keys [status] response :body} (if body
                                          (mt/user-http-request-full-response user method path body)
                                          (mt/user-http-request-full-response user method path))]
    {:status status :body response}))

(defn scan-entry-points!
  "Call every scanned entry point (`:request` ones) as the sandboxed user under `policy-k`. Returns entry point ->
  `{:status :leaked :hygiene}`, where `:leaked` is the canary ids outside the sandbox."
  [policy-k]
  (harness/do-with-policy!
   policy-k
   (fn [{:keys [ids allowed] :as ctx}]
     (do-with-dashboard!
      ids
      (fn [dashboard]
        (into (sorted-map)
              (for [[k {scanned? :request}] entry-points
                    :when scanned?]
                (let [{:keys [status body]} (request-as :rasta (request k ids dashboard))]
                  [k {:status  status
                      :leaked  (not-empty (into (sorted-set) (remove allowed) (dataset/canary-ids body)))
                      :hygiene (invariants/hygiene ctx {:body body})}]))))))))

(def ^:private scanned-policies [:table-tier :card-hide-price :table-tier+value-remap])

;;; --------------------------------------------------- Tests ------------------------------------------------------

(deftest ^:parallel every-entry-point-is-classified-or-excused-test
  (testing "the tested and untested lists are disjoint, every tested entry point has a kind and a way to be tested"
    (is (empty? (set/intersection (set (keys entry-points)) (set (keys untested-entry-points)))))
    (doseq [[k {:keys [kind transport request policies]}] entry-points]
      (testing k
        (is (#{:routed :bypassing :setup} kind))
        (is (or transport request policies))))))

(deftest ^:synchronized routed-entry-points-equal-the-processor-test
  (testing "A result-set entry point returns what the processor returns in-process, and passes every invariant"
    (mt/test-drivers (harness/drivers)
      (doseq [policy-k scanned-policies
              [k {:keys [transport]}] entry-points
              :when transport
              :let [probes (cond-> hand-written/roles (= k :dashcard-query) (merge hand-written/parameters))]
              result (harness/do-with-policy!
                      policy-k
                      (fn [ctx]
                        (doall (for [[probe-name probe-fn] probes]
                                 (harness/run-probe (assoc ctx :transport transport) probe-name probe-fn)))))]
        (testing (format "%s through %s under %s" (:probe result) k policy-k)
          (is (not (invariants/unexpected-outcome? (:probe result) (:outcome result)))
              (pr-str (select-keys result [:outcome :http-equivalence :differential :canary :hygiene :user :http :query]))))))))

(deftest ^:synchronized scanned-entry-points-reveal-nothing-outside-the-sandbox-test
  (mt/test-drivers #{:h2}
    (doseq [policy-k scanned-policies
            [k {:keys [status leaked hygiene]}] (scan-entry-points! policy-k)]
      (testing (format "%s (%s) under %s (HTTP %s)" k (:kind (entry-points k)) policy-k status)
        (testing "no canary of a hidden row"
          (is (nil? leaked)))
        (testing "nothing internal in the response"
          (is (= :clean (:verdict hygiene)) (pr-str hygiene)))))))

(deftest ^:parallel setup-entry-points-are-covered-by-the-processor-tests-test
  (testing "every policy a setup entry point names is in the processor tests' CI scope"
    (let [in-scope (set (concat (:policies (policies/scope :ci)) (:policies (policies/scope :attributes))))]
      (doseq [[k {:keys [policies]}] setup]
        (testing k
          (is (every? in-scope policies)))))))

(defn mapped-model-filter-values!
  "Under `policy-k`, the canaries the sandboxed user gets from a dashboard filter on their model column that is mapped
  to SHOPS.SECRET (see `:model-mapped-to-shops`), by filter target and endpoint. Returns `[target endpoint leaked]`,
  where `leaked` is the canary ids outside the sandbox."
  [policy-k]
  (harness/do-with-policy!
   policy-k
   (fn [{:keys [ids allowed]}]
     (let [{:keys [card] :as created} ((:author (hand-written/authored :model-mapped-to-shops)) ids :rasta)]
       (try
         (doall
          (for [[target-k target] {:name-ref ["field" (dataset/fname ids :orders :name) {:base-type "type/Text"}]
                                   :id-ref   ["field" (dataset/fid ids :shops :secret) nil]}
                [endpoint leaked] (mt/with-temp [:model/Dashboard {dash :id} {:parameters [{:id "p" :slug "p" :name "p" :type "string/="}]}
                                                 :model/DashboardCard _ {:dashboard_id       dash
                                                                         :card_id            card
                                                                         :parameter_mappings [{:parameter_id "p" :card_id card
                                                                                               :target       ["dimension" target]}]}]
                                    (doall (for [[endpoint path] {:values (format "dashboard/%d/params/p/values" dash)
                                                                  :search (format "dashboard/%d/params/p/search/CANARY" dash)}]
                                             [endpoint (not-empty (into (sorted-set) (remove allowed)
                                                                        (dataset/canary-ids (mt/user-http-request :rasta :get 200 path))))])))]
            [target-k endpoint leaked]))
         (finally
           (hand-written/delete-authored! created)))))))

(deftest ^:synchronized mapped-model-column-filter-values-are-sandboxed-test
  (testing "A dashboard filter on a model column mapped to a sandboxed column offers only values the sandbox allows"
    (mt/test-drivers #{:h2}
      (doseq [policy-k                  [:table-tier :card-hide-price]
              [target endpoint leaked] (mapped-model-filter-values! policy-k)]
        (testing (format "%s %s under %s" target endpoint policy-k)
          (is (nil? leaked)))))))

(deftest ^:synchronized sandboxed-users-cannot-create-segments-test
  (testing "Segments are admin content, so a sandboxed user cannot define one over the sandboxed table"
    (mt/test-drivers #{:h2}
      (harness/do-with-policy!
       :table-tier
       (fn [{:keys [ids]}]
         (is (= "You don't have permissions to do that."
                (mt/user-http-request :rasta :post 403 "segment"
                                      {:name       "tier 2"
                                       :table_id   (dataset/tid ids :shops)
                                       :definition (assoc-in (hand-written/table-query ids :shops) [:query :filter]
                                                             ["=" ["field" (dataset/fid ids :shops :tier) nil] 2])}))))))))

;;; ------------------------------------------- Edits to a sandbox card -------------------------------------------

(def ^:private sandbox-card-edits
  "Edit name -> `[expected-status body-fn]`, `body-fn` a fn of an ids map returning the `PUT /api/card/:id` body. The
  guard refuses a non-admin's change to what a sandbox reads (`:dataset_query`, `:result_metadata`, `:archived`) on
  any card in the sandbox dependency set. The rest is accepted, `:type` included; the probes then show whether any
  accepted edit changes what the sandbox lets through."
  (let [shops-md #(hand-written/model-metadata (hand-written/table-query % :shops))]
    (array-map
     :result-metadata     [403 (fn [ids] {:result_metadata (shops-md ids)})]
     :dataset-query       [403 (fn [ids] {:dataset_query (hand-written/table-query ids :shops)})]
     :archive             [403 (fn [_] {:archived true})]
     :to-model-with-metadata [403 (fn [ids] {:type "model" :result_metadata (shops-md ids)})]
     :rename              [200 (fn [_] {:name "renamed by a curator"})]
     :move                [200 (fn [_] {:collection_id nil})]
     :to-model            [#{200 400} (fn [_] {:type "model"})]
     :to-metric           [200 (fn [_] {:type "metric"})])))

(defn- do-with-curated-sandbox-card!
  "Put the current SHOPS sandbox card in a collection All Users curate, call `(f card-id)`, then restore the card and
  delete the collection. Setup and teardown run as an admin: the guard would refuse them as the sandboxed user."
  [f]
  (let [card-id (t2/select-one-fn :card_id :model/Sandbox :table_id (mt/id :shops))
        card    (t2/select-one :model/Card :id card-id)
        coll    (mt/with-test-user :crowberto
                  (:id (t2/insert-returning-instance! :model/Collection {:name (str "sandbox questions " (random-uuid))})))]
    (try
      (mt/with-test-user :crowberto
        (t2/update! :model/Card card-id {:collection_id coll})
        ;; a new collection may inherit a grant from its parent; replace it
        (t2/delete! :model/Permissions :group_id (:id (perms/all-users-group)) :collection_id coll)
        (perms/grant-collection-readwrite-permissions! (perms/all-users-group) coll))
      (f card-id)
      (finally
        (mt/with-test-user :crowberto
          (t2/update! :model/Card card-id (select-keys card [:collection_id :archived :type :name]))
          (t2/delete! :model/Collection :id coll))))))

(deftest ^:synchronized sandbox-card-edits-by-a-curator-test
  (testing "A sandboxed user who curates the sandbox card's collection cannot change what the sandbox reads, and no edit they can make widens it"
    (mt/test-drivers #{:h2}
      (doseq [policy-k                          (:policies (setup :sandbox-card-edits))
              [edit-k [expected-status body-fn]] sandbox-card-edits]
        (harness/in-mode
         :production
         #(harness/do-with-policy!
           policy-k
           (fn [{:keys [ids] :as ctx}]
             (do-with-curated-sandbox-card!
              (fn [card-id]
                (let [status (:status (mt/user-http-request-full-response :rasta :put (str "card/" card-id) (body-fn ids)))]
                  (testing (format "%s on the sandbox card under %s" edit-k policy-k)
                    (is ((if (set? expected-status) expected-status #{expected-status}) status))
                    (doseq [[probe-name probe-fn] hand-written/all
                            :let [{:keys [outcome] :as result} (harness/run-probe (assoc ctx :transport :in-process) probe-name probe-fn)]]
                      (testing (format "then %s" probe-name)
                        (is (not (invariants/leak-outcomes outcome))
                            (pr-str (select-keys result [:outcome :differential :canary :user :expected]))))))))))))))))

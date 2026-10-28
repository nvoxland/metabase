(ns metabase-enterprise.sandbox.enforcement-test.entry-points
  "The entry points the sandboxed user can reach data through, as data: [[entry-points]] classifies each as `:routed`
  (hands the processor a context), `:bypassing` (reads without the processor) or `:setup` (decides which sandboxes
  apply), [[request]] builds the request for each scanned one, and [[untested-entry-points]] names the doors the suite
  does not open, with the reason. [[scan!]] calls every scanned entry point as one user. The tests are in
  `entrypoints-test` and, for several users at once, `multi-user-test`."
  (:require
   [metabase-enterprise.sandbox.enforcement-test.dataset :as dataset]
   [metabase-enterprise.sandbox.enforcement-test.harness :as harness]
   [metabase-enterprise.sandbox.enforcement-test.invariants :as invariants]
   [metabase-enterprise.sandbox.enforcement-test.policies :as policies]
   [metabase-enterprise.sandbox.enforcement-test.queries.hand-written :as hand-written]
   [metabase.test :as mt]
   [metabase.util.json :as json]))

(set! *warn-on-reflection* true)

;;; ---------------------------------------------- Requests per entry point ----------------------------------------

(defn do-with-dashboard!
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

(defn request
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

(def routed-by-transport
  "Routed entry points that return a result set, tested by equivalence with the in-process result: entry point ->
  the harness transport that sends a probe through it."
  {:dataset        [:http :dataset]
   :card-query     [:http :card]
   :dashcard-query [:http :dashcard]})

(def routed-by-scan
  "Routed entry points whose response is not a plain result set (a file, a pivot, a model run), or that fix the query
  themselves; tested by scanning the response. Each is a key of [[request]]."
  #{:download-csv :download-json :pivot :pivot-secret-num :card-run-model :card-run-model-csv
    :dashcard-orders-hidden-tier :dashcard-shops-hidden-tier :binning-breakout :binning-secret-num})

(def bypassing
  "Entry points that read data or metadata without the processor. Each is a key of [[request]]."
  #{:field-secret-num :field-secret :field-values-secret :field-values-secret-num :field-search-secret
    :field-summary-secret-num :table-query-metadata :table :table-fks :database-metadata
    :database-fields :database-idfields :parameter-values :parameter-search :card-get-model :card-get
    :card-query-metadata :table-card-metadata :dashboard-get :dashboard-query-metadata :dashboard-param-values
    :dashboard-param-search :dashboard-chain-filter :dashboard-chain-filter-tier :xray-table :xray-field-secret-num})

(def setup
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
  {:field-related        "returns a few related fields picked by a tie-break on field ids, so whether its response holds a hidden column's fingerprint changes between setups; that fingerprint leak is checked deterministically through :field-secret-num"
   :mcp-and-metabot      "no HTTP contract to replay; they reach the processor through the card and ad-hoc paths tested here"
   :api-keys             "authentication, not data access; the same handlers run"
   :public-and-embed     "run as the card's creator by design, so no sandbox applies; documented, with its own suite"
   :subscriptions-alerts "rendered as the recipient out of band; metabase-enterprise.sandbox.pulse-test owns them"
   :actions              "the write path; metabase-enterprise.sandbox.actions-test owns it"
   :search               "returns metadata, not rows, from an index built as an admin: a separate design question"
   :documents            "no endpoint that reads table data"
   :database-routing     "routes a whole database; orthogonal to row and column rules, with its own suite"})

;;; ---------------------------------------------------- Scans -----------------------------------------------------

(defn request-as
  "`[method path body?]` as `user`: `{:status :body}`."
  [user [method path body]]
  (let [{:keys [status] response :body} (if body
                                          (mt/user-http-request-full-response user method path body)
                                          (mt/user-http-request-full-response user method path))]
    {:status status :body response}))

(defn scan!
  "Call every scanned entry point (`:request` ones) as `(:user ctx)` (default `:rasta`) against the dashboard of
  [[do-with-dashboard!]]. Returns entry point -> `{:status :leaked :hygiene}`, where `:leaked` is the canary ids
  outside that user's sandbox."
  [{:keys [ids allowed user] :or {user :rasta} :as ctx} dashboard]
  (into (sorted-map)
        (for [[k {scanned? :request}] entry-points
              :when scanned?]
          (let [{:keys [status body]} (request-as user (request k ids dashboard))]
            [k {:status  status
                :leaked  (not-empty (into (sorted-set) (remove allowed) (dataset/canary-ids body)))
                :hygiene (invariants/hygiene ctx {:body body})}]))))

(defn scan-entry-points!
  "[[scan!]] as the sandboxed user under `policy-k`."
  [policy-k]
  (harness/do-with-policy!
   policy-k
   (fn [{:keys [ids] :as ctx}]
     (do-with-dashboard! ids #(scan! ctx %)))))

(def scanned-policies
  "The policies the scanned entry points run under." [:table-tier :card-hide-price :table-tier+value-remap])

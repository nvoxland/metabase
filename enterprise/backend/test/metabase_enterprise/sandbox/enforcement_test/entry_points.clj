(ns metabase-enterprise.sandbox.enforcement-test.entry-points
  "The entry points the sandboxed user can reach data through, as data: [[entry-points]] classifies each as `:routed`
  (hands the processor a context), `:bypassing` (reads without the processor) or `:setup` (decides which sandboxes
  apply), [[request]] builds the request for each scanned one, and [[untested-entry-points]] names the doors the suite
  does not open, with the reason. [[scan!]] calls every scanned entry point as one user. The tests are in
  `entrypoints-test` and, for several users at once, `multi-user-test`."
  (:require
   [clojure.walk :as walk]
   [metabase-enterprise.sandbox.enforcement-test.dataset :as dataset]
   [metabase-enterprise.sandbox.enforcement-test.harness :as harness]
   [metabase-enterprise.sandbox.enforcement-test.invariants :as invariants]
   [metabase-enterprise.sandbox.enforcement-test.policies :as policies]
   [metabase-enterprise.sandbox.enforcement-test.queries.hand-written :as hand-written]
   [metabase.mcp.v2.test-util :as mcp.tu]
   [metabase.sync.core :as sync]
   [metabase.test :as mt]
   [metabase.test.http-client :as client]
   [metabase.util.json :as json]
   [toucan2.core :as t2]))

(set! *warn-on-reflection* true)

;;; ---------------------------------------------- Requests per entry point ----------------------------------------

(defn- admin-metadata
  "The result metadata of an admin's run of `query`, which the query builder saves with a question: fingerprints of
  the whole table, an aggregation's and a native column's included."
  [query]
  (get-in (mt/user-http-request :crowberto :post 202 "dataset" query) [:data :results_metadata :columns]))

(defn- do-with-fingerprinted-pk!
  "Call `thunk` with SHOPS.ID fingerprinted, as it is when an admin marks a column as the entity key after sync
  fingerprinted it (a view, or a table without a key constraint). The loader's tables have one, so sync skips the
  column, and the fingerprinter gives a key nothing: fingerprint it as a category, then type it."
  [ids thunk]
  (let [field-id (dataset/fid ids :shops :id)]
    (t2/update! :model/Field field-id {:semantic_type :type/Category})
    (sync/refingerprint-field! (t2/select-one :model/Field :id field-id))
    (t2/update! :model/Field field-id {:semantic_type :type/PK})
    (try
      (thunk)
      (finally
        (t2/update! :model/Field field-id {:fingerprint nil})))))

(defn- do-with-segment!
  "Call `(f segment-id)` with an admin's segment on SHOPS. Not `with-temp`: its defaults read a table of another
  dataset."
  [ids f]
  (let [shops (dataset/tid ids :shops)
        id    (t2/insert-returning-pk! :model/Segment {:name       "tier 1 shops"
                                                       :creator_id (mt/user->id :crowberto)
                                                       :table_id   shops
                                                       :definition {:source-table shops
                                                                    :filter       [:= [:field (dataset/fid ids :shops :tier) nil] 1]}})]
    (try
      (f id)
      (finally
        (t2/delete! :model/Segment :id id)))))

(defn do-with-dashboard!
  "Create the content the scans read, and call `(f fixture)`. A dashboard shows the SHOPS card, with two series, and
  the ORDERS card with implicit SHOPS columns; its `secret` and `tier` parameters are wired to SHOPS directly and
  through the implicit join, and its `shop` parameter to the ORDERS foreign key, so it hydrates the key's target and
  name fields. The series are saved questions with an admin's result metadata: an aggregation of SHOPS and a native
  query on it, whose columns name no table. A metric and a segment on SHOPS, and SHOPS.ID fingerprinted. `fixture`
  has the dashboard `:id`, `:dashcards` (card key -> `[dashcard-id card-id]`), `:cards` (`:summary`, `:native`,
  `:metric`), `:segment`, and `:queries`, those cards' queries, whose fingerprints the scan compares with the user's
  own."
  [ids f]
  (let [orders-card (get-in ids [:cards :card/orders-implicit])
        shops-card  (get-in ids [:cards :card/shops])
        shops       (dataset/tid ids :shops)
        field       #(vector :field (dataset/fid ids :shops %) nil)
        summary-q   {:database (:db ids) :type :query
                     :query    {:source-table shops
                                :aggregation  [[:count] [:sum (field :price)] [:max (field :secret_num)]]
                                :breakout     [(field :tier)]}}
        metric-q    {:database (:db ids) :type :query
                     :query    {:source-table shops :aggregation [[:max (field :secret_num)]]}}
        native-q    {:database (:db ids) :type :native
                     :native   {:query (format "SELECT %s, COUNT(*) AS N, MAX(%s) AS M FROM %s GROUP BY %s"
                                               (mt/format-name "tier") (mt/format-name "secret_num")
                                               (mt/format-name "shops") (mt/format-name "tier"))}}
        mappings    (fn [card-id field-ref-fn]
                      (for [param ["secret" "tier"]]
                        {:parameter_id param :card_id card-id :target ["dimension" (field-ref-fn (keyword param))]}))]
    (mt/with-temp [:model/Card {summary-card :id} {:dataset_query summary-q :display :bar :result_metadata (admin-metadata summary-q)}
                   :model/Card {native-card :id} {:dataset_query native-q :display :bar :result_metadata (admin-metadata native-q)}
                   :model/Card {metric-card :id} {:dataset_query metric-q :type :metric :result_metadata (admin-metadata metric-q)}
                   :model/Dashboard {dash-id :id} {:parameters [{:id "secret" :slug "secret" :name "secret" :type "string/="}
                                                                {:id "tier" :slug "tier" :name "tier" :type "number/="}
                                                                {:id "shop" :slug "shop" :name "shop" :type "id"}]}
                   :model/DashboardCard {orders-dc :id} {:dashboard_id       dash-id
                                                         :card_id            orders-card
                                                         :parameter_mappings (cons {:parameter_id "shop" :card_id orders-card
                                                                                    :target       ["dimension" [:field (dataset/fid ids :orders :shop_id) nil]]}
                                                                                   (mappings orders-card #(hand-written/implicit ids :orders :shop_id :shops %)))}
                   :model/DashboardCard {shops-dc :id} {:dashboard_id       dash-id
                                                        :card_id            shops-card
                                                        :parameter_mappings (mappings shops-card field)}
                   :model/DashboardCardSeries _ {:dashboardcard_id shops-dc :card_id summary-card :position 0}
                   :model/DashboardCardSeries _ {:dashboardcard_id shops-dc :card_id native-card :position 1}]
      (do-with-segment!
       ids
       (fn [segment]
         (do-with-fingerprinted-pk!
          ids
          #(f {:id        dash-id
               :dashcards {:card/orders-implicit [orders-dc orders-card] :card/shops [shops-dc shops-card]}
               :cards     {:summary summary-card :native native-card :metric metric-card}
               :segment   segment
               :queries   [summary-q native-q metric-q]})))))))

(defn request
  "`[method path body?]` for entry point `k`, against the base copy `ids` and the fixture of [[do-with-dashboard!]].
  Method `:mcp` calls the MCP tool named by `path` with `body` as its arguments. Tier 2 is hidden by the row
  sandboxes."
  [k ids {dash :id :keys [dashcards cards segment]}]
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
      :card-series              [:get (format "card/%d/series" (:summary cards))]
      :table-related            [:get (format "table/%d/related" shops)]
      :segment-related          [:get (format "segment/%d/related" segment)]
      :virtual-db-metadata      [:get "database/-1337/metadata"]
      :virtual-db-schema        [:get "database/-1337/schema/Everything else"]
      :mcp-get-content          [:mcp "get_content" {:items   [{:type "question" :id card}
                                                               {:type "question" :id (:summary cards)}
                                                               {:type "question" :id (:native cards)}
                                                               {:type "metric" :id (:metric cards)}
                                                               {:type "dashboard" :id dash}]
                                                     :include ["fields"]}]
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
    :dashboard-param-search :dashboard-chain-filter :dashboard-chain-filter-tier :xray-table :xray-field-secret-num
    :card-series :table-related :segment-related :virtual-db-metadata :virtual-db-schema :mcp-get-content})

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
   :mcp-other-tools      "query tools reach the processor through the card and ad-hoc paths tested here; :mcp-get-content stands for the metadata reads"
   :metabot              "no HTTP contract to replay; it reaches the processor through the card and ad-hoc paths tested here"
   :api-keys             "authentication, not data access; the same handlers run"
   :public-and-embed     "run as the card's creator by design, so no sandbox applies; documented, with its own suite"
   :subscriptions-alerts "rendered as the recipient out of band; metabase-enterprise.sandbox.pulse-test owns them"
   :actions              "the write path; metabase-enterprise.sandbox.actions-test owns it"
   :search               "returns metadata, not rows, from an index built as an admin: a separate design question"
   :documents            "no endpoint that reads table data"
   :database-routing     "routes a whole database; orthogonal to row and column rules, with its own suite"})

;;; ---------------------------------------------------- Scans -----------------------------------------------------

(defn- mcp-call
  "Call MCP tool `tool` with `arguments` as `user`, over a bearer token holding every scope: `{:status :body}`, the
  body being the data the tool returned, or the JSON-RPC error."
  [user tool arguments]
  (mcp.tu/do-with-bearer-token!
   user mcp.tu/all-scopes
   (fn [auth]
     (let [post!      (fn [body headers]
                        (client/client-full-response :post "metabase-mcp" {:request-options {:headers (merge auth headers)}} body))
           session-id (get-in (post! {:jsonrpc "2.0" :method "initialize" :params {:capabilities {}} :id 1} {})
                              [:headers "Mcp-Session-Id"])
           _          (post! {:jsonrpc "2.0" :method "notifications/initialized" :params {}} {"mcp-session-id" session-id})
           {:keys [status body]} (post! {:jsonrpc "2.0" :method "tools/call" :params {:name tool :arguments arguments} :id 2}
                                        {"mcp-session-id" session-id})
           text       (-> body :result :content first :text)]
       {:status status
        :body   (if-let [[_ data] (some-> text mcp.tu/data-parts)] (json/decode+kw data) body)}))))

(defn request-as
  "`[method path body?]` as `user`: `{:status :body}`."
  [user [method path body]]
  (if (= :mcp method)
    (mcp-call user path body)
    (let [{:keys [status] response :body} (if body
                                            (mt/user-http-request-full-response user method path body)
                                            (mt/user-http-request-full-response user method path))]
      {:status status :body response})))

(defn- secret-fingerprints
  "The fingerprints an admin's run of `queries` gives that `user`'s own run does not: what the whole table says and
  the user's sandbox does not."
  [user queries]
  (let [fingerprints (fn [u q]
                       (let [{:keys [body]} (mt/user-http-request-full-response u :post "dataset" q)]
                         (into #{} (keep (comp invariants/normalize-fingerprint :fingerprint))
                               (get-in body [:data :results_metadata :columns]))))]
    (into #{} (mapcat (fn [q] (remove (fingerprints user q) (fingerprints :crowberto q)))) queries)))

(defn canary-outcome
  "`:leak` when the response shows a canary outside the sandbox, `:fingerprint-leak` when only a fingerprint's
  numbers do, else `:clean`."
  [allowed body]
  (let [leaked #(seq (remove allowed (dataset/canary-ids %)))]
    (cond
      (leaked (walk/postwalk #(if (map? %) (dissoc % :fingerprint) %) body)) :leak
      (leaked body)                                                          :fingerprint-leak
      :else                                                                  :clean)))

(defn scan!
  "Call every scanned entry point (`:request` ones) as `(:user ctx)` (default `:rasta`) against the fixture of
  [[do-with-dashboard!]]. Returns entry point -> `{:status :leaked :canary :hygiene}`, where `:leaked` is the canary
  ids outside that user's sandbox and `:canary` the [[canary-outcome]]. Hygiene compares fingerprints with the
  fixture queries' [[secret-fingerprints]]."
  [{:keys [ids allowed user] :or {user :rasta} :as ctx} fixture]
  (let [ctx (assoc ctx :secret-fingerprints (secret-fingerprints user (:queries fixture)))]
    (into (sorted-map)
          (for [[k {scanned? :request}] entry-points
                :when scanned?]
            (let [{:keys [status body]} (request-as user (request k ids fixture))]
              [k {:status  status
                  :leaked  (not-empty (into (sorted-set) (remove allowed) (dataset/canary-ids body)))
                  :canary  (canary-outcome allowed body)
                  :hygiene (invariants/hygiene ctx {:body body})}])))))

(defn scan-entry-points!
  "[[scan!]] as the sandboxed user under `policy-k`."
  [policy-k]
  (harness/do-with-policy!
   policy-k
   (fn [{:keys [ids] :as ctx}]
     (do-with-dashboard! ids #(scan! ctx %)))))

(def scanned-policies
  "The policies the scanned entry points run under." [:table-tier :card-hide-price :table-tier+value-remap])

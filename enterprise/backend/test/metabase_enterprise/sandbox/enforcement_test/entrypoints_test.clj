(ns ^:mb/driver-tests metabase-enterprise.sandbox.enforcement-test.entrypoints-test
  "Sandbox enforcement at entry points other than `POST /api/dataset`: everything the sandboxed user can reach that reads
  SHOPS data or metadata, scanned for canaries of hidden rows and for hygiene; dashboard filter values on a model column
  mapped to a sandboxed column; segment creation."
  (:require
   [clojure.test :refer :all]
   [metabase-enterprise.sandbox.enforcement-test.dataset :as dataset]
   [metabase-enterprise.sandbox.enforcement-test.harness :as harness]
   [metabase-enterprise.sandbox.enforcement-test.invariants :as invariants]
   [metabase-enterprise.sandbox.enforcement-test.queries.hand-written :as hand-written]
   [metabase.test :as mt]
   [metabase.util.json :as json]))

(set! *warn-on-reflection* true)

;;; ------------------------------------------------ Endpoint scan -------------------------------------------------
;;;
;;; Everything the sandboxed user can reach that reads SHOPS data or metadata, outside `POST /api/dataset`: scanned
;;; for canaries of hidden rows and for hygiene. The base database gets one full sync first, so fingerprints and
;;; field values exist as they would in a real instance.

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

(defn- dashboard-requests
  "Endpoint name -> request for the dashboard of [[do-with-dashboard!]]. Tier 2 is hidden by the row sandboxes."
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
                                                 (dataset/canary-text :shops 2))]
     :dashcard-orders-hidden-tier  (run-dashcard :card/orders-implicit hidden-tier)
     :dashcard-shops-hidden-tier   (run-dashcard :card/shops hidden-tier)
     :xray-table                   [:get (format "automagic-dashboards/table/%d" (dataset/tid ids :shops))]
     :xray-field-secret-num        [:get (format "automagic-dashboards/field/%d" (dataset/fid ids :shops :secret_num))])))

(defn- endpoint-requests
  "Endpoint name -> `[method path body?]` for the current (base copy) database."
  [ids]
  (let [shops      (dataset/tid ids :shops)
        secret     (dataset/fid ids :shops :secret)
        secret-num (dataset/fid ids :shops :secret_num)
        price      (dataset/fid ids :shops :price)
        model      (get-in ids [:cards :model/shops])
        card       (get-in ids [:cards :card/shops])
        canonical  (hand-written/implicit ids :orders :shop_id :shops :secret)
        orders-q   {:database (:db ids) :type "query"
                    :query    {:source-table (dataset/tid ids :orders) :fields [[:field (dataset/fid ids :orders :id) nil] canonical]}}]
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
                                 :query    {:source-table (dataset/tid ids :orders) :breakout [canonical] :aggregation [["count"]]}}]
     :pivot-secret-num         [:post "dataset/pivot"
                                {:database (:db ids) :type "query"
                                 :query    {:source-table (dataset/tid ids :orders)
                                            :breakout     [(hand-written/implicit ids :orders :shop_id :shops :secret_num)]
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
  "Call every endpoint of [[endpoint-requests]] as the sandboxed user under `policy-k`, after a full sync of the base
  copy. Returns endpoint -> `{:status, :leaked-venue-ids, :hygiene}`."
  [policy-k]
  (harness/do-with-policy!
   policy-k
   (fn [{:keys [ids allowed] :as ctx}]
     (do-with-dashboard!
      ids
      (fn [dashboard]
        (into (array-map)
              (for [[endpoint request] (concat (endpoint-requests ids) (dashboard-requests ids dashboard))]
                (let [{:keys [status body]} (request-as :rasta request)
                      user                  {:body body}]
                  [endpoint {:status  status
                             :leaked  (not-empty (into (sorted-set) (remove allowed) (dataset/canary-ids body)))
                             :hygiene (invariants/hygiene ctx user)}]))))))))

(deftest ^:synchronized endpoints-reveal-nothing-outside-the-sandbox-test
  (mt/test-drivers #{:h2}
    (doseq [policy-k [:table-tier :card-hide-price :table-tier+value-remap]
            [endpoint {:keys [status leaked hygiene]}] (run-canary-endpoints! policy-k)]
      (testing (format "%s under %s (HTTP %s)" endpoint policy-k status)
        (testing "no canary of a hidden row"
          (is (nil? leaked)))
        (testing "nothing internal in the response"
          (is (= :clean (:verdict hygiene)) (pr-str hygiene)))))))

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

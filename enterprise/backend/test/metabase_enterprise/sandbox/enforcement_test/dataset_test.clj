(ns ^:mb/driver-tests metabase-enterprise.sandbox.enforcement-test.dataset-test
  "The dataset and each restricted variant load as designed: FK metadata, row counts, canary sets, both FKs from ORDERS."
  (:require
   [clojure.test :refer :all]
   [metabase-enterprise.sandbox.enforcement-test.dataset :as dataset]
   [metabase-enterprise.sandbox.enforcement-test.harness :as harness]
   [metabase-enterprise.sandbox.enforcement-test.policies.generated :as generated]
   [metabase.test :as mt]
   [toucan2.core :as t2]))

(set! *warn-on-reflection* true)

(defn- run-as-admin [inner-query]
  (:body (harness/execute-as :crowberto {:database (mt/id) :type "query" :query inner-query})))

(defn- canary-ids-of [table body]
  (into (sorted-set) (keep (fn [[t id]] (when (= t table) id))) (dataset/canary-ids body)))

(deftest ^:synchronized corpus-dataset-loads-as-designed-test
  (mt/test-drivers (harness/drivers)
    (doseq [[dataset-var {:keys [shops regions shop-rows]}] dataset/variant-contents]
      (testing (:name (meta dataset-var))
        (mt/dataset (var-get dataset-var)
          (dataset/ensure-fk-metadata!)
          (testing "every FK is marked in the metadata"
            (doseq [[from-table from-col to-table to-col] dataset/fks]
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

(deftest ^:parallel secrets-test
  (testing "What the base dataset holds that a restricted variant does not"
    (let [values (fn [d] (into #{} (map (juxt :table :column :value :why)) (:values (dataset/secrets d))))]
      (testing "text values of the rows a variant drops, but not values a kept row shares"
        (is (contains? (values #'dataset/shops-tier1) [:shops :name "Beta" :hidden-row]))
        (is (not-any? #(= "Alpha" (nth % 2)) (values #'dataset/shops-tier1))))
      (testing "text values of a hidden column, in every row"
        (is (= #{[:regions :code "N-1" :hidden-column] [:regions :code "S-2" :hidden-column]
                 [:regions :code "E-1" :hidden-column] [:regions :code "W-2" :hidden-column]}
               (values (generated/restricted {:regions {:kind :card :filters [] :hidden #{:code}}})))))
      (testing "the canary columns are not values: they are matched as canaries"
        (is (not-any? #(#{:secret :secret_num} (second %)) (values #'dataset/shops-tier1))))
      (testing "nothing, when the variant is the base"
        (is (empty? (values #'dataset/shops)))))
    (testing "the canaries a variant shows, by table, id and kind"
      (let [visible (:visible-canaries (dataset/secrets #'dataset/shops-tier1))]
        (is (contains? visible [:shops 1 :text]))
        (is (contains? visible [:shops 1 :number]))
        (is (not (contains? visible [:shops 2 :text])))
        (is (contains? visible [:regions 2 :text])))
      (is (not-any? #(= :shops (first %)) (:visible-canaries (dataset/secrets #'dataset/shops-tier1-hide-price)))))))

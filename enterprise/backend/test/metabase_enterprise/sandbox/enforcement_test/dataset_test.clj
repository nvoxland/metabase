(ns ^:mb/driver-tests metabase-enterprise.sandbox.enforcement-test.dataset-test
  "The dataset and each restricted variant load as designed: FK metadata, row counts, canary sets, both FKs from ORDERS."
  (:require
   [clojure.test :refer :all]
   [metabase-enterprise.sandbox.enforcement-test.dataset :as dataset]
   [metabase-enterprise.sandbox.enforcement-test.harness :as harness]
   [metabase.test :as mt]
   [toucan2.core :as t2]))

(set! *warn-on-reflection* true)

(defn- run-as-admin [inner-query]
  (mt/user-http-request :crowberto :post 202 "dataset" {:database (mt/id) :type "query" :query inner-query}))

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

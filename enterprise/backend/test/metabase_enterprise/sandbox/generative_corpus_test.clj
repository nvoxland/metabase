(ns ^:mb/driver-tests metabase-enterprise.sandbox.generative-corpus-test
  "Systematic sandboxing tests over [[metabase-enterprise.sandbox.corpus-test-dataset]]: every role a sandboxed table
  can play, under every kind of sandbox, judged by detectors that need no expected answer per case. See the namespace
  docstring of the dataset for the schema and the canaries."
  (:require
   [clojure.test :refer :all]
   [metabase-enterprise.sandbox.corpus-test-dataset :as corpus]
   [metabase.test :as mt]
   [toucan2.core :as t2]))

;;; --------------------------------------------- Dataset smoke test -----------------------------------------------

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

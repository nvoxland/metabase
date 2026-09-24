(ns metabase.engine.execute-test
  "MBQL questions routed through the native query engine against a real Postgres, compared with the
  JDBC path. Needs the engine library built and the Postgres test dataset (`DRIVERS=postgres`)."
  (:require
   [clojure.test :refer :all]
   [metabase.engine.core :as engine]
   [metabase.query-processor :as qp]
   [metabase.test :as mt]))

(defn- rows-via!
  "Run `query` with the given `query-engine` mode and return its rows."
  [mode query]
  (mt/with-temporary-setting-values [query-engine mode]
    (mt/rows (qp/process-query query))))

(defn- cols-via! [mode query]
  (mt/with-temporary-setting-values [query-engine mode]
    (map :name (mt/cols (qp/process-query query)))))

(defn- same-value?
  "Equality that ignores the numeric representation: the engine types `AVG` as a double where
  Postgres returns a numeric, and both are the same answer."
  [a b]
  (if (and (number? a) (number? b))
    (== (double a) (double b))
    (= a b)))

(defn- same-rows? [expected actual]
  (and (= (count expected) (count actual))
       (every? true? (map (fn [row-a row-b]
                            (and (= (count row-a) (count row-b))
                                 (every? true? (map same-value? row-a row-b))))
                          expected actual))))

(defmacro ^:private with-engine [& body]
  `(mt/with-temporary-setting-values [~'engine-transport :ffi]
     (try
       ~@body
       (finally
         (engine/stop!)))))

(deftest ^:mb/engine-tests ^:mb/driver-tests routed-queries-match-jdbc-test
  (mt/test-drivers #{:postgres}
    (with-engine
      (doseq [[description query]
              [["a plain select with order and limit"
                (mt/mbql-query venues {:order-by [[:asc $id]] :limit 5})]
               ["a filter with a string parameter"
                (mt/mbql-query venues {:filter [:starts-with $name "B"] :order-by [[:asc $id]]})]
               ["aggregations with a breakout"
                (mt/mbql-query venues {:aggregation [[:count] [:sum $price] [:avg $price]]
                                       :breakout    [$category_id]
                                       :order-by    [[:asc $category_id]]})]
               ["a temporal breakout"
                (mt/mbql-query checkins {:aggregation [[:count]] :breakout [!month.date] :order-by [[:asc !month.date]]})]
               ["a join"
                (mt/mbql-query venues {:joins    [{:source-table $$categories
                                                   :alias        "c"
                                                   :condition    [:= $category_id &c.categories.id]
                                                   :fields       [&c.categories.name]}]
                                       :fields   [$id $name]
                                       :order-by [[:asc $id]]
                                       :limit    5})]
               ["an expression"
                (mt/mbql-query venues {:expressions {"double" [:* $price 2]}
                                       :fields      [$id [:expression "double"]]
                                       :order-by    [[:asc $id]]
                                       :limit       5})]
               ["a timestamp column"
                (mt/mbql-query users {:fields [$id $last_login] :order-by [[:asc $id]] :limit 3})]]]
        (testing description
          (is (= (cols-via! :jdbc query) (cols-via! :datafusion query)))
          (let [expected (rows-via! :jdbc query)
                actual   (rows-via! :datafusion query)]
            (is (same-rows? expected actual)
                (str "expected " (pr-str (take 3 expected)) " got " (pr-str (take 3 actual))))))))))

(deftest ^:mb/engine-tests ^:mb/driver-tests routing-binds-the-datafusion-driver-test
  (mt/test-drivers #{:postgres}
    (with-engine
      (mt/with-temporary-setting-values [query-engine :datafusion]
        (is (= 1 (count (mt/rows (qp/process-query (mt/mbql-query venues {:limit 1}))))))
        (is (some #(= (mt/id) (:database_id %)) (:pools (engine/pool-stats)))
            "the engine opened a pool for the routed database"))
      (mt/with-temporary-setting-values [query-engine :jdbc]
        (is (= 1 (count (mt/rows (qp/process-query (mt/mbql-query venues {:limit 1}))))))))))

(deftest ^:mb/engine-tests ^:mb/driver-tests native-queries-stay-on-jdbc-test
  (mt/test-drivers #{:postgres}
    (with-engine
      (mt/with-temporary-setting-values [query-engine :datafusion]
        (is (= [[1]] (mt/rows (qp/process-query (mt/native-query {:query "SELECT 1"})))))))))

(deftest ^:mb/engine-tests ^:mb/driver-tests unsupported-constructs-are-rejected-test
  (mt/test-drivers #{:postgres}
    (with-engine
      (mt/with-temporary-setting-values [query-engine :datafusion]
        ;; run one routed query so the engine knows the database, then ask it to plan something Postgres can't run
        (mt/rows (qp/process-query (mt/mbql-query venues {:limit 1})))
        (engine/with-session [session {:database_id (mt/id)}]
          (let [e (is (thrown? clojure.lang.ExceptionInfo
                               (engine/plan-and-unparse session "SELECT arrow_cast(1, 'Utf8')")))]
            (is (= :unsupported-feature (:type (ex-data e))))
            (is (= "function arrow_cast" (:construct (ex-data e))))))))))

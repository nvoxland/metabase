(ns metabase.driver.datafusion-test
  "The `:datafusion` driver compiles MBQL to DataFusion SQL; the engine must then plan that SQL and
  unparse it for the warehouse. These tests do both against the H2 test dataset's metadata, so they
  need the engine library built (`cd engine && cargo build`)."
  (:require
   [clojure.string :as str]
   [clojure.test :refer :all]
   [metabase.driver :as driver]
   [metabase.driver.datafusion]
   [metabase.engine.catalog :as engine.catalog]
   [metabase.engine.core :as engine]
   [metabase.engine.test-util :as engine.tu]
   [metabase.query-processor.compile :as qp.compile]
   [metabase.test :as mt]))

(comment metabase.driver.datafusion/keep-me)

(defn- compile-datafusion
  "Compile an MBQL query with the `:datafusion` driver bound, returning `{:query sql :params [...]}`."
  [query]
  (binding [driver/*driver* :datafusion]
    (select-keys (qp.compile/compile query) [:query :params])))

(deftest numbered-placeholders-test
  (testing "JDBC `?` placeholders become `$n`, except inside string literals and quoted identifiers"
    (is (= "SELECT \"pk?\" FROM t WHERE a = $1 AND b = 'x?' AND c IN ($2, $3) AND d = 'it''s?'"
           (#'metabase.driver.datafusion/numbered-placeholders
            "SELECT \"pk?\" FROM t WHERE a = ? AND b = 'x?' AND c IN (?, ?) AND d = 'it''s?'")))))

(deftest compile-test
  (mt/test-drivers #{:h2}
    (testing "placeholders are numbered and identifiers are quoted exactly"
      ;; numbers in filters are inlined by the shared compiler; strings become parameters
      (let [{:keys [query params]} (compile-datafusion (mt/mbql-query venues
                                                         {:filter   [:and [:> $price 2] [:= $name "Bob"] [:!= $name "Al"]]
                                                          :order-by [[:asc $id]]
                                                          :limit    5}))]
        (is (str/includes? query "\"PUBLIC\".\"VENUES\""))
        (is (str/includes? query "> 2"))
        (is (str/includes? query "= $1"))
        (is (str/includes? query "$2"))
        (is (not (str/includes? query "?")))
        (is (= ["Bob" "Al"] params))))
    (testing "temporal bucketing uses date_trunc"
      (let [{:keys [query]} (compile-datafusion (mt/mbql-query checkins
                                                  {:aggregation [[:count]]
                                                   :breakout    [!month.date]}))]
        (is (str/includes? query "DATE_TRUNC('month'"))))
    (testing "datetime-diff avoids age() and does not parameterize its constants"
      (let [{:keys [query params]} (compile-datafusion (mt/mbql-query checkins
                                                         {:expressions {"months" [:datetime-diff $date [:now] :month]}
                                                          :fields      [[:expression "months"]]
                                                          :limit       1}))]
        (is (not (str/includes? query "age(")))
        (is (str/includes? query "extract("))
        (is (empty? params))))))

(def ^:private corpus
  "MBQL shapes the query builder produces. Each must compile and plan for Postgres."
  [(fn [] (mt/mbql-query venues {:limit 10}))
   (fn [] (mt/mbql-query venues {:fields [$id $name] :order-by [[:desc $price]] :limit 3}))
   (fn [] (mt/mbql-query venues {:aggregation [[:count]]}))
   (fn [] (mt/mbql-query venues {:aggregation [[:count] [:sum $price] [:avg $price] [:min $price] [:max $price]]
                                 :breakout    [$category_id]}))
   (fn [] (mt/mbql-query venues {:aggregation [[:distinct $category_id]]}))
   (fn [] (mt/mbql-query venues {:aggregation [[:stddev $price]]}))
   (fn [] (mt/mbql-query venues {:filter [:and [:>= $price 2] [:<= $price 4] [:not-null $name]]}))
   (fn [] (mt/mbql-query venues {:filter [:or [:starts-with $name "B"] [:contains $name "ar"] [:ends-with $name "s"]]}))
   (fn [] (mt/mbql-query venues {:filter [:between $price 1 3]}))
   (fn [] (mt/mbql-query venues {:filter [:= $category_id 5 10 15]}))
   (fn [] (mt/mbql-query venues {:expressions {"double" [:* $price 2]
                                               "label"  [:concat $name " (" [:coalesce $name "?"] ")"]}
                                 :fields      [$id [:expression "double"] [:expression "label"]]
                                 :limit       5}))
   (fn [] (mt/mbql-query venues {:expressions {"cased" [:case [[[:> $price 3] "pricey"]] {:default "cheap"}]}
                                 :fields      [$id [:expression "cased"]]}))
   (fn [] (mt/mbql-query venues {:joins    [{:source-table $$categories
                                             :alias        "c"
                                             :condition    [:= $category_id &c.categories.id]
                                             :fields       :all}]
                                 :order-by [[:asc $id]]
                                 :limit    5}))
   (fn [] (mt/mbql-query venues {:aggregation [[:count]]
                                 :breakout    [&c.categories.name]
                                 :joins       [{:source-table $$categories
                                                :alias        "c"
                                                :condition    [:= $category_id &c.categories.id]
                                                :strategy     :left-join}]}))
   (fn [] (mt/mbql-query checkins {:aggregation [[:count]] :breakout [!month.date]}))
   (fn [] (mt/mbql-query checkins {:aggregation [[:count]] :breakout [!week.date]}))
   (fn [] (mt/mbql-query checkins {:aggregation [[:count]] :breakout [!day-of-week.date]}))
   (fn [] (mt/mbql-query checkins {:aggregation [[:count]] :breakout [!quarter-of-year.date]}))
   (fn [] (mt/mbql-query checkins {:filter [:time-interval $date -30 :day]}))
   (fn [] (mt/mbql-query checkins {:filter [:between $date "2014-01-01" "2015-01-01"]}))
   (fn [] (mt/mbql-query checkins {:expressions {"days" [:datetime-diff $date [:now] :day]
                                                 "months" [:datetime-diff $date [:now] :month]
                                                 "years" [:datetime-diff $date [:now] :year]}
                                   :fields      [[:expression "days"] [:expression "months"] [:expression "years"]]
                                   :limit       5}))
   (fn [] (mt/mbql-query checkins {:expressions {"next" [:datetime-add $date 1 :month]}
                                   :fields      [[:expression "next"]]
                                   :limit       5}))
   (fn [] (mt/mbql-query checkins {:expressions {"y" [:get-year $date] "m" [:get-month $date] "d" [:get-day $date]}
                                   :fields      [[:expression "y"] [:expression "m"] [:expression "d"]]
                                   :limit       5}))
   (fn [] (mt/mbql-query venues {:source-query {:source-table $$venues
                                                :aggregation  [[:count]]
                                                :breakout     [$category_id]}
                                 :filter       [:> *count/Integer 5]}))
   (fn [] (mt/mbql-query venues {:aggregation [[:cum-count]] :breakout [$category_id]}))
   (fn [] (mt/mbql-query venues {:aggregation [[:cum-sum $price]] :breakout [$category_id]}))
   (fn [] (mt/mbql-query venues {:aggregation [[:count]]
                                 :breakout    [[:field %price {:binning {:strategy :num-bins :num-bins 4}}]]}))
   (fn [] (mt/mbql-query venues {:aggregation [[:count-where [:> $price 2]] [:sum-where $price [:< $price 3]]
                                               [:share [:= $price 1]]]}))
   (fn [] (mt/mbql-query venues {:expressions {"t" [:text $price] "l" [:length $name] "u" [:upper $name]
                                               "s" [:substring $name 1 3] "r" [:replace $name "a" "b"]}
                                 :fields      [[:expression "t"] [:expression "l"] [:expression "u"]
                                               [:expression "s"] [:expression "r"]]
                                 :limit       2}))
   (fn [] (mt/mbql-query venues {:expressions {"m" [:regex-match-first $name "^(\\w+)"]}
                                 :fields      [[:expression "m"]]
                                 :limit       2}))
   (fn [] (mt/mbql-query venues {:expressions {"p" [:split-part $name " " 1]}
                                 :fields      [[:expression "p"]]
                                 :limit       2}))
   (fn [] (mt/mbql-query venues {:aggregation [[:count]] :breakout [$category_id] :order-by [[:desc [:aggregation 0]]] :limit 3}))
   (fn [] (mt/mbql-query users {:filter [:= $last_login "2014-01-01T08:30:00"]}))
   (fn [] (mt/mbql-query venues {:expressions {"f" [:floor $price] "c" [:ceil $price] "r" [:round $price]
                                               "a" [:abs $price] "q" [:sqrt $price] "e" [:exp $price] "lg" [:log $price]
                                               "pw" [:power $price 2]}
                                 :fields      [[:expression "f"] [:expression "c"] [:expression "r"] [:expression "a"]
                                               [:expression "q"] [:expression "e"] [:expression "lg"] [:expression "pw"]]
                                 :limit       2}))
   (fn [] (mt/mbql-query venues {:aggregation [[:median $price] [:percentile $price 0.9]]
                                 :breakout    [$category_id]
                                 :limit       5}))])

(deftest ^:mb/engine-tests corpus-plans-for-postgres-test
  (mt/test-drivers #{:h2}
    (engine.tu/with-engine
      (do
        ;; Plan the H2 dataset's metadata as if it were Postgres: this checks the compiler and the unparser, not H2.
        (engine/register-catalog! (assoc (engine.catalog/snapshot (mt/db)) :connector "postgres"))
        (engine/with-session [session {:database_id (mt/id)}]
          (doseq [make-query corpus
                  :let [query               (make-query)
                        {:keys [query params]} (compile-datafusion query)]]
            (testing (str "\n" query)
              (let [result (try
                             (engine/plan-and-unparse session query [])
                             (catch Exception e
                               {:error (ex-message e) :data (ex-data e)}))]
                (is (nil? (:error result)) (pr-str (:data result)))
                (when-not (:error result)
                  (is (= "postgres" (:warehouse result)))
                  (is (seq (:schema result)))
                  (testing "every parameter has a placeholder in the warehouse SQL"
                    (doseq [n (range 1 (inc (count params)))]
                      (is (str/includes? (:warehouse_sql result) (str "$" n))))))))))))))

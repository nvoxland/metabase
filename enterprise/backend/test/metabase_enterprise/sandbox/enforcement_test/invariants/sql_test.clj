(ns metabase-enterprise.sandbox.enforcement-test.invariants.sql-test
  "Pure tests of the compiled-SQL placement rule on literal SQL in the quoting styles of H2, Postgres and MariaDB."
  (:require
   [clojure.test :refer :all]
   [metabase-enterprise.sandbox.enforcement-test.invariants.sql :as sql]))

(def ^:private h2-sandboxed
  "The shape H2 compiles for an implicit join to a table-sandboxed SHOPS."
  (str "-- Metabase:: userID: 2 queryType: MBQL queryHash: abc\n"
       "SELECT \"PUBLIC\".\"ORDERS\".\"ID\" AS \"ID\", \"SHOPS__via__SHOP_ID\".\"PRICE\" AS \"SHOPS__via__SHOP_ID__PRICE\" "
       "FROM \"PUBLIC\".\"ORDERS\" LEFT JOIN (SELECT \"PUBLIC\".\"SHOPS\".\"ID\" AS \"ID\", \"PUBLIC\".\"SHOPS\".\"PRICE\" AS \"PRICE\" "
       "FROM \"PUBLIC\".\"SHOPS\" WHERE \"PUBLIC\".\"SHOPS\".\"TIER\" = 1) AS \"SHOPS__via__SHOP_ID\" "
       "ON \"PUBLIC\".\"ORDERS\".\"SHOP_ID\" = \"SHOPS__via__SHOP_ID\".\"ID\" LIMIT 1048575"))

(deftest ^:parallel placement-test
  (testing "every read inside the sandbox subquery, one subquery"
    (is (= {:inside 1 :raw []} (sql/placement h2-sandboxed "SHOPS"))))
  (testing "the raw table read in the outer query"
    (let [{:keys [raw]} (sql/placement "SELECT * FROM \"PUBLIC\".\"SHOPS\" WHERE \"PUBLIC\".\"SHOPS\".\"TIER\" = 1" "SHOPS")]
      (is (= 2 (count raw)))))
  (testing "an IN (SELECT ...) subquery is not a derived table: the read is raw"
    (is (= 1 (count (:raw (sql/placement "SELECT * FROM \"ORDERS\" WHERE \"SHOP_ID\" IN (SELECT \"ID\" FROM \"SHOPS\")" "SHOPS"))))))
  (testing "Postgres and MariaDB quoting, lower case"
    (is (= {:inside 1 :raw []}
           (sql/placement "SELECT o.id FROM \"public\".\"orders\" o LEFT JOIN (SELECT id FROM \"public\".\"shops\" WHERE tier = 1) AS \"shops__via__shop_id\" ON 1=1" "shops")))
    (is (= {:inside 1 :raw []}
           (sql/placement "SELECT o.id FROM `orders` o LEFT JOIN (SELECT id FROM `shops` WHERE tier = 1) AS `shops__via__shop_id` ON 1=1" "shops"))))
  (testing "two sandbox subqueries for two FKs"
    (is (= 2 (:inside (sql/placement "SELECT 1 FROM \"ORDERS\" LEFT JOIN (SELECT \"ID\" FROM \"SHOPS\" WHERE 1=1) AS \"A\" ON 1=1 LEFT JOIN (SELECT \"ID\" FROM \"SHOPS\" WHERE 1=1) AS \"B\" ON 1=1" "SHOPS")))))
  (testing "an alias equal to the table name, a string literal and a comment are not reads"
    (is (= {:inside 1 :raw []}
           (sql/placement "-- shops\nSELECT 'shops' FROM (SELECT 1 FROM \"SHOPS\") AS \"SHOPS\"" "SHOPS"))))
  (testing "the implicit-join alias does not match the table name"
    (is (= {:inside 0 :raw []} (sql/placement "SELECT \"SHOPS__via__SHOP_ID\".\"PRICE\" FROM \"ORDERS\"" "SHOPS"))))
  (testing "a quoted alias that contains the name as a word is another identifier"
    (is (= {:inside 1 :raw []}
           (sql/placement "SELECT \"Shops - Parent Shop\".\"ID\" FROM (SELECT 1 FROM \"SHOPS\") AS \"Shops - Parent Shop\"" "SHOPS"))))
  (testing "a join alias equal to the table name, used to qualify columns, is not a table read"
    (is (= {:inside 1 :raw []}
           (sql/placement "SELECT \"Shops\".\"ID\" FROM \"ORDERS\" LEFT JOIN (SELECT \"ID\" FROM \"PUBLIC\".\"SHOPS\" WHERE 1=1) AS \"Shops\" ON \"ORDERS\".\"SHOP_ID\" = \"Shops\".\"ID\"" "SHOPS"))))
  (testing "a CTE body and a derived table that opens with a CTE are subqueries"
    (is (= {:inside 1 :raw []}
           (sql/placement "SELECT * FROM (WITH \"__mb_source\" (\"ID\") AS (SELECT \"ID\" FROM \"SHOPS\" WHERE 1=1) SELECT \"ID\" FROM \"__mb_source\") AS \"__mb_source\"" "SHOPS")))))

(deftest ^:parallel hidden-column-reads-test
  (let [hidden #{"PRICE" "SECRET" "SECRET_NUM"}]
    (testing "a hidden column read through a sandbox subquery that does not return it"
      (is (= [{:alias "__mb_source" :column "PRICE"}]
             (sql/hidden-column-reads "SELECT \"__mb_source\".\"ID\" AS \"ID\", \"__mb_source\".\"PRICE\" AS \"PRICE\" FROM (SELECT \"PUBLIC\".\"SHOPS\".\"ID\" AS \"ID\" FROM \"PUBLIC\".\"SHOPS\") AS \"__mb_source\"" hidden))))
    (testing "through an implicit-join alias, and with a join-alias prefix"
      (is (= [{:alias "SHOPS__via__SHOP_ID" :column "PRICE"}]
             (sql/hidden-column-reads "SELECT \"SHOPS__via__SHOP_ID\".\"PRICE\" AS \"P\" FROM \"PUBLIC\".\"ORDERS\" LEFT JOIN (SELECT \"PUBLIC\".\"SHOPS\".\"ID\" AS \"ID\" FROM \"PUBLIC\".\"SHOPS\") AS \"SHOPS__via__SHOP_ID\" ON 1=1" hidden)))
      (is (= [{:alias "__mb_source" :column "S__SECRET"}]
             (sql/hidden-column-reads "SELECT \"__mb_source\".\"S__SECRET\" FROM (SELECT \"ID\" AS \"ID\" FROM \"SHOPS\") AS \"__mb_source\"" hidden))))
    (testing "a hidden name the alias does return is fine: REGIONS has SECRET too"
      (is (= [] (sql/hidden-column-reads "SELECT \"R\".\"SECRET\" AS \"SECRET\" FROM (SELECT \"PUBLIC\".\"REGIONS\".\"SECRET\" AS \"SECRET\" FROM \"PUBLIC\".\"REGIONS\") AS \"R\"" hidden))))
    (testing "a CTE's column list is what it returns"
      (is (= [] (sql/hidden-column-reads "WITH \"__mb_source\" (\"ID\", \"PRICE\") AS (SELECT 1, 2) SELECT \"__mb_source\".\"PRICE\" FROM \"__mb_source\"" hidden)))
      (is (= [{:alias "__mb_source" :column "PRICE"}]
             (sql/hidden-column-reads "WITH \"__mb_source\" (\"ID\") AS (SELECT 1) SELECT \"__mb_source\".\"PRICE\" FROM \"__mb_source\"" hidden))))
    (testing "MariaDB quoting"
      (is (= [{:alias "__mb_source" :column "price"}]
             (sql/hidden-column-reads "SELECT `__mb_source`.`price` FROM (SELECT `shops`.`id` AS `id` FROM `shops`) AS `__mb_source`" #{"price"}))))
    (testing "a column the subquery's nested group returns is not returned by the outer subquery"
      (is (= [{:alias "__mb_source" :column "PRICE"}]
             (sql/hidden-column-reads "SELECT \"__mb_source\".\"PRICE\" FROM (SELECT \"ID\" AS \"ID\" FROM (SELECT \"PRICE\" AS \"PRICE\" FROM \"SHOPS\") AS \"X\") AS \"__mb_source\"" hidden))))))

(deftest ^:parallel dangling-reads-test
  (testing "a native subquery's unaliased select list is what it returns"
    (is (= [] (sql/dangling-reads "SELECT \"__mb_source\".\"NAME\" FROM (SELECT id, name, tier FROM shops WHERE tier = 1) AS \"__mb_source\"" #{"PRICE"})))
    (is (= [{:alias "__mb_source" :column "PRICE" :hidden? true}]
           (sql/dangling-reads "SELECT \"__mb_source\".\"PRICE\" FROM (SELECT id, name, tier FROM shops WHERE tier = 1) AS \"__mb_source\"" #{"PRICE"}))))
  (testing "a `*` makes the subquery's columns unknown, so nothing through it is flagged"
    (is (= [] (sql/dangling-reads "SELECT \"__mb_source\".\"PRICE\" FROM (SELECT * FROM shops) AS \"__mb_source\"" #{"PRICE"}))))
  (testing "a read of a column no subquery returns, hidden or not"
    (is (= [{:alias "S" :column "RATING" :hidden? false}]
           (sql/dangling-reads "SELECT \"S\".\"RATING\" FROM (SELECT \"ID\" AS \"ID\" FROM \"SHOPS\") AS \"S\"" #{"PRICE"})))))

(deftest ^:parallel dangling-reads-through-an-aliased-cte-test
  (testing "Postgres compiles stages as CTEs and aliases them in FROM"
    (is (= [{:alias "__mb_source" :column "price" :hidden? true}]
           (sql/dangling-reads (str "WITH \"__mb_stage_0\" AS (SELECT \"public\".\"shops\".\"id\" AS \"id\" FROM \"public\".\"shops\") "
                                    "SELECT \"__mb_source\".\"id\" AS \"id\", \"__mb_source\".\"price\" AS \"price\" "
                                    "FROM \"__mb_stage_0\" AS \"__mb_source\"")
                               #{"price"})))))

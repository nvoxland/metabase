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

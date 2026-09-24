(ns metabase.engine.catalog-test
  (:require
   [clojure.test :refer :all]
   [metabase.engine.catalog :as engine.catalog]
   [metabase.test :as mt]))

(deftest base-type->arrow-type-test
  (are [base-type arrow] (= arrow (engine.catalog/base-type->arrow-type base-type))
    :type/Integer        "Int64"
    :type/BigInteger     "Int64"
    :type/Float          "Float64"
    :type/Decimal        "Decimal128(38, 10)"
    :type/Boolean        "Boolean"
    :type/Text           "Utf8"
    :type/Date           "Date32"
    :type/DateTime       "Timestamp(Microsecond, None)"
    :type/DateTimeWithTZ "Timestamp(Microsecond, Some(\"UTC\"))"
    :type/Time           "Time64(Microsecond)"
    :type/UUID           "Utf8"
    :type/JSON           "Utf8"
    :type/*              "Utf8"))

(deftest snapshot-test
  (mt/test-drivers #{:h2}
    (let [snapshot (engine.catalog/snapshot (mt/db))
          venues   (first (filter #(= "VENUES" (:name %)) (:tables snapshot)))]
      (is (= (mt/id) (:database_id snapshot)))
      (is (= "h2" (:connector snapshot)))
      (is (= "PUBLIC" (:default_schema snapshot)))
      (is (re-matches #"\d+/\d+@.*" (:catalog_version snapshot)))
      (testing "tables carry their schema and exact names"
        (is (= "PUBLIC" (:schema venues)))
        (is (= ["ID" "NAME" "CATEGORY_ID" "LATITUDE" "LONGITUDE" "PRICE"]
               (map :name (:columns venues)))))
      (testing "columns carry Arrow types, the warehouse type, and nullability"
        ;; H2 sync doesn't record which columns are required, so nullability is only checked for shape here.
        (is (=? {:name "ID" :type "Int64" :nullable boolean? :db_type "BIGINT"}
                (first (:columns venues))))
        (is (= "Float64" (:type (nth (:columns venues) 3))))))))

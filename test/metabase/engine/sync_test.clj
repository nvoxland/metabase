(ns metabase.engine.sync-test
  "Sync metadata fetched through the engine matches what the JDBC driver reports."
  (:require
   [clojure.test :refer :all]
   [metabase.driver :as driver]
   [metabase.engine.core :as engine]
   [metabase.lib-be.core :as lib-be]
   [metabase.sync.core :as sync]
   [metabase.test :as mt]))

(defmacro ^:private with-engine [& body]
  `(mt/with-temporary-setting-values [~'engine-transport :ffi]
     (try
       ~@body
       (finally
         (engine/stop!)))))

(deftest ^:mb/engine-tests ^:mb/driver-tests describe-through-engine-matches-jdbc-test
  (mt/test-drivers #{:postgres}
    (with-engine
      (let [db (mt/db)]
        (testing "tables"
          ;; the JDBC driver answers with a reducible
          (is (= (into #{} (:tables (driver/describe-database :postgres db)))
                 (into #{} (:tables (driver/describe-database :datafusion db))))))
        (testing "fields of one table"
          (is (= (set (driver/describe-fields :postgres db :table-names ["venues"] :schema-names ["public"]))
                 (set (driver/describe-fields :datafusion db :table-names ["venues"] :schema-names ["public"])))))
        (testing "foreign keys"
          (let [metadata-db (lib-be/instance->metadata db :metadata/database)]
            (is (= (set (driver/describe-fks :postgres metadata-db))
                   (set (driver/describe-fks :datafusion metadata-db))))))
        (testing "version"
          (is (= (:version (driver/dbms-version :postgres db))
                 (:version (driver/dbms-version :datafusion db)))))))))

(deftest ^:mb/engine-tests ^:mb/driver-tests sync-routed-database-test
  (mt/test-drivers #{:postgres}
    (with-engine
      (mt/with-temporary-setting-values [query-engine :datafusion]
        (testing "a full sync of a routed database completes through the engine"
          (is (some? (sync/sync-database! (mt/db) {:scan :schema}))))))))

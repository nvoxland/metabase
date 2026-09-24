(ns metabase.engine.sync-test
  "Sync metadata fetched through the engine matches what the JDBC driver reports."
  (:require
   [clojure.test :refer :all]
   [metabase.driver :as driver]
   [metabase.engine.test-util :as engine.tu]
   [metabase.lib-be.core :as lib-be]
   [metabase.sync.core :as sync]
   [metabase.test :as mt]
   [toucan2.core :as t2]))

(deftest ^:mb/engine-tests ^:mb/driver-tests describe-through-engine-matches-jdbc-test
  (mt/test-drivers #{:postgres}
    (engine.tu/with-engine
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
    (engine.tu/with-engine
      (testing "a fresh sync of a routed database through the engine finds the same tables, fields and FKs as JDBC"
        (let [jdbc-db     (mt/db)
              table-ids   (t2/select-fn-set :id :model/Table :db_id (:id jdbc-db) :active true)
              jdbc-counts {:tables (count table-ids)
                           :fields (t2/count :model/Field :table_id [:in table-ids] :active true)
                           :fks    (t2/count :model/Field :table_id [:in table-ids] :active true :fk_target_field_id [:not= nil])}]
          (mt/with-temp [:model/Database db {:engine   :postgres
                                              :details  (:details jdbc-db)
                                              :settings {:query-engine "datafusion"}}]
            (sync/sync-database! db {:scan :schema})
            (let [table-ids (t2/select-fn-set :id :model/Table :db_id (:id db) :active true)]
              (is (pos? (:fields jdbc-counts)))
              (is (= jdbc-counts
                     {:tables (count table-ids)
                      :fields (if (seq table-ids) (t2/count :model/Field :table_id [:in table-ids] :active true) 0)
                      :fks    (if (seq table-ids) (t2/count :model/Field :table_id [:in table-ids] :active true :fk_target_field_id [:not= nil]) 0)})))))))))

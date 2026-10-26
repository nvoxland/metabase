(ns metabase-enterprise.sandbox.restricted-db-test-util
  "Build a pair of H2 databases for differential sandbox tests: a base copy of some tables, and a restricted copy that
  is the same data with a sandbox applied by hand (rows deleted, columns nulled). A query run as a sandboxed user on the
  base copy must give the same rows as the same query run as an admin on the restricted copy.

  Both copies keep the source rows' primary keys, so one query renders against either copy from the table and column
  names alone."
  (:require
   [clojure.java.jdbc :as jdbc]
   [clojure.string :as str]
   [metabase.driver.sql-jdbc.connection :as sql-jdbc.conn]
   [metabase.sync.core :as sync]
   [metabase.test :as mt]
   [toucan2.core :as t2]))

(defn- columns
  "Column name -> H2 type name for `table` in the PUBLIC schema of `spec`, in column order."
  [spec table]
  (mapv (juxt :column_name :data_type)
        (jdbc/query spec ["SELECT COLUMN_NAME, DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS
                           WHERE TABLE_SCHEMA = 'PUBLIC' AND TABLE_NAME = ? ORDER BY ORDINAL_POSITION" table])))

(defn- copy-table!
  "Create `table` in `to-spec` with the columns and rows it has in `from-spec`, and `ID` as its primary key. No foreign
  key constraints: the restricted copy has dangling keys by design."
  [from-spec to-spec table]
  (let [cols (columns from-spec table)
        ddl  (format "CREATE TABLE \"%s\" (%s, PRIMARY KEY (\"ID\"))"
                     table
                     (str/join ", " (for [[col type] cols] (format "\"%s\" %s" col type))))
        ;; the header row of `:as-arrays?` is lower-cased, so take the column names from `cols`
        rows (rest (jdbc/query from-spec [(format "SELECT * FROM \"PUBLIC\".\"%s\"" table)] {:as-arrays? true}))]
    (jdbc/execute! to-spec [ddl])
    (when (seq rows)
      (jdbc/insert-multi! to-spec table (mapv first cols) rows {:entities #(str "\"" % "\"")}))))

(defn- set-fk-metadata!
  "Mark `[from-table from-col to-table to-col]` as a foreign key in Metabase's metadata for `db-id`, the way sync would
  from a real constraint."
  [db-id [from-table from-col to-table to-col]]
  (let [field-id (fn [table col]
                   (t2/select-one-pk :model/Field
                                     :name col
                                     :table_id (t2/select-one-pk :model/Table :db_id db-id :name table)))]
    (t2/update! :model/Field (field-id from-table from-col)
                {:semantic_type      :type/FK
                 :fk_target_field_id (field-id to-table to-col)})))

(defn- create-db!
  "Create an in-memory H2 database named `db-name` holding `tables` copied from `from-spec`, run `restrict-sql` in it,
  register and sync it in Metabase, and mark `fks`. Returns the Metabase Database."
  [from-spec dataset-name db-name tables fks restrict-sql]
  (let [details {:db (format "mem:%s;DB_CLOSE_DELAY=-1" db-name)}
        spec    {:classname "org.h2.Driver" :subprotocol "h2" :subname (:db details)}]
    (doseq [table tables]
      (copy-table! from-spec spec table))
    (doseq [stmt restrict-sql]
      (jdbc/execute! spec [stmt]))
    (let [db (t2/insert-returning-instance! :model/Database {:name     db-name
                                                             :engine   :h2
                                                             :details  details
                                                             ;; `mt/with-db` and `mt/id` resolve names through this
                                                             :settings {:database-source-dataset-name dataset-name}})]
      (sync/sync-database! db {:scan :schema})
      (doseq [fk fks]
        (set-fk-metadata! (:id db) fk))
      db)))

(defn- drop-db! [{db-id :id {h2-db :db} :details}]
  (t2/delete! :model/Database :id db-id)
  ;; SHUTDOWN closes the connection, so it cannot run inside a transaction that then commits
  (jdbc/execute! {:classname "org.h2.Driver" :subprotocol "h2" :subname h2-db} ["SHUTDOWN"] {:transaction? false}))

(defn do-with-restricted-pair!
  "Copy `tables` from the current test database (which must be H2) into a fresh base database and a fresh restricted
  database, run `restrict-sql` statements in the restricted one only, mark `fks` (`[from-table from-col to-table
  to-col]`) in both, and call `(f base-db restricted-db)`. Both databases are removed afterwards."
  [{:keys [tables fks restrict-sql]} f]
  (let [from-spec    (sql-jdbc.conn/db->pooled-connection-spec (mt/db))
        dataset-name (get-in (mt/db) [:settings :database-source-dataset-name])
        suffix       (str (random-uuid))
        base         (create-db! from-spec dataset-name (str "sbx-base-" suffix) tables fks nil)]
    (try
      (let [restricted (create-db! from-spec dataset-name (str "sbx-restricted-" suffix) tables fks restrict-sql)]
        (try
          (f base restricted)
          (finally
            (drop-db! restricted))))
      (finally
        (drop-db! base)))))

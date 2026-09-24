(ns metabase.driver.datafusion
  "The engine-facing driver: compiles MBQL to DataFusion SQL and hands it to the native query engine
  (see dev/notes/datafusion-engine.md). Registered abstract so it never appears as an engine people
  can add; the query processor binds it for databases routed through the engine.

  Everything about the warehouse that isn't SQL generation (start of week, default timezone) is
  delegated to the database's own driver, because the database keeps its real engine."
  (:require
   [clojure.core.async :as a]
   [medley.core :as m]
   [metabase.driver :as driver]
   [metabase.driver-api.core :as driver-api]
   [metabase.driver.connection :as driver.conn]
   [metabase.driver.datafusion.query-processor]
   [metabase.driver.settings :as driver.settings]
   [metabase.driver.sql-jdbc.sync :as sql-jdbc.sync]
   [metabase.driver.sql-jdbc.sync.describe-table :as sql-jdbc.describe-table]
   [metabase.driver.sql.query-processor.like-escape-char-built-in :as-alias like-escape-char-built-in]
   [metabase.driver.sync :as driver.s]
   [metabase.driver.util :as driver.u]
   [metabase.engine.arrow :as engine.arrow]
   [metabase.engine.catalog :as engine.catalog]
   [metabase.engine.core :as engine]
   [metabase.premium-features.core :refer [defenterprise]]
   [metabase.util :as u]
   [metabase.util.log :as log]
   [metabase.util.performance :as perf])
  (:import
   (java.time Instant LocalDate LocalDateTime LocalTime OffsetDateTime ZonedDateTime)))

(set! *warn-on-reflection* true)

(comment metabase.driver.datafusion.query-processor/keep-me)

(driver/register! :datafusion
                  :parent #{:sql ::like-escape-char-built-in/like-escape-char-built-in}
                  :abstract? true)

(defn warehouse-driver
  "The driver of the database the current query runs against."
  []
  (keyword (:engine (driver-api/database (driver-api/metadata-provider)))))

;;; Features the engine dialect supports today. Anything absent falls back to the `:sql` defaults. Constructs the
;;; engine can plan but a warehouse can't run are rejected by the engine at query time, not here.
(doseq [[feature supported?] {:convert-timezone               false ; needs timezone-aware unparsing; tracked for M4
                              :datetime-diff                  true
                              :expression-literals            true
                              :expressions                    true
                              :expressions/date               true
                              :expressions/float              true
                              :expressions/integer            true
                              :expressions/text               true
                              :identifiers-with-spaces        true
                              :left-join                      true
                              :right-join                     true
                              :inner-join                     true
                              :full-join                      true
                              :native-parameters              true
                              :nested-field-columns           false
                              :nested-queries                 true
                              :now                            true
                              :percentile-aggregations        false ; WITHIN GROUP support in DataFusion unverified
                              :regex                          true
                              :schemas                        true
                              :set-timezone                   true
                              :split-part                     true
                              :standard-deviation-aggregations true
                              :uuid-type                      false
                              :window-functions/cumulative    true
                              :window-functions/offset        true}]
  (defmethod driver/database-supports? [:datafusion feature] [_driver _feature _db] supported?))

(defmethod driver/db-start-of-week :datafusion
  [_driver]
  (driver/db-start-of-week (warehouse-driver)))

(defmethod driver/db-default-timezone :datafusion
  [_driver database]
  (driver/db-default-timezone (warehouse-driver) database))

;;; ------------------------------------------------ engine registration -------------------------------------------------

(defonce ^:private registered
  ;; database id -> {:stamp [catalog version, details hash], :transport <the engine it was registered with>}
  ;; The transport is part of the key so a restarted engine (which starts empty) gets registered again.
  (atom {}))

(defn- pool-config []
  {:max_size            (driver.settings/jdbc-data-warehouse-max-connection-pool-size)
   :checkout_timeout_ms 10000
   :connect_timeout_ms  (driver.settings/db-connection-timeout-ms)})

(defn- ensure-registered!
  "Make sure the engine has the database's connection details and current catalog. Checks a cheap
  metadata stamp on every query and only builds the full snapshot when it changed."
  [database]
  (let [details (driver.conn/effective-details database)
        stamp   [(engine.catalog/stamp database) (hash details)]]
    (when (not= (get @registered (:id database)) {:stamp stamp :transport (engine/transport)})
      (let [snapshot (engine.catalog/snapshot database)]
        (log/infof "Registering database %d with the native query engine (%s)" (:id database) (:catalog_version snapshot))
        (engine/register-database! {:database_id (:id database)
                                    :connector   (name (:engine database))
                                    :details     details
                                    :pool        (pool-config)})
        (engine/register-catalog! snapshot)
        (swap! registered assoc (:id database) {:stamp stamp :transport (engine/transport)})))))

(defenterprise impersonation-role
  "The warehouse role a routed query must run as: the current user's impersonation role, else the
  database's default role, when connection impersonation applies. OSS has no impersonation."
  metabase-enterprise.impersonation.driver
  [_database]
  nil)

(defn forget-database!
  "Drop what the engine knows about a database (after its details or metadata change)."
  [database-id]
  (swap! registered dissoc database-id)
  (engine/invalidate-database! database-id))

;;; ---------------------------------------------------- execution -------------------------------------------------------

(defn- param->engine
  "A compiled parameter value as the engine's `{:type :value}` shape."
  [value]
  (condp instance? value
    Long                 {:type "Int64" :value value}
    Integer              {:type "Int32" :value value}
    Short                {:type "Int32" :value value}
    Double               {:type "Float64" :value value}
    Float                {:type "Float64" :value value}
    java.math.BigDecimal {:type "Decimal128(38, 10)" :value (str value)}
    Boolean              {:type "Boolean" :value value}
    LocalDate            {:type "Date32" :value (str value)}
    LocalDateTime        {:type "Timestamp(Microsecond, None)" :value (str value)}
    LocalTime            {:type "Time64(Microsecond)" :value (str value)}
    OffsetDateTime       {:type "Timestamp(Microsecond, Some(\"UTC\"))" :value (str (.toInstant ^OffsetDateTime value))}
    ZonedDateTime        {:type "Timestamp(Microsecond, Some(\"UTC\"))" :value (str (.toInstant ^ZonedDateTime value))}
    Instant              {:type "Timestamp(Microsecond, Some(\"UTC\"))" :value (str value)}
    {:type "Utf8" :value (some-> value str)}))

(defn- session-context [driver database outer-query]
  {:database_id          (:id database)
   ;; Only an impersonation role is set; the engine resets the role after every statement, so the warehouse's
   ;; default role (which Postgres reports as the special `NONE`) never needs setting.
   :role                 (when (driver.u/supports? (warehouse-driver) :connection-impersonation database)
                           (let [role (impersonation-role database)]
                             (when-not (or (nil? role) (= (u/lower-case-en role) "none"))
                               role)))
   :user_id              (-> outer-query :info :executed-by)
   :report_timezone      (driver-api/report-timezone-id-if-supported driver database)
   :row_limit            (driver-api/determine-query-max-rows outer-query)
   :remark               (driver-api/query->remark driver outer-query)
   :statement_timeout_ms (* 60000 (driver.settings/db-query-timeout-minutes))
   :query_origin         (when-let [context (-> outer-query :info :context)]
                           {:type (name context)})})

(defmethod driver/execute-reducible-query :datafusion
  [driver {{sql :query, params :params} :native, :as outer-query} _context respond]
  (let [database (driver-api/database (driver-api/metadata-provider))]
    (ensure-registered! database)
    (engine/with-session [session (session-context driver database outer-query)]
      (let [{:keys [reader cancel! close!]} (engine/execute! session sql (perf/mapv param->engine params))
            canceled-chan                   (driver-api/canceled-chan)]
        (when canceled-chan
          (a/go
            (when (a/<! canceled-chan)
              (try
                (cancel!)
                (catch Throwable e
                  (log/warn e "Couldn't cancel engine query"))))))
        (try
          (respond {:cols (engine.arrow/cols reader)}
                   (driver-api/reducible-rows (engine.arrow/row-thunk reader) canceled-chan))
          (finally
            (close!)))))))

;;; ------------------------------------------------------ sync ---------------------------------------------------------
;;;
;;; Sync runs the warehouse driver's own metadata SQL, but through the engine's connection pool instead of JDBC, so a
;;; database routed to the engine has a single connection stack.

(defn- numbered-placeholders
  "Rewrite JDBC-style `?` placeholders as `$1`, `$2`, ... outside string literals and quoted identifiers
  (a doubled quote inside either toggles twice, so it needs no special case)."
  [sql]
  (let [out (StringBuilder.)]
    (loop [chars (seq sql) quote nil n 0]
      (if-let [c (first chars)]
        (cond
          (= c quote)                        (do (.append out c) (recur (rest chars) nil n))
          (and (nil? quote) (#{\' \"} c))    (do (.append out c) (recur (rest chars) c n))
          (and (nil? quote) (= c \?))        (do (.append out (str "$" (inc n))) (recur (rest chars) nil (inc n)))
          :else                              (do (.append out c) (recur (rest chars) quote n)))
        (str out)))))

(defn- rows->maps [{:keys [cols rows]}]
  (let [ks (perf/mapv (comp keyword :name) cols)]
    (map #(zipmap ks %) rows)))

(defn- metadata-query!
  "Run one of the warehouse driver's metadata statements through the engine and return its rows as maps."
  [database [sql & params]]
  (ensure-registered! database)
  (engine/with-session [session {:database_id (:id database)}]
    (rows->maps (engine/query-rows! session (numbered-placeholders sql) (perf/mapv param->engine params)))))

(defn- warehouse-driver-of [database]
  (keyword (:engine database)))

(defmethod driver/dbms-version :datafusion
  [_driver database]
  (let [[{:keys [version]}] (metadata-query! database ["SELECT current_setting('server_version') AS version"])
        [major minor]       (map parse-long (re-seq #"\d+" (str version)))]
    {:flavor           "PostgreSQL"
     :version          version
     :semantic-version (into [] (remove nil?) [major minor])}))

(def ^:private syncable-tables-sql
  "The same listing as the Postgres driver's `get-tables-sql`, with the privilege checks the JDBC path makes in a
  second query folded in. Foreign tables get no writability verdict, as on the JDBC path."
  "SELECT n.nspname AS schema,
          c.relname AS name,
          CASE c.relkind WHEN 'r' THEN 'TABLE' WHEN 'p' THEN 'PARTITIONED TABLE' WHEN 'v' THEN 'VIEW'
                         WHEN 'f' THEN 'FOREIGN TABLE' WHEN 'm' THEN 'MATERIALIZED VIEW' END AS type,
          d.description AS description,
          NULLIF(stat.n_live_tup, 0) AS estimated_row_count,
          has_table_privilege(c.oid, 'SELECT') AS can_select,
          CASE WHEN c.relkind = 'f' THEN NULL
               ELSE has_any_column_privilege(c.oid, 'INSERT')
                    AND has_any_column_privilege(c.oid, 'UPDATE')
                    AND has_table_privilege(c.oid, 'DELETE') END AS is_writable
   FROM pg_catalog.pg_class c
   JOIN pg_catalog.pg_namespace n ON c.relnamespace = n.oid
   LEFT JOIN pg_catalog.pg_description d ON c.oid = d.objoid AND d.objsubid = 0 AND d.classoid = 'pg_class'::regclass
   LEFT JOIN pg_stat_user_tables stat ON n.nspname = stat.schemaname AND c.relname = stat.relname
   WHERE n.nspname !~ '^pg_' AND n.nspname <> 'information_schema'
     AND c.relkind IN ('r', 'p', 'v', 'f', 'm')
   ORDER BY type, schema, name")

(defmethod driver/describe-database* :datafusion
  [_driver database]
  (let [excluded            (set (sql-jdbc.sync/excluded-schemas (warehouse-driver-of database)))
        [inclusion
         exclusion]         (driver.s/db-details->schema-filter-patterns database)
        syncable-schema?    (fn [schema]
                              (and (not (contains? excluded schema))
                                   (not (re-find #"^metabase_cache.*" schema))
                                   (driver.s/include-schema? inclusion exclusion schema)))]
    {:tables (into #{}
                   (comp (filter (comp syncable-schema? :schema))
                         (filter :can_select)
                         (map #(dissoc % :type :can_select)))
                   (metadata-query! database [syncable-tables-sql]))}))

(defmethod driver/describe-fields :datafusion
  [_driver database & {:keys [schema-names table-names] :as opts}]
  ;; the same pipeline as `sql-jdbc.sync/describe-fields`, fed by the engine instead of a JDBC result set
  (if (or (and schema-names (perf/empty? schema-names))
          (and table-names (perf/empty? table-names)))
    []
    (let [warehouse (warehouse-driver-of database)
          sql       (sql-jdbc.sync/describe-fields-sql warehouse (assoc opts :details (driver.conn/effective-details database)))]
      (eduction (comp (m/mapply sql-jdbc.sync/describe-fields-pre-process-xf warehouse database opts)
                      (sql-jdbc.describe-table/limit-fields-per-table-xf (driver.settings/sync-max-fields-per-table))
                      (sql-jdbc.describe-table/describe-fields-xf warehouse database))
                (metadata-query! database sql)))))

(defmethod driver/describe-fks :datafusion
  [_driver database & {:as opts}]
  (metadata-query! database (sql-jdbc.sync/describe-fks-sql (warehouse-driver-of database) opts)))

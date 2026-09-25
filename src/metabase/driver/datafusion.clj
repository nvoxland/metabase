(ns metabase.driver.datafusion
  "The engine-facing driver: compiles MBQL to DataFusion SQL and hands it to the native query engine
  (see dev/notes/datafusion-engine.md). Registered abstract so it never appears as an engine people
  can add; the query processor binds it for databases routed through the engine.

  Everything about the warehouse that isn't SQL generation (start of week, default timezone) is
  delegated to the database's own driver, because the database keeps its real engine."
  (:require
   [clojure.core.async :as a]
   [clojure.string :as str]
   [medley.core :as m]
   [metabase.driver :as driver]
   [metabase.driver-api.core :as driver-api]
   [metabase.driver.connection :as driver.conn]
   [metabase.driver.datafusion.query-processor]
   [metabase.driver.settings :as driver.settings]
   [metabase.driver.sql :as driver.sql]
   [metabase.driver.sql-jdbc.sync :as sql-jdbc.sync]
   [metabase.driver.sql-jdbc.sync.describe-table :as sql-jdbc.describe-table]
   [metabase.driver.sql.query-processor.like-escape-char-built-in :as-alias like-escape-char-built-in]
   [metabase.driver.sync :as driver.s]
   [metabase.driver.util :as driver.u]
   [metabase.engine.arrow :as engine.arrow]
   [metabase.engine.catalog :as engine.catalog]
   [metabase.engine.core :as engine]
   [metabase.engine.settings :as engine.settings]
   [metabase.premium-features.core :refer [defenterprise]]
   [metabase.util :as u]
   [metabase.util.i18n :refer [tru]]
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
(doseq [[feature supported?] {:convert-timezone               true
                              :datetime-diff                  true
                              ;; sync: the warehouse driver's metadata SQL runs through the engine (see below)
                              :describe-fields                true
                              :metadata/key-constraints       true
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
                              :native-parameters              true
                              :nested-queries                 true
                              :now                            true
                              ;; persisted model tables live outside the synced catalog; the model's own query runs instead
                              :persist-models                 false
                              :split-part                     true
                              :standard-deviation-aggregations true
                              :uuid-type                      false
                              :window-functions/cumulative    true}]
  (defmethod driver/database-supports? [:datafusion feature] [_driver _feature _db] supported?))

;;; Features that depend on the warehouse behind the engine rather than the engine: what the plan can say is one
;;; thing, what the warehouse can run another (MySQL has no schemas, no FULL JOIN, no GROUPING SETS, no percentiles).
(doseq [feature [:schemas
                 ;; index sync happens only where the warehouse driver does it (see `describe-indexes` below)
                 :describe-indexes
                 :index-info
                 :full-join
                 :percentile-aggregations
                 :native-pivot-tables
                 :window-functions/offset
                 :set-timezone
                 :regex
                 ;; JSON-path columns are the warehouse driver's to find (below) and to query (routing keeps their
                 ;; queries on JDBC); a routed database must not lose them at sync
                 :nested-field-columns]]
  (defmethod driver/database-supports? [:datafusion feature]
    [_driver feature database]
    (if-let [warehouse (some-> database :engine keyword)]
      (driver/database-supports? warehouse feature database)
      true)))

(defmethod driver/db-start-of-week :datafusion
  [_driver]
  ;; the SQL is DataFusion's, whose `date_trunc('week', ...)` starts weeks on Monday whatever the warehouse does
  :monday)

(defmethod driver/db-default-timezone :datafusion
  [_driver database]
  ;; sync calls this outside any query, so the warehouse comes from the database, not the query's metadata
  (driver/db-default-timezone (keyword (:engine database)) database))

;;; ------------------------------------------------ engine registration -------------------------------------------------

(defn- pool-config []
  {:max_size            (driver.settings/jdbc-data-warehouse-max-connection-pool-size)
   :checkout_timeout_ms 10000
   :connect_timeout_ms  (driver.settings/db-connection-timeout-ms)})

(def ^:private stamp-ttl-ms
  "How long a catalog stamp is trusted before the app DB is asked again. The stamp is two aggregate
  queries over the database's fields; once per query would double app-DB load under traffic."
  10000)

(defonce ^:private stamps
  ;; database id -> {:stamp .. :at ms}
  (atom {}))

(defn- catalog-stamp [database]
  (let [now (System/currentTimeMillis)
        {:keys [stamp at]} (get @stamps (:id database))]
    (if (and stamp (< (- now at) stamp-ttl-ms))
      stamp
      (let [fresh (engine.catalog/stamp database)]
        (swap! stamps assoc (:id database) {:stamp fresh :at now})
        fresh))))

(defn- metadata-database
  "The database whose synced tables and fields `database` queries: itself, or for a database-routing
  destination (which has no synced metadata of its own) its router."
  [database]
  (if-let [router-id (:router-database-id database)]
    (assoc database :id router-id)
    database))

(defn- registration-behind?
  "Whether the metadata the engine was registered with for `database` has since changed (a sync, a
  schema change), as opposed to a query naming a column that doesn't exist."
  [database]
  (let [metadata (metadata-database database)]
    (not= (:stamp (get @stamps (:id metadata))) (engine.catalog/stamp metadata))))

(defn- ensure-registered!
  "Make sure the engine has the database's connection details and current catalog. Checks a cheap
  metadata stamp on every query and only builds the full snapshot when it changed."
  [database]
  ;; The transport is part of the stamp so a restarted engine (which starts empty) gets registered again.
  (let [details  (driver.conn/effective-details database)
        metadata (metadata-database database)
        stamp    {:stamp [(catalog-stamp metadata) (hash details)] :transport (engine/transport)}]
    (when (not= (engine/registration (:id database)) stamp)
      (let [snapshot (assoc (engine.catalog/snapshot metadata) :database_id (:id database))]
        (log/infof "Registering database %d with the native query engine (%s)" (:id database) (:catalog_version snapshot))
        (engine/register-database! (if (contains? (engine.settings/bridge-warehouses) (:engine database))
                                     ;; the engine reaches this warehouse through Metabase's own JDBC pool
                                     {:database_id (:id database)
                                      :connector   "jdbc-bridge"
                                      :details     {:url         (or (engine.settings/bridge-url)
                                                                     (throw (ex-info (tru "The JDBC bridge is not running.") {})))
                                                    :token       (engine.settings/bridge-token)
                                                    :database_id (:id database)
                                                    :warehouse   (name (:engine database))}
                                      :pool        (pool-config)}
                                     {:database_id (:id database)
                                      :connector   (name (:engine database))
                                      :details     details
                                      :pool        (pool-config)}))
        (engine/register-catalog! snapshot)
        (engine/remember-registration! (:id database) stamp)))))

(defenterprise impersonation-role
  "The warehouse role a routed query must run as: the current user's impersonation role, else the
  database's default role, when connection impersonation applies. OSS has no impersonation."
  metabase-enterprise.impersonation.driver
  [_database]
  nil)

(defn forget-database!
  "Drop what the engine knows about a database (after its details or metadata change)."
  [database-id]
  (swap! stamps dissoc database-id)
  (engine/forget-database! database-id))

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
   :remark               (driver-api/query->remark driver outer-query)
   :statement_timeout_ms (* 60000 (driver.settings/db-query-timeout-minutes))
   ;; the row cap the JDBC path passes to `Statement.setMaxRows`; the engine makes it a LIMIT the warehouse applies
   :row_limit            (driver-api/determine-query-max-rows outer-query)
   :query_origin         (when-let [context (-> outer-query :info :context)]
                           {:type (name context)})})

(defn- stale-registration-error?
  "Whether the engine failed because what it holds for `database` is behind: it has no registration
  at all (a restarted sidecar starts empty while this process still remembers registering), or its
  catalog doesn't know a table or column and the database's metadata has changed since it was
  registered (the snapshot predates a sync, or a schema change). A query naming a column that never
  existed fails without a re-registration."
  [e database]
  (let [{:keys [kind message]} (:engine/error (ex-data e))]
    (boolean (or (= kind "catalog_missing")
                 (and (= kind "plan")
                      (re-find #"table '[^']*' not found|No field named" (str message))
                      (registration-behind? database))))))

(defn- start-statement!
  "Register the database if needed, open a session, and start `sql`: `[session-id handle]`. The
  session is destroyed if the statement can't start."
  [driver database outer-query sql params]
  (ensure-registered! database)
  (let [session (engine/create-session! (session-context driver database outer-query))]
    (try
      [session (engine/execute! session sql params)]
      (catch Throwable e
        (u/ignore-exceptions (engine/destroy-session! session))
        (throw e)))))

(defn- native-form
  "The warehouse SQL as a native form: with its `$1`, `$2`, ... placeholders as JDBC's `?` when they appear in
  order (the driver's native path binds by position), else as the engine sent it."
  [warehouse-sql params]
  (let [numbers (map (comp parse-long second) (re-seq #"\$(\d+)" warehouse-sql))]
    (if (= numbers (range 1 (inc (count numbers))))
      {:query (str/replace warehouse-sql #"\$\d+" "?") :params (vec params)}
      {:query warehouse-sql :params (vec params)})))

(defmethod driver/execute-reducible-query :datafusion
  [driver {{sql :query, params :params} :native, :as outer-query} _context respond]
  (let [database         (driver-api/database (driver-api/metadata-provider))
        params           (perf/mapv param->engine params)
        [session handle] (try
                           (start-statement! driver database outer-query sql params)
                           (catch clojure.lang.ExceptionInfo e
                             (if (stale-registration-error? e database)
                               ;; register again from a fresh snapshot and try once more
                               (do (swap! stamps dissoc (:id (metadata-database database)))
                                   (engine/forget-registration! (:id database))
                                   (start-statement! driver database outer-query sql params))
                               (throw e))))
        {:keys [reader cancel! close!]} handle
        canceled-chan    (driver-api/canceled-chan)]
    (when canceled-chan
      (a/go
        (when (a/<! canceled-chan)
          (try
            (cancel!)
            (catch Throwable e
              (log/warn e "Couldn't cancel engine query"))))))
    (try
      (respond (cond-> {:cols (engine.arrow/cols reader)}
                 ;; what ran on the warehouse is the query's native form (the compiled engine-dialect SQL isn't
                 ;; something the warehouse driver could run again)
                 (engine.arrow/warehouse-sql reader)
                 (assoc :native_form (native-form (engine.arrow/warehouse-sql reader) (:params (:native outer-query)))))
               (driver-api/reducible-rows (engine.arrow/row-thunk reader) canceled-chan))
      (finally
        (close!)
        (u/ignore-exceptions (engine/destroy-session! session))))))

;;; ------------------------------------------------------ sync ---------------------------------------------------------
;;;
;;; Sync runs the warehouse driver's own metadata SQL, but through the engine's connection pool instead of JDBC, so a
;;; database routed to the engine has a single connection stack.

(defn- numbered-placeholders
  "Rewrite JDBC-style `?` placeholders as `$1`, `$2`, ... outside string literals and quoted identifiers
  (a doubled quote inside either toggles twice, so it needs no special case). Backticks are MySQL's
  identifier quotes, and a column alias like `pk?` is not a placeholder."
  [sql]
  (let [out (StringBuilder.)]
    (loop [chars (seq sql) quote nil n 0]
      (if-let [c (first chars)]
        (cond
          (= c quote)                        (do (.append out c) (recur (rest chars) nil n))
          (and (nil? quote) (#{\' \" \`} c)) (do (.append out c) (recur (rest chars) c n))
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

(defn- metadata-reducible
  "Like [[metadata-query!]], but the rows come out of the engine as the returned reducible is reduced,
  never all at once: the field listing of a large warehouse is the one metadata result that doesn't
  fit in memory comfortably. The session lives for the reduction."
  [database [sql & params]]
  (ensure-registered! database)
  (reify clojure.lang.IReduceInit
    (reduce [_ rf init]
      (engine/with-session [session {:database_id (:id database)}]
        (let [{:keys [reader close!]} (engine/execute-raw! session (numbered-placeholders sql) (perf/mapv param->engine params))]
          (try
            (let [ks       (perf/mapv (comp keyword :name) (engine.arrow/cols reader))
                  next-row (engine.arrow/row-thunk reader)]
              (loop [acc init]
                (if-let [row (next-row)]
                  (let [acc (rf acc (zipmap ks row))]
                    (if (reduced? acc)
                      @acc
                      (recur acc)))
                  acc)))
            (finally
              (close!))))))))

(defn- warehouse-driver-of [database]
  (keyword (:engine database)))

(defmulti ^:private dbms-version*
  "The warehouse's version, the way the JDBC path reports it, fetched through the engine."
  {:arglists '([warehouse database])}
  (fn [warehouse _database] warehouse))

(defmethod dbms-version* :postgres
  [_warehouse database]
  (let [[{:keys [version]}] (metadata-query! database ["SELECT current_setting('server_version') AS version"])
        [major minor]       (map parse-long (re-seq #"\d+" (str version)))]
    {:flavor           "PostgreSQL"
     :version          version
     :semantic-version (into [] (remove nil?) [major minor])}))

(defmethod dbms-version* :mysql
  [_warehouse database]
  (let [[{:keys [version]}] (metadata-query! database ["SELECT VERSION() AS version"])
        version             (str version)
        mariadb?            (re-find #"(?i)mariadb" version)
        ;; older MariaDB servers announce themselves as `5.5.5-10.6.12-MariaDB`
        [major minor]       (map parse-long (re-seq #"\d+" (str/replace version #"^5\.5\.5-" "")))]
    {:flavor           (if mariadb? "MariaDB" "MySQL")
     :version          version
     :semantic-version (into [] (remove nil?) [major minor])}))

(defmethod driver/dbms-version :datafusion
  [_driver database]
  (dbms-version* (warehouse-driver-of database) database))

(defmulti ^:private syncable-tables-sql
  "The listing of syncable tables for a warehouse: `schema`, `name`, `type`, `description`, `estimated_row_count`,
  `can_select`, `is_writable`, in the warehouse's own dialect (it runs without planning)."
  {:arglists '([warehouse])}
  identity)

(defmethod syncable-tables-sql :mysql
  [_warehouse]
  ;; The same shape as the JDBC path's `DatabaseMetaData` listing: no schema, no row estimate, and no writability
  ;; verdict (JDBC parses `SHOW GRANTS` on MySQL and gives up on MariaDB; here both give up, so actions on a routed
  ;; MySQL database wait for a grants query through the engine). Readability isn't checked either way: a table the
  ;; user can't read fails at query time rather than being hidden by sync.
  "SELECT NULL AS `schema`,
          table_name AS `name`,
          table_type AS `type`,
          NULLIF(table_comment, '') AS description,
          TRUE AS can_select,
          NULL AS is_writable
   FROM information_schema.tables
   WHERE table_schema = DATABASE()
     AND table_type IN ('BASE TABLE', 'VIEW', 'SYSTEM VERSIONED')
   ORDER BY table_name")

(defmethod syncable-tables-sql :postgres
  [_warehouse]
  ;; The same listing as the Postgres driver's `get-tables-sql`, with the privilege checks the JDBC path makes in a
  ;; second query folded in. Foreign tables get no writability verdict, as on the JDBC path.
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
                              ;; a warehouse without schemas has nothing to filter on
                              (or (nil? schema)
                                  (and (not (contains? excluded schema))
                                       (not (re-find #"^metabase_cache.*" schema))
                                       (driver.s/include-schema? inclusion exclusion schema))))]
    {:tables (into #{}
                   (comp (filter (comp syncable-schema? :schema))
                         (filter :can_select)
                         (map #(dissoc % :type :can_select)))
                   (metadata-query! database [(syncable-tables-sql (warehouse-driver-of database))]))}))

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
                (metadata-reducible database sql)))))

(defmethod driver/describe-fks :datafusion
  [_driver database & {:keys [schema-names table-names] :as opts}]
  (if (or (and schema-names (perf/empty? schema-names))
          (and table-names (perf/empty? table-names)))
    []
    (metadata-query! database (sql-jdbc.sync/describe-fks-sql (warehouse-driver-of database)
                                                              (assoc opts :details (driver.conn/effective-details database))))))

(defn- index-sql?
  "Whether `warehouse` lists indexes with SQL of its own (Postgres). The others read JDBC metadata, which only the
  driver's own connection can do."
  [warehouse]
  (some? (get-method sql-jdbc.sync/describe-indexes-sql warehouse)))

(defmethod driver/describe-indexes :datafusion
  [_driver database & {:keys [schema-names table-names] :as opts}]
  (let [warehouse (warehouse-driver-of database)]
    (cond
      (or (and schema-names (perf/empty? schema-names))
          (and table-names (perf/empty? table-names)))
      []

      (index-sql? warehouse)
      (eduction (map #(perf/select-keys % [:table-schema :table-name :field-name]))
                (metadata-query! database (sql-jdbc.sync/describe-indexes-sql warehouse
                                                                              (assoc opts :details (driver.conn/effective-details database)))))

      :else
      (apply driver/describe-indexes warehouse database (mapcat identity opts)))))

(defmethod driver/describe-table-indexes :datafusion
  [driver database table]
  (let [warehouse (warehouse-driver-of database)]
    (if (index-sql? warehouse)
      (into #{}
            (map (fn [{:keys [field-name]}] {:type :normal-column-index :value field-name}))
            (driver/describe-indexes driver database :schema-names [(:schema table)] :table-names [(:name table)]))
      (driver/describe-table-indexes warehouse database table))))

;;; Sync steps with no engine equivalent yet defer to the warehouse driver.
(defmethod driver/table-exists? :datafusion
  [_driver database table]
  (driver/table-exists? (warehouse-driver-of database) database table))

(defmethod sql-jdbc.sync/describe-nested-field-columns :datafusion
  [_driver database table]
  (sql-jdbc.sync/describe-nested-field-columns (warehouse-driver-of database) database table))

(defmethod driver.sql/default-schema :datafusion
  [_driver database]
  (driver.sql/default-schema (warehouse-driver-of database) database))

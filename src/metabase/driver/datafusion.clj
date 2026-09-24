(ns metabase.driver.datafusion
  "The engine-facing driver: compiles MBQL to DataFusion SQL and hands it to the native query engine
  (see dev/notes/datafusion-engine.md). Registered abstract so it never appears as an engine people
  can add; the query processor binds it for databases routed through the engine.

  Everything about the warehouse that isn't SQL generation (start of week, default timezone) is
  delegated to the database's own driver, because the database keeps its real engine."
  (:require
   [clojure.core.async :as a]
   [metabase.driver :as driver]
   [metabase.driver-api.core :as driver-api]
   [metabase.driver.connection :as driver.conn]
   [metabase.driver.datafusion.query-processor]
   [metabase.driver.settings :as driver.settings]
   [metabase.driver.sql.query-processor.like-escape-char-built-in :as-alias like-escape-char-built-in]
   [metabase.engine.arrow :as engine.arrow]
   [metabase.engine.catalog :as engine.catalog]
   [metabase.engine.core :as engine]
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
  "Make sure the engine has the database's connection details and current catalog."
  [database]
  (let [details  (driver.conn/effective-details database)
        snapshot (engine.catalog/snapshot database)
        stamp    [(:catalog_version snapshot) (hash details)]]
    (when (not= (get @registered (:id database)) {:stamp stamp :transport (engine/transport)})
      (log/infof "Registering database %d with the native query engine (%s)" (:id database) (:catalog_version snapshot))
      (engine/register-database! {:database_id (:id database)
                                  :connector   (name (:engine database))
                                  :details     details
                                  :pool        (pool-config)})
      (engine/register-catalog! snapshot)
      (swap! registered assoc (:id database) {:stamp stamp :transport (engine/transport)}))))

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
          (respond {:cols (engine.arrow/reader->cols reader)}
                   (driver-api/reducible-rows (engine.arrow/row-thunk reader) canceled-chan))
          (finally
            (close!)))))))

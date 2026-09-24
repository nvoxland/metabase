(ns metabase.engine-bridge.core
  "The JDBC bridge: an Arrow Flight service, bound to loopback, that runs warehouse SQL over
  Metabase's own JDBC connection pools and streams the rows back as Arrow batches. The native query
  engine's `jdbc-bridge` connector is its only client. It exists for warehouses without a good Rust
  driver; the engine still plans and unparses, the bridge only executes.

  A `DoGet` ticket is JSON: `{token database_id sql params settings}` where `params` are the
  engine's `{type value}` parameter values and `settings` carries `time_zone`,
  `statement_timeout_ms`, `role`, and `read_only`. Failures come back as a gRPC status whose
  message is the engine's error JSON."
  (:require
   [clojure.core.async :as a]
   [clojure.string :as str]
   [metabase.driver :as driver]
   [metabase.driver.sql-jdbc.execute :as sql-jdbc.execute]
   [metabase.driver.util :as driver.u]
   [metabase.engine-bridge.db :as engine-bridge.db]
   [metabase.engine.settings :as engine.settings]
   [metabase.util :as u]
   [metabase.util.json :as json]
   [metabase.util.log :as log])
  (:import
   (java.math BigDecimal RoundingMode)
   (java.nio.charset StandardCharsets)
   (java.sql PreparedStatement ResultSet Statement)
   (java.time Instant LocalDate LocalDateTime LocalTime OffsetDateTime ZonedDateTime ZoneOffset)
   (org.apache.arrow.flight CallStatus FlightProducer$CallContext FlightProducer$ServerStreamListener FlightServer
                            Location NoOpFlightProducer Ticket)
   (org.apache.arrow.memory BufferAllocator RootAllocator)
   (org.apache.arrow.vector BigIntVector BitVector DateDayVector DecimalVector FieldVector Float8Vector
                            TimeMicroVector TimeStampMicroTZVector TimeStampMicroVector VarCharVector VectorSchemaRoot)
   (org.apache.arrow.vector.types DateUnit FloatingPointPrecision TimeUnit)
   (org.apache.arrow.vector.types.pojo ArrowType$Bool ArrowType$Date ArrowType$Decimal ArrowType$FloatingPoint
                                       ArrowType$Int ArrowType$Time ArrowType$Timestamp ArrowType$Utf8 Field FieldType)))

(set! *warn-on-reflection* true)

(def ^:private batch-rows 4096)

;;; ------------------------------------------------- parameters -------------------------------------------------------

(defn- param->jdbc
  "An engine parameter (`{:type arrow-type :value v}`) as the Java value the JDBC driver binds."
  [{:keys [type value]}]
  (when (some? value)
    ;; matched by prefix, so `Timestamp` must come before `Time`
    (condp #(str/starts-with? %2 %1) (str type)
      "Int8"      (long value)
      "Int16"     (long value)
      "Int32"     (long value)
      "Int64"     (long value)
      "Float"     (double value)
      "Decimal"   (BigDecimal. (str value))
      "Boolean"   (boolean value)
      "Date32"    (LocalDate/parse (str value))
      "Timestamp" (if (str/includes? (str type) "Some(")
                    (.atOffset (Instant/parse (str value)) ZoneOffset/UTC)
                    (LocalDateTime/parse (str value)))
      "Time"      (LocalTime/parse (str value))
      (str value))))

;;; ---------------------------------------------- Arrow column writers ------------------------------------------------

(defn- arrow-type
  "The Arrow type a result column is streamed as, from the base type the JDBC driver reports."
  [{:keys [base_type]}]
  (cond
    (isa? base_type :type/Boolean)             (ArrowType$Bool.)
    (isa? base_type :type/Integer)             (ArrowType$Int. 64 true)
    (isa? base_type :type/Decimal)             (ArrowType$Decimal. 38 10 128)
    (isa? base_type :type/Float)               (ArrowType$FloatingPoint. FloatingPointPrecision/DOUBLE)
    (isa? base_type :type/DateTimeWithTZ)      (ArrowType$Timestamp. TimeUnit/MICROSECOND "UTC")
    (isa? base_type :type/DateTime)            (ArrowType$Timestamp. TimeUnit/MICROSECOND nil)
    (isa? base_type :type/Date)                (ArrowType$Date. DateUnit/DAY)
    (isa? base_type :type/Time)                (ArrowType$Time. TimeUnit/MICROSECOND 64)
    :else                                      (ArrowType$Utf8.)))

(defn- ->micros ^long [^Instant instant]
  (+ (* 1000000 (.getEpochSecond instant)) (quot (.getNano instant) 1000)))

(defn- ->instant ^Instant [v]
  (condp instance? v
    Instant        v
    OffsetDateTime (.toInstant ^OffsetDateTime v)
    ZonedDateTime  (.toInstant ^ZonedDateTime v)
    LocalDateTime  (.toInstant ^LocalDateTime v ZoneOffset/UTC)
    (.toInstant ^OffsetDateTime (OffsetDateTime/parse (str v)))))

(defn- ->local-date-time ^LocalDateTime [v]
  (condp instance? v
    LocalDateTime  v
    OffsetDateTime (.toLocalDateTime ^OffsetDateTime v)
    ZonedDateTime  (.toLocalDateTime ^ZonedDateTime v)
    Instant        (LocalDateTime/ofInstant ^Instant v ZoneOffset/UTC)
    LocalDate      (.atStartOfDay ^LocalDate v)
    (LocalDateTime/parse (str v))))

(defn- writer
  "A function `(f index value)` that writes one value into `vector` (nil writes a null)."
  [^FieldVector vector]
  (condp instance? vector
    BigIntVector           (fn [^long i v] (if (nil? v) (.setNull ^BigIntVector vector i) (.setSafe ^BigIntVector vector i (long v))))
    Float8Vector           (fn [^long i v] (if (nil? v) (.setNull ^Float8Vector vector i) (.setSafe ^Float8Vector vector i (double v))))
    DecimalVector          (fn [^long i v] (if (nil? v)
                                             (.setNull ^DecimalVector vector i)
                                             (.setSafe ^DecimalVector vector i ^BigDecimal (.setScale (bigdec v) 10 RoundingMode/HALF_UP))))
    BitVector              (fn [^long i v] (if (nil? v) (.setNull ^BitVector vector i) (.setSafe ^BitVector vector i (if v 1 0))))
    DateDayVector          (fn [^long i v] (if (nil? v)
                                             (.setNull ^DateDayVector vector i)
                                             (.setSafe ^DateDayVector vector i (int (.toEpochDay (if (instance? LocalDate v) ^LocalDate v (LocalDate/parse (str v))))))))
    TimeStampMicroTZVector (fn [^long i v] (if (nil? v) (.setNull ^TimeStampMicroTZVector vector i) (.setSafe ^TimeStampMicroTZVector vector i (->micros (->instant v)))))
    TimeStampMicroVector   (fn [^long i v] (if (nil? v)
                                             (.setNull ^TimeStampMicroVector vector i)
                                             (.setSafe ^TimeStampMicroVector vector i (->micros (.toInstant (->local-date-time v) ZoneOffset/UTC)))))
    TimeMicroVector        (fn [^long i v] (if (nil? v)
                                             (.setNull ^TimeMicroVector vector i)
                                             (.setSafe ^TimeMicroVector vector i (quot (.toNanoOfDay (if (instance? LocalTime v) ^LocalTime v (LocalTime/parse (str v)))) 1000))))
    (fn [^long i v] (if (nil? v)
                      (.setNull ^VarCharVector vector i)
                      (.setSafe ^VarCharVector vector i (.getBytes (str v) StandardCharsets/UTF_8))))))

(defn- root-for
  "A `VectorSchemaRoot` for the result columns."
  ^VectorSchemaRoot [^BufferAllocator allocator cols]
  (let [fields (mapv (fn [{col-name :name :as col}]
                       (Field. (str col-name) (FieldType/nullable (arrow-type col)) nil))
                     cols)]
    (VectorSchemaRoot/create (org.apache.arrow.vector.types.pojo.Schema. ^java.util.List fields) allocator)))

;;; ---------------------------------------------------- serving -------------------------------------------------------

(defn- error-json
  "The engine's error envelope for a failure on the bridge."
  [^Throwable e]
  (json/encode {:kind       "warehouse"
                :error_type (name (or (:type (ex-data e)) :db))
                :message    (or (ex-message e) (str (class e)))
                :warehouse  "jdbc-bridge"
                :retryable  false}))

(defn- stream-result-set!
  "Write `rs` to `listener` in batches until it is exhausted or the client cancels."
  [driver ^ResultSet rs ^BufferAllocator allocator ^FlightProducer$ServerStreamListener listener]
  (let [rsmeta   (.getMetaData rs)
        cols     (sql-jdbc.execute/column-metadata driver rsmeta)
        next-row (sql-jdbc.execute/row-thunk driver rs rsmeta)]
    (with-open [root (root-for allocator cols)]
      (let [writers (mapv writer (.getFieldVectors root))]
        (.start listener root)
        (loop []
          (when-not (.isCancelled listener)
            (.allocateNew root)
            (let [n (loop [i 0]
                      (if (< i batch-rows)
                        (if-let [row (next-row)]
                          (do (dorun (map-indexed (fn [c v] ((nth writers c) i v)) row))
                              (recur (inc i)))
                          i)
                        i))]
              (.setRowCount root n)
              (when (pos? n)
                (.putNext listener))
              (when (= n batch-rows)
                (recur)))))
        (.completed listener)))))

(defn- serve-ticket!
  [^BufferAllocator allocator ^FlightProducer$ServerStreamListener listener
   {:keys [database_id sql params settings]}]
  (let [database (engine-bridge.db/database database_id)
        _        (when-not database
                   (throw (ex-info (format "Database %s not found" database_id) {:type :invalid-query})))
        driver   (driver.u/database->driver database)
        canceled (a/promise-chan)]
    (sql-jdbc.execute/do-with-connection-with-options
     driver
     database
     {:session-timezone (:time_zone settings)}
     (fn [conn]
       ;; the engine decided the impersonation role for this statement; apply it the way the JDBC path does
       (when-let [role (:role settings)]
         (driver/set-role! driver conn role))
       (let [jdbc-params (mapv param->jdbc params)]
         (with-open [stmt (sql-jdbc.execute/statement-or-prepared-statement driver conn sql jdbc-params canceled)
                     rs   (do
                            (when-let [ms (:statement_timeout_ms settings)]
                              (.setQueryTimeout ^Statement stmt (int (max 1 (quot ms 1000)))))
                            (if (instance? PreparedStatement stmt)
                              (sql-jdbc.execute/execute-prepared-statement! driver stmt)
                              (sql-jdbc.execute/execute-statement! driver stmt sql)))]
           (stream-result-set! driver rs allocator listener)))))))

(defn- producer
  "The Flight producer: `DoGet` runs a ticket's SQL over JDBC."
  [^BufferAllocator allocator token]
  (proxy [NoOpFlightProducer] []
    (getStream [^FlightProducer$CallContext _context ^Ticket ticket ^FlightProducer$ServerStreamListener listener]
      (try
        (let [request (json/decode+kw (String. (.getBytes ticket) StandardCharsets/UTF_8))]
          (when-not (and token (= token (:token request)))
            (throw (ex-info "JDBC bridge token mismatch" {:type :invalid-query})))
          (serve-ticket! allocator listener request))
        (catch Throwable e
          (log/warn e "JDBC bridge statement failed")
          (.error listener (.toRuntimeException (.withDescription CallStatus/INTERNAL (error-json e)))))))))

(defonce ^:private server
  ;; {:server FlightServer :allocator RootAllocator :port int :token string}
  (atom nil))

(defn running?
  "Whether the bridge is listening."
  []
  (some? @server))

(defn url
  "Where the bridge listens, or nil."
  []
  (some->> @server :port (str "http://127.0.0.1:")))

(defn start!
  "Start the bridge on loopback. `port` 0 picks a free port; the token is generated when not given.
  Returns `{:url :token}`."
  ([] (start! (engine.settings/engine-bridge-port) nil))
  ([port token]
   (or (some-> @server (select-keys [:url :token]))
       (let [token     (or token (str (random-uuid)))
             allocator (RootAllocator.)
             flight    (.build (FlightServer/builder allocator (Location/forGrpcInsecure "127.0.0.1" (int port)) (producer allocator token)))]
         (.start flight)
         (let [state {:server flight :allocator allocator :port (.getPort flight) :token token
                      :url    (str "http://127.0.0.1:" (.getPort flight))}]
           (reset! server state)
           (engine.settings/bridge-state! (select-keys state [:url :token]))
           (log/infof "JDBC bridge for the native query engine listening on %s" (:url state))
           (select-keys state [:url :token]))))))

(defn stop!
  "Stop the bridge if it is running."
  []
  (when-let [{^FlightServer flight :server, ^RootAllocator allocator :allocator} @server]
    (reset! server nil)
    (engine.settings/bridge-state! nil)
    (u/ignore-exceptions (.close flight))
    (u/ignore-exceptions (.close allocator))
    nil))

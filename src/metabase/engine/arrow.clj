(ns metabase.engine.arrow
  "Arrow record batches from the engine -> the column metadata and rows the query processor expects.
  Values become the same Java types the JDBC drivers produce (java.time for temporal columns,
  BigDecimal for decimals), so everything downstream is unchanged.

  Batches arrive either through an `ArrowReader` (the in-process C Data Interface stream) or a
  `FlightStream` (the sidecar); [[Batches]] hides the difference."
  (:import
   (java.nio.charset StandardCharsets)
   (java.time Instant LocalDate LocalDateTime LocalTime ZoneOffset)
   (org.apache.arrow.flight FlightStream)
   (org.apache.arrow.vector BigIntVector BitVector DateDayVector DecimalVector FieldVector Float4Vector Float8Vector
                            IntVector SmallIntVector TimeMicroVector TimeMilliVector TimeNanoVector TimeSecVector
                            TimeStampVector TimeStampMicroTZVector TimeStampMicroVector TimeStampMilliTZVector
                            TimeStampMilliVector TimeStampNanoTZVector TimeStampNanoVector TimeStampSecTZVector
                            TimeStampSecVector TinyIntVector UInt4Vector UInt8Vector VarBinaryVector VarCharVector VectorSchemaRoot)
   (org.apache.arrow.vector.ipc ArrowReader)
   (org.apache.arrow.vector.types.pojo ArrowType ArrowType$Binary ArrowType$Bool ArrowType$Date ArrowType$Decimal
                                       ArrowType$FloatingPoint ArrowType$Int ArrowType$LargeUtf8 ArrowType$Time
                                       ArrowType$Timestamp ArrowType$Utf8 Field Schema)))

(set! *warn-on-reflection* true)

(defprotocol Batches
  "A source of Arrow batches that reuses one `VectorSchemaRoot`."
  (-schema ^Schema [this])
  (-root ^VectorSchemaRoot [this])
  (-load-next! [this] "Load the next batch into the root; false when exhausted."))

(extend-protocol Batches
  ArrowReader
  (-schema [reader] (.getSchema (.getVectorSchemaRoot reader)))
  (-root [reader] (.getVectorSchemaRoot reader))
  (-load-next! [reader] (.loadNextBatch reader))

  FlightStream
  (-schema [stream] (.getSchema stream))
  (-root [stream] (.getRoot stream))
  (-load-next! [stream] (.next stream)))

(defn arrow-type->base-type
  "The Metabase base type for an Arrow type."
  [^ArrowType t]
  (condp instance? t
    ArrowType$Int           (if (>= (.getBitWidth ^ArrowType$Int t) 64) :type/BigInteger :type/Integer)
    ArrowType$FloatingPoint :type/Float
    ArrowType$Decimal       :type/Decimal
    ArrowType$Utf8          :type/Text
    ArrowType$LargeUtf8     :type/Text
    ArrowType$Bool          :type/Boolean
    ArrowType$Date          :type/Date
    ArrowType$Timestamp     (if (.getTimezone ^ArrowType$Timestamp t) :type/DateTimeWithLocalTZ :type/DateTime)
    ArrowType$Time          :type/Time
    ArrowType$Binary        :type/*
    :type/*))

(defn field->column
  "Result column metadata for an Arrow field. The warehouse's own type name, when the catalog
  tagged the field with it, wins for types Arrow can't carry (`uuid`, `timetz`, `bytea`), so the
  column reports the base type the JDBC driver would."
  [^Field field]
  (let [db-type (some-> field .getMetadata (.get "metabase.db_type"))]
    {:name          (.getName field)
     :base_type     (case db-type
                      "uuid"   :type/UUID
                      "timetz" :type/TimeWithTZ
                      "bytea"  :type/*
                      (arrow-type->base-type (.getType field)))
     ;; the Arrow type for every column, tagged or not, so a bucketed column and its source column compare equal
     :database_type (str (.getType field))}))

(defn cols
  "Result column metadata for every column `batches` will produce."
  [batches]
  (mapv field->column (.getFields (-schema batches))))

(defn- strip-zeros ^java.math.BigDecimal [^java.math.BigDecimal d]
  (let [stripped (.stripTrailingZeros d)]
    (if (neg? (.scale stripped))
      (.setScale stripped 0)
      stripped)))

(defn- db-type
  "The warehouse's own type name for a column, when the catalog snapshot carried it (see
  `DB_TYPE_METADATA` in the engine's catalog)."
  [^FieldVector vector]
  (some-> vector .getField .getMetadata (.get "metabase.db_type")))

(defn- hex-text
  "`\\x` followed by the bytes in lowercase hex: how Postgres prints `bytea`."
  ^String [^bytes bs]
  (let [sb (StringBuilder. "\\x")]
    (doseq [b bs]
      (.append sb (format "%02x" (bit-and 0xff (long b)))))
    (str sb)))

(defn- parse-offset-time
  "An `OffsetTime` from Postgres' text form of `timetz`, whose offset omits the minutes when they
  are zero (`+00`, `-07`, `+05:30`)."
  ^java.time.OffsetTime [^String s]
  (java.time.OffsetTime/parse (if (re-find #"[+-]\d\d$" s) (str s ":00") s)))

(defn- timestamp-fn
  "Accessor for any timestamp vector: `nanos-per-unit` scales the stored integer to nanoseconds; a
  zoned column (Arrow carries the zone, always UTC from the engine) becomes an `OffsetDateTime`,
  an unzoned one a `LocalDateTime`, matching what the JDBC drivers return."
  [^TimeStampVector vector ^long nanos-per-unit zoned?]
  (fn [i]
    (when-not (.isNull vector (int i))
      (let [nanos   (* nanos-per-unit (.get vector (int i)))
            instant (Instant/ofEpochSecond (Math/floorDiv nanos 1000000000) (Math/floorMod nanos 1000000000))]
        (if zoned?
          (.atOffset instant ZoneOffset/UTC)
          (LocalDateTime/ofInstant instant ZoneOffset/UTC))))))

(defn- value-fn
  "A function from row index to the Clojure value of `vector` at that index, or nil for nulls."
  [^FieldVector vector]
  (let [null? (fn [^long i] (.isNull vector (int i)))]
    (condp instance? vector
      BigIntVector          (fn [i] (when-not (null? i) (.get ^BigIntVector vector (int i))))
      IntVector             (fn [i] (when-not (null? i) (.get ^IntVector vector (int i))))
      SmallIntVector        (fn [i] (when-not (null? i) (.get ^SmallIntVector vector (int i))))
      TinyIntVector         (fn [i] (when-not (null? i) (.get ^TinyIntVector vector (int i))))
      UInt4Vector           (fn [i] (when-not (null? i) (.getValueAsLong ^UInt4Vector vector (int i))))
      ;; a MySQL `bigint unsigned`: a BigInteger past Long/MAX_VALUE, as the JDBC driver reads it
      UInt8Vector           (fn [i] (when-not (null? i)
                                      (let [v (.getObjectNoOverflow ^UInt8Vector vector (int i))]
                                        (if (<= (.bitLength ^java.math.BigInteger v) 63) (.longValue ^java.math.BigInteger v) v))))
      Float8Vector          (fn [i] (when-not (null? i) (.get ^Float8Vector vector (int i))))
      Float4Vector          (fn [i] (when-not (null? i) (double (.get ^Float4Vector vector (int i)))))
      DecimalVector         (fn [i] (when-not (null? i) (strip-zeros (.getObject ^DecimalVector vector (int i)))))
      VarCharVector         (let [text (fn [^long i] (String. (.get ^VarCharVector vector (int i)) StandardCharsets/UTF_8))]
                              ;; the catalog tags columns with the warehouse's type name; restore what Arrow can't carry
                              (case (db-type vector)
                                "uuid"   (fn [i] (when-not (null? i) (java.util.UUID/fromString (text i))))
                                ;; Arrow has no time-with-zone; Postgres sends `07:23:18.331+00` as text
                                "timetz" (fn [i] (when-not (null? i) (parse-offset-time (text i))))
                                (fn [i] (when-not (null? i) (text i)))))
      BitVector             (fn [i] (when-not (null? i) (= 1 (.get ^BitVector vector (int i)))))
      DateDayVector         (fn [i] (when-not (null? i) (LocalDate/ofEpochDay (.get ^DateDayVector vector (int i)))))
      TimeStampSecTZVector   (timestamp-fn vector 1000000000 true)
      TimeStampMilliTZVector (timestamp-fn vector 1000000 true)
      TimeStampMicroTZVector (timestamp-fn vector 1000 true)
      TimeStampNanoTZVector  (timestamp-fn vector 1 true)
      TimeStampSecVector     (timestamp-fn vector 1000000000 false)
      TimeStampMilliVector   (timestamp-fn vector 1000000 false)
      TimeStampMicroVector   (timestamp-fn vector 1000 false)
      TimeStampNanoVector    (timestamp-fn vector 1 false)
      TimeSecVector          (fn [i] (when-not (null? i) (LocalTime/ofSecondOfDay (.get ^TimeSecVector vector (int i)))))
      TimeMilliVector        (fn [i] (when-not (null? i) (LocalTime/ofNanoOfDay (* 1000000 (long (.get ^TimeMilliVector vector (int i)))))))
      TimeMicroVector        (fn [i] (when-not (null? i) (LocalTime/ofNanoOfDay (* 1000 (.get ^TimeMicroVector vector (int i))))))
      TimeNanoVector         (fn [i] (when-not (null? i) (LocalTime/ofNanoOfDay (.get ^TimeNanoVector vector (int i)))))
      VarBinaryVector       (if (= "bytea" (db-type vector))
                              ;; the Postgres JDBC driver hands `bytea` back in Postgres' hex text form
                              (fn [i] (when-not (null? i) (hex-text (.get ^VarBinaryVector vector (int i)))))
                              (fn [i] (when-not (null? i) (.get ^VarBinaryVector vector (int i)))))
      (fn [i] (when-not (null? i)
                (let [v (.getObject vector (int i))]
                  (if (instance? org.apache.arrow.vector.util.Text v) (str v) v)))))))

(defn row-thunk
  "A function that returns the next row (a vector) each time it is called, and nil once `batches`
  is exhausted. Loads batches from the engine as needed."
  [batches]
  (let [accessors (volatile! nil)
        row-count (volatile! 0)
        index     (volatile! 0)
        load!     (fn []
                    (if (-load-next! batches)
                      (let [root (-root batches)]
                        (vreset! accessors (mapv value-fn (.getFieldVectors root)))
                        (vreset! row-count (.getRowCount root))
                        (vreset! index 0)
                        true)
                      false))]
    (fn next-row []
      (if (< ^long @index ^long @row-count)
        (let [i @index]
          (vswap! index inc)
          (mapv (fn [f] (f i)) @accessors))
        (when (load!)
          (next-row))))))

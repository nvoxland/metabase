(ns metabase.engine.arrow
  "Arrow record batches from the engine -> the column metadata and rows the query processor expects.
  Values become the same Java types the JDBC drivers produce (java.time for temporal columns,
  BigDecimal for decimals), so everything downstream is unchanged.

  Batches arrive either through an `ArrowReader` (the in-process C Data Interface stream) or a
  `FlightStream` (the sidecar); [[Batches]] hides the difference."
  (:import
   (java.nio.charset StandardCharsets)
   (java.time Instant LocalDate LocalTime ZoneOffset)
   (org.apache.arrow.flight FlightStream)
   (org.apache.arrow.vector BigIntVector BitVector DateDayVector DecimalVector FieldVector Float4Vector Float8Vector
                            IntVector SmallIntVector TimeMicroVector TimeStampMicroTZVector TimeStampMicroVector
                            TinyIntVector UInt4Vector VarBinaryVector VarCharVector VectorSchemaRoot)
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
  "Result column metadata for an Arrow field."
  [^Field field]
  {:name          (.getName field)
   :base_type     (arrow-type->base-type (.getType field))
   :database_type (str (.getType field))})

(defn cols
  "Result column metadata for every column `batches` will produce."
  [batches]
  (mapv field->column (.getFields (-schema batches))))

(defn- strip-zeros ^java.math.BigDecimal [^java.math.BigDecimal d]
  (let [stripped (.stripTrailingZeros d)]
    (if (neg? (.scale stripped))
      (.setScale stripped 0)
      stripped)))

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
      Float8Vector          (fn [i] (when-not (null? i) (.get ^Float8Vector vector (int i))))
      Float4Vector          (fn [i] (when-not (null? i) (double (.get ^Float4Vector vector (int i)))))
      DecimalVector         (fn [i] (when-not (null? i) (strip-zeros (.getObject ^DecimalVector vector (int i)))))
      VarCharVector         (fn [i] (when-not (null? i) (String. (.get ^VarCharVector vector (int i)) StandardCharsets/UTF_8)))
      BitVector             (fn [i] (when-not (null? i) (= 1 (.get ^BitVector vector (int i)))))
      DateDayVector         (fn [i] (when-not (null? i) (LocalDate/ofEpochDay (.get ^DateDayVector vector (int i)))))
      TimeStampMicroTZVector (fn [i] (when-not (null? i)
                                       (let [micros (.get ^TimeStampMicroTZVector vector (int i))]
                                         (.atOffset (Instant/ofEpochSecond (Math/floorDiv micros 1000000)
                                                                           (* 1000 (Math/floorMod micros 1000000)))
                                                    ZoneOffset/UTC))))
      TimeStampMicroVector  (fn [i] (when-not (null? i) (.getObject ^TimeStampMicroVector vector (int i))))
      TimeMicroVector       (fn [i] (when-not (null? i) (LocalTime/ofNanoOfDay (* 1000 (.get ^TimeMicroVector vector (int i))))))
      VarBinaryVector       (fn [i] (when-not (null? i) (.get ^VarBinaryVector vector (int i))))
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

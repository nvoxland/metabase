(ns metabase.engine.arrow-test
  (:require
   [clojure.test :refer :all]
   [metabase.engine.arrow :as engine.arrow])
  (:import
   (java.time LocalDateTime LocalTime OffsetDateTime ZoneOffset)
   (org.apache.arrow.memory RootAllocator)
   (org.apache.arrow.vector TimeNanoVector TimeStampMicroTZVector TimeStampMilliVector TimeStampNanoTZVector
                            TimeStampNanoVector VectorSchemaRoot)
   (org.apache.arrow.vector.types TimeUnit)
   (org.apache.arrow.vector.types.pojo ArrowType$Time ArrowType$Timestamp Field FieldType)))

(set! *warn-on-reflection* true)

(defn- one-batch
  "A [[engine.arrow/Batches]] over a single in-memory root."
  [^VectorSchemaRoot root]
  (let [loaded? (volatile! false)]
    (reify engine.arrow/Batches
      (-schema [_] (.getSchema root))
      (-root [_] root)
      (-load-next! [_] (when-not @loaded? (vreset! loaded? true))))))

(defn- field ^Field [name arrow-type]
  (Field. name (FieldType/nullable arrow-type) nil))

(deftest ^:parallel timestamp-units-test
  (testing "every Arrow timestamp unit becomes the java.time value the JDBC drivers would return"
    (with-open [allocator (RootAllocator.)
                nano-tz   (TimeStampNanoTZVector. (field "nano_tz" (ArrowType$Timestamp. TimeUnit/NANOSECOND "UTC")) allocator)
                nano      (TimeStampNanoVector. (field "nano" (ArrowType$Timestamp. TimeUnit/NANOSECOND nil)) allocator)
                micro-tz  (TimeStampMicroTZVector. (field "micro_tz" (ArrowType$Timestamp. TimeUnit/MICROSECOND "UTC")) allocator)
                milli     (TimeStampMilliVector. (field "milli" (ArrowType$Timestamp. TimeUnit/MILLISECOND nil)) allocator)
                time-nano (TimeNanoVector. (field "t" (ArrowType$Time. TimeUnit/NANOSECOND 64)) allocator)]
      (let [vectors [nano-tz nano micro-tz milli time-nano]
            ;; 2015-05-31T07:00:00Z, as the date-bucketing tests produce it
            epoch-s 1433055600]
        (doseq [^org.apache.arrow.vector.FieldVector v vectors] (.allocateNew v 2))
        (.setSafe nano-tz 0 (* epoch-s 1000000000))
        (.setSafe nano 0 (* epoch-s 1000000000))
        (.setSafe micro-tz 0 (* epoch-s 1000000))
        (.setSafe milli 0 (* epoch-s 1000))
        (.setSafe time-nano 0 (* 3723 1000000000))
        (doseq [^org.apache.arrow.vector.FieldVector v vectors] (.setNull v 1) (.setValueCount v 2))
        (with-open [root (VectorSchemaRoot. ^java.util.List (vec vectors))]
          (.setRowCount root 2)
          (let [next-row (engine.arrow/row-thunk (one-batch root))]
            (is (= [(OffsetDateTime/of 2015 5 31 7 0 0 0 ZoneOffset/UTC)
                    (LocalDateTime/of 2015 5 31 7 0 0)
                    (OffsetDateTime/of 2015 5 31 7 0 0 0 ZoneOffset/UTC)
                    (LocalDateTime/of 2015 5 31 7 0 0)
                    (LocalTime/of 1 2 3)]
                   (next-row)))
            (is (= [nil nil nil nil nil] (next-row)))
            (is (nil? (next-row)))))))))

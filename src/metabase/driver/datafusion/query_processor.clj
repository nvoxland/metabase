(ns metabase.driver.datafusion.query-processor
  "MBQL to DataFusion SQL. Started from the Postgres compiler, since DataFusion's SQL is
  Postgres-flavored, and changed where the two differ.

  One rule shapes every method here: the engine unparses DataFusion function names verbatim, so
  the SQL must be valid for DataFusion *and* for the warehouse it ends up on. Where Postgres's own
  compiler uses something DataFusion lacks (`age`, `make_time`, `convert_from`), this uses a form
  both accept. Per-warehouse differences beyond that are the engine dialect's job."
  (:require
   [honey.sql :as sql]
   [metabase.driver :as driver]
   [metabase.driver-api.core :as driver-api]
   [metabase.driver.common :as driver.common]
   [metabase.driver.sql.query-processor :as sql.qp]
   [metabase.driver.sql.query-processor.format :as sql.qp.format]
   [metabase.util.honey-sql-2 :as h2x]
   [metabase.util.malli :as mu]))

(set! *warn-on-reflection* true)

;;; --------------------------------------------------- formatting -------------------------------------------------------

(defmethod sql.qp.format/format-honeysql :datafusion
  [driver honeysql-form]
  ;; `:numbered` gives `$1`-style placeholders, which is what DataFusion binds. Parameters come back in order.
  (sql/format honeysql-form {:dialect      (sql.qp.format/quote-style driver)
                             :quoted       true
                             :quoted-snake false
                             :numbered     true
                             :inline       driver/*compile-with-inline-parameters*
                             :nested       (not (map? honeysql-form))}))

(defmethod sql.qp/use-ctes-for-stages? :datafusion
  [_driver]
  ;; Stage CTEs reuse the same names in nested scopes (each source query gets its own `__mb_stage_0`), which
  ;; Postgres allows and DataFusion rejects ("WITH query name specified more than once"). Nested subqueries plan fine.
  false)

(defmethod sql.qp/float-dbtype :datafusion
  [_driver]
  "DOUBLE PRECISION")

;;; ------------------------------------------------- temporal basics ----------------------------------------------------

(defmethod sql.qp/add-interval-honeysql-form :datafusion
  [_driver hsql-form amount unit]
  (h2x/add-interval-honeysql-form :postgres hsql-form amount unit))

(defmethod sql.qp/current-datetime-honeysql-form :datafusion
  [_driver]
  (h2x/current-datetime-honeysql-form :postgres))

(defmethod sql.qp/unix-timestamp->honeysql [:datafusion :seconds]
  [_driver _unit expr]
  ;; `to_timestamp(seconds)` means the same thing in DataFusion and Postgres.
  (h2x/with-database-type-info [:to_timestamp expr] "timestamptz"))

(defn- extract [unit expr]
  [::h2x/extract unit expr])

(defn- ->timestamp [expr]
  (h2x/->pg-timestamp expr))

(defn- two-digits
  "A time-of-day part as a two-character string, in both dialects."
  [part expr]
  [:lpad (h2x/cast "varchar" (h2x/->integer (extract part expr))) [:inline 2] (h2x/literal "0")])

(defn- time-trunc
  "Truncate a `time` column. Neither dialect has a `date_trunc` for times that both agree on (DataFusion has none;
  Postgres returns an interval), so the parts are reassembled as text and cast back."
  [unit expr]
  (let [hour   (two-digits :hour expr)
        minute (if (#{:minute :second} unit) (two-digits :minute expr) (h2x/literal "00"))
        second (if (= unit :second) (two-digits :second expr) (h2x/literal "00"))]
    (h2x/with-database-type-info
     (h2x/cast "time" [:concat hour (h2x/literal ":") minute (h2x/literal ":") second])
     "time")))

(mu/defn- date-trunc
  [unit :- driver-api/schema.temporal-bucketing.unit.date-time.truncate
   expr :- ::h2x/expr]
  (cond
    (h2x/database-or-effective-type-isa? expr "time" :type/Time)
    (time-trunc unit expr)

    ;; `date_trunc` returns a timestamp; keep date columns as dates.
    (h2x/database-or-effective-type-isa? expr "date" :type/Date)
    (h2x/cast "date" [:date_trunc (h2x/literal unit) expr])

    :else
    (let [expr' (->timestamp expr)]
      (cond-> [:date_trunc (h2x/literal unit) expr']
        (h2x/type-info expr') (h2x/with-type-info (h2x/type-info expr'))))))

(defn- extract-integer [unit expr]
  (h2x/->integer (extract unit (if (h2x/database-or-effective-type-isa? expr "time" :type/Time)
                                 expr
                                 (->timestamp expr)))))

(defmethod sql.qp/date [:datafusion :default]          [_ _ expr] expr)
(defmethod sql.qp/date [:datafusion :second-of-minute] [_ _ expr] (extract-integer :second expr))
(defmethod sql.qp/date [:datafusion :minute]           [_ _ expr] (date-trunc :minute expr))
(defmethod sql.qp/date [:datafusion :minute-of-hour]   [_ _ expr] (extract-integer :minute expr))
(defmethod sql.qp/date [:datafusion :hour]             [_ _ expr] (date-trunc :hour expr))
(defmethod sql.qp/date [:datafusion :hour-of-day]      [_ _ expr] (extract-integer :hour expr))
(defmethod sql.qp/date [:datafusion :day-of-month]     [_ _ expr] (extract-integer :day expr))
(defmethod sql.qp/date [:datafusion :day-of-year]      [_ _ expr] (extract-integer :doy expr))
(defmethod sql.qp/date [:datafusion :month]            [_ _ expr] (date-trunc :month expr))
(defmethod sql.qp/date [:datafusion :month-of-year]    [_ _ expr] (extract-integer :month expr))
(defmethod sql.qp/date [:datafusion :quarter]          [_ _ expr] (date-trunc :quarter expr))
(defmethod sql.qp/date [:datafusion :quarter-of-year]  [_ _ expr] (extract-integer :quarter expr))
(defmethod sql.qp/date [:datafusion :year]             [_ _ expr] (date-trunc :year expr))
(defmethod sql.qp/date [:datafusion :year-of-era]      [_ _ expr] (extract-integer :year expr))
(defmethod sql.qp/date [:datafusion :week-of-year-iso] [_ _ expr] (extract-integer :week expr))

(defmethod sql.qp/date [:datafusion :day]
  [_ _ expr]
  (h2x/maybe-cast (h2x/database-type expr) (h2x/->date expr)))

(defmethod sql.qp/date [:datafusion :day-of-week]
  [driver _unit expr]
  ;; `dow` is Sunday(0)..Saturday(6) in both engines.
  (sql.qp/adjust-day-of-week driver
                             (h2x/+ (extract-integer :dow expr) 1)
                             (driver.common/start-of-week-offset-for-day :sunday)))

(defmethod sql.qp/date [:datafusion :week]
  [driver _ expr]
  (sql.qp/adjust-start-of-week driver (partial date-trunc :week) expr))

;;; -------------------------------------------------- datetime-diff -----------------------------------------------------
;;;
;;; Postgres uses `age()`, which DataFusion doesn't have. These use only `extract` and arithmetic.

(defn- epoch-seconds [expr]
  (extract :epoch (->timestamp expr)))

(defmethod sql.qp/datetime-diff [:datafusion :second]
  [_driver _unit x y]
  (h2x/->integer [:trunc (h2x/- (epoch-seconds y) (epoch-seconds x))]))

(defmethod sql.qp/datetime-diff [:datafusion :minute]
  [driver _unit x y]
  (h2x// (sql.qp/datetime-diff driver :second x y) 60))

(defmethod sql.qp/datetime-diff [:datafusion :hour]
  [driver _unit x y]
  (h2x// (sql.qp/datetime-diff driver :second x y) 3600))

(defmethod sql.qp/datetime-diff [:datafusion :day]
  [_driver _unit x y]
  ;; Calendar days: midnight to midnight, so the time of day doesn't matter.
  (h2x// (h2x/->integer [:trunc (h2x/- (epoch-seconds (h2x/->date y)) (epoch-seconds (h2x/->date x)))])
         86400))

(defmethod sql.qp/datetime-diff [:datafusion :week]
  [driver _unit x y]
  (h2x// (sql.qp/datetime-diff driver :day x y) 7))

(defn- months-between
  "Whole months from `x` to `y`: the year and month difference, minus one when `y` hasn't reached `x`'s
  day of month and time yet (or plus one in the negative direction)."
  [x y]
  (let [x         (->timestamp x)
        y         (->timestamp y)
        raw       (h2x/+ (h2x/* (h2x/- (extract :year y) (extract :year x)) 12)
                         (h2x/- (extract :month y) (extract :month x)))
        ;; seconds into the month, so day-of-month and time compare as one number
        into-month (fn [expr]
                     (h2x/- (extract :epoch expr) (extract :epoch [:date_trunc (h2x/literal :month) expr])))]
    (h2x/->integer
     [:case
      [:and [:> raw [:inline 0]] [:< (into-month y) (into-month x)]] (h2x/- raw [:inline 1])
      [:and [:< raw [:inline 0]] [:> (into-month y) (into-month x)]] (h2x/+ raw [:inline 1])
      :else raw])))

(defmethod sql.qp/datetime-diff [:datafusion :month]
  [_driver _unit x y]
  (months-between x y))

(defmethod sql.qp/datetime-diff [:datafusion :quarter]
  [driver _unit x y]
  (h2x// (sql.qp/datetime-diff driver :month x y) 3))

(defmethod sql.qp/datetime-diff [:datafusion :year]
  [driver _unit x y]
  (h2x// (sql.qp/datetime-diff driver :month x y) 12))

;;; ---------------------------------------------------- expressions -----------------------------------------------------

(defn- format-regex-match-first [_fn [identifier pattern]]
  ;; `regexp_match` returns an array in both engines; `[1]` is the first capture group.
  (let [[identifier-sql & identifier-args] (sql/format-expr identifier {:nested true})
        [pattern-sql & pattern-args]       (sql/format-expr pattern {:nested true})]
    (into [(format "regexp_match(%s, %s)[1]" identifier-sql pattern-sql)]
          cat
          [identifier-args pattern-args])))

(sql/register-fn! ::regex-match-first #'format-regex-match-first)

(defmethod sql.qp/->honeysql [:datafusion :regex-match-first]
  [driver [_ _opts arg pattern]]
  [::regex-match-first (sql.qp/->honeysql driver arg) (sql.qp/->honeysql driver pattern)])

(defmethod sql.qp/->honeysql [:datafusion :split-part]
  [driver [_ _opts text divider position]]
  (let [position (sql.qp/->honeysql driver position)]
    [:case
     [:< position 1]
     ""
     :else
     [:split_part (sql.qp/->honeysql driver text) (sql.qp/->honeysql driver divider) position]]))

(defmethod sql.qp/->honeysql [:datafusion :text]
  [driver [_ _opts value]]
  (h2x/maybe-cast "TEXT" (sql.qp/->honeysql driver value)))

(defmethod sql.qp/cast-temporal-string [:datafusion :Coercion/YYYYMMDDHHMMSSString->Temporal]
  [_driver _coercion-strategy expr]
  ;; DataFusion uses Chrono format strings; the Rust Postgres dialect translates them to `to_timestamp` patterns.
  (h2x/with-database-type-info [:to_timestamp expr (h2x/literal "%Y%m%d%H%M%S")] "timestamp"))

;;; ISO 8601 strings cast directly in both dialects.
(defmethod sql.qp/cast-temporal-string [:datafusion :Coercion/ISO8601->DateTime]
  [_driver _coercion-strategy expr]
  (h2x/cast "timestamp" expr))

(defmethod sql.qp/cast-temporal-string [:datafusion :Coercion/ISO8601->Date]
  [_driver _coercion-strategy expr]
  (h2x/cast "date" expr))

(defmethod sql.qp/cast-temporal-string [:datafusion :Coercion/ISO8601->Time]
  [_driver _coercion-strategy expr]
  (h2x/cast "time" expr))

;;; DataFusion's one-argument `log` is base 10, like Postgres'. The two-argument form Postgres only has for
;;; `numeric`, which a double column can't satisfy.
(defmethod sql.qp/->honeysql [:datafusion :log]
  [driver [_ _opts arg]]
  [:log (sql.qp/->honeysql driver arg)])

;;; DataFusion has `median`, but Postgres doesn't; both have `percentile_cont(p) WITHIN GROUP (ORDER BY x)`, which
;;; DataFusion plans as an ordered-set aggregate and unparses in that form.
(defmethod sql.qp/->honeysql [:datafusion :median]
  [driver [_ _opts arg]]
  (sql.qp/->honeysql driver [:percentile {} arg 0.5]))

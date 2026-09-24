(ns metabase.engine.catalog
  "Builds the catalog snapshot the engine plans against from Metabase's own table and field
  metadata. Names are passed exactly as stored, since the engine resolves identifiers case-sensitively
  and the compiler quotes everything."
  (:require
   [metabase.engine.core :as engine]
   [metabase.engine.db :as engine.db]
   [metabase.util :as u]))

(set! *warn-on-reflection* true)

(def ^:private timestamp-utc "Timestamp(Microsecond, Some(\"UTC\"))")

(defn base-type->arrow-type
  "The Arrow type the engine should assume for a column with this Metabase base type. Coarse on purpose:
  the engine casts warehouse results to the plan's schema, and precision for decimals comes later."
  [base-type]
  (condp #(isa? %2 %1) base-type
    :type/Boolean        "Boolean"
    :type/BigInteger     "Int64"
    :type/Integer        "Int64"
    :type/Decimal        "Decimal128(38, 10)"
    :type/Float          "Float64"
    :type/Number         "Float64"
    :type/DateTimeWithTZ timestamp-utc
    :type/DateTime       "Timestamp(Microsecond, None)"
    :type/Date           "Date32"
    :type/TimeWithTZ     "Utf8"
    :type/Time           "Time64(Microsecond)"
    "Utf8"))

(defn- db-type->arrow-type
  "A more precise Arrow type from the warehouse's own type name, when the name is unambiguous.
  Keeps int4 columns as Int32 and real columns as Float32, the way JDBC returns them."
  [database-type]
  (case (some-> database-type u/lower-case-en)
    ("int2" "smallint" "smallserial")                     "Int16"
    ("int4" "integer" "int" "serial")                     "Int32"
    ("int8" "bigint" "bigserial")                         "Int64"
    ("float4" "real")                                     "Float32"
    ("float8" "double precision" "double")                "Float64"
    nil))

(defn- field->column [{:keys [name base_type database_type database_required]}]
  {:name     name
   :type     (or (db-type->arrow-type database_type) (base-type->arrow-type base_type))
   :nullable (not database_required)
   :db_type  database_type})

(defn- catalog-version [tables fields]
  (let [latest (->> (concat tables fields) (keep :updated_at) sort last)]
    (str (count tables) "/" (count fields) "@" latest)))

(defn snapshot
  "The engine catalog snapshot for a database: active tables and their active, top-level fields."
  [database]
  (let [tables   (engine.db/active-tables (:id database))
        fields   (engine.db/active-top-level-fields (map :id tables))
        by-table (group-by :table_id fields)]
    {:database_id     (:id database)
     :catalog_version (catalog-version tables fields)
     :connector       (name (:engine database))
     :default_schema  (some :schema tables)
     :tables          (for [table tables]
                        {:schema  (:schema table)
                         :name    (:name table)
                         :columns (mapv field->column (get by-table (:id table)))})}))

(defn stamp
  "A cheap value that changes whenever the database's table or field metadata changes, so callers can
  skip building a snapshot when nothing did."
  [database]
  (engine.db/catalog-stamp (:id database)))

(defn register!
  "Register `database`'s catalog with the engine. Returns the engine's acknowledgement."
  [database]
  (engine/register-catalog! (snapshot database)))

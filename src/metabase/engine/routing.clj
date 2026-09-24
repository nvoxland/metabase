(ns metabase.engine.routing
  "Decides whether a query runs through the native query engine. The database keeps its real engine
  (`:postgres`); only the driver the query processor binds for compiling and executing changes."
  (:require
   [metabase.engine.settings :as engine.settings]
   [metabase.settings.core :as setting]))

(def ^:private engine-warehouses
  "Warehouses the engine has a dialect and a connector for."
  #{:postgres})

(defn- native-query?
  "Whether a legacy or MBQL 5 query has a native stage. Native SQL is the warehouse's own dialect,
  which the engine can't plan, so it stays on the driver."
  [query]
  (or (= (keyword (:type query)) :native)
      (some #(= (:lib/type %) :mbql.stage/native) (:stages query))))

(defn engine-mode
  "The `query-engine` setting for `database`: `:jdbc`, `:datafusion`, or `:shadow`."
  [database]
  (setting/with-database database
    (engine.settings/query-engine)))

(defn effective-driver
  "The driver to bind for `query`. `candidate` is what the query processor would bind on its own;
  `warehouse-driver` is the database's engine; `database` is the database (with its settings).
  Returns `:datafusion` when the database is routed to the engine and the query can run there,
  otherwise `candidate`."
  [query database candidate warehouse-driver]
  (if (and (= candidate warehouse-driver)
           (contains? engine-warehouses warehouse-driver)
           (not (native-query? query))
           (contains? #{:datafusion :shadow} (engine-mode database)))
    :datafusion
    candidate))

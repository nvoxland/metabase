(ns metabase.engine.routing
  "Decides whether a query runs through the native query engine. The database keeps its real engine
  (`:postgres`); only the driver the query processor binds for compiling and executing changes."
  (:require
   [clojure.walk :as walk]
   [metabase.engine.settings :as engine.settings]
   [metabase.lib.metadata :as lib.metadata]
   [metabase.settings.core :as setting]))

(def ^:private engine-warehouses
  "Warehouses the engine has a dialect and a connector for."
  #{:postgres})

(defn- source-card-ids
  "Every card the query draws from, in any stage or join (`:source-card` in MBQL 5, `card__N` legacy)."
  [query]
  (let [ids (volatile! #{})]
    (walk/postwalk (fn [x]
                     (when (map? x)
                       (when-let [id (:source-card x)]
                         (vswap! ids conj id))
                       (when-let [[_ id] (and (string? (:source-table x))
                                              (re-matches #"card__(\d+)" (:source-table x)))]
                         (vswap! ids conj (parse-long id))))
                     x)
                   query)
    @ids))

(defn- native-card?
  "Whether a card's own query is native, or is built on a native card."
  [metadata-provider card-id seen]
  (when-not (contains? seen card-id)
    (when-let [card (lib.metadata/card metadata-provider card-id)]
      (let [query (:dataset-query card)]
        (or (= (keyword (:type query)) :native)
            (some #(= (:lib/type %) :mbql.stage/native) (:stages query))
            (some #(native-card? metadata-provider % (conj seen card-id)) (source-card-ids query)))))))

(defn- native-query?
  "Whether a legacy or MBQL 5 query has a native stage, directly or through a source card. Native
  SQL is the warehouse's own dialect, which the engine can't plan, so it stays on the driver."
  [query metadata-provider]
  (boolean
   (or (= (keyword (:type query)) :native)
       (some #(= (:lib/type %) :mbql.stage/native) (:stages query))
       (some #(native-card? metadata-provider % #{}) (source-card-ids query)))))

(defn engine-mode
  "The `query-engine` setting for `database`: `:jdbc`, `:datafusion`, or `:shadow`."
  [database]
  (setting/with-database database
    (engine.settings/query-engine)))

(def ^:dynamic *force*
  "When bound to `:jdbc` or `:datafusion`, overrides the database's `query-engine` setting. Shadow
  mode uses it to run the same query through both paths."
  nil)

(defn- routable?
  "Whether `query` against `warehouse-driver` could run through the engine at all."
  [query metadata-provider warehouse-driver]
  (and (contains? engine-warehouses warehouse-driver)
       (not (native-query? query metadata-provider))))

(defn shadow?
  "Whether `query` should also be run through both paths in the background and compared: the
  database is in shadow mode, nothing forced a path, and the engine could run the query."
  [query metadata-provider database warehouse-driver]
  (and (nil? *force*)
       (= :shadow (engine-mode database))
       (routable? query metadata-provider warehouse-driver)))

(defn effective-driver
  "The driver to bind for `query`. `candidate` is what the query processor would bind on its own;
  `warehouse-driver` is the database's engine; `database` is the database (with its settings).
  Returns `:datafusion` when the database is routed to the engine and the query can run there,
  otherwise `candidate`."
  [query metadata-provider database candidate warehouse-driver]
  (let [mode (or *force* (engine-mode database))]
    ;; In shadow mode the driver answers the query; the engine run happens in the background with `*force*` bound.
    (if (and (= candidate warehouse-driver)
             (= mode :datafusion)
             (routable? query metadata-provider warehouse-driver))
      :datafusion
      candidate)))

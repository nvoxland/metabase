(ns metabase.engine.routing
  "Decides whether a query runs through the native query engine. The database keeps its real engine
  (`:postgres`); only the driver the query processor binds for compiling and executing changes."
  (:require
   [clojure.string :as str]
   [clojure.walk :as walk]
   [metabase.engine.core :as engine]
   [metabase.engine.settings :as engine.settings]
   [metabase.lib.metadata :as lib.metadata]
   [metabase.settings.core :as setting]))

(def ^:private dialect-warehouses
  "Warehouses the engine has a dialect for. A warehouse needs one whether it is reached through a
  Rust connector or the JDBC bridge; `engine-bridge-warehouses` only changes how one of these is
  reached, it can't add a warehouse the engine can't unparse for. `:mysql` covers MariaDB, which
  Metabase reaches through the same driver."
  #{:postgres :mysql})

(defn engine-warehouses
  "Warehouses the engine can run queries against."
  []
  dialect-warehouses)

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
  "Whether a legacy or MBQL 5 query has a native stage anywhere (top level, a nested source query,
  a join's source, or a source card), directly or through a source card. Native SQL is the
  warehouse's own dialect, which the engine can't plan, so it stays on the driver."
  [query metadata-provider]
  (boolean
   (or (= (keyword (:type query)) :native)
       (some (fn [x]
               (and (map? x)
                    (or (contains? x :native)
                        (= (:lib/type x) :mbql.stage/native))))
             (tree-seq coll? seq query))
       (some #(native-card? metadata-provider % #{}) (source-card-ids query)))))

(defn- field-ids
  "Every field id referenced by `:field` clauses anywhere in a legacy or MBQL 5 query."
  [query]
  (let [ids (volatile! #{})]
    ;; legacy `[:field id opts]` and MBQL 5 `[:field opts id]`
    (walk/postwalk (fn [x]
                     (when (and (vector? x) (= :field (first x)))
                       (when-let [id (first (filter integer? (rest x)))]
                         (vswap! ids conj id)))
                     x)
                   query)
    @ids))

(defn- source-table-ids
  "Every table the query reads directly, in any stage or join."
  [query]
  (let [ids (volatile! #{})]
    (walk/postwalk (fn [x]
                     (when (and (map? x) (integer? (:source-table x)))
                       (vswap! ids conj (:source-table x)))
                     x)
                   query)
    @ids))

(defn- nested-field-column-query?
  "Whether the query touches a nested-field column (a JSON path Metabase unfolded at sync time into
  its own Field): named in a `:field` clause, or present on a table the query reads whole (the
  query processor adds every field of a bare source table). The engine compiler has no JSON path
  support yet, so those stay on JDBC."
  [query metadata-provider]
  (boolean
   (or (some (fn [id]
               (some-> (lib.metadata/field metadata-provider id) :nfc-path seq))
             (field-ids query))
       (some (fn [table-id]
               (some #(seq (:nfc-path %)) (lib.metadata/fields metadata-provider table-id)))
             (source-table-ids query)))))

(def ^:private unsupported-detail-keys
  "Connection details the Rust connectors don't implement yet. A database using any of them stays
  on JDBC; the engine's connector would otherwise silently connect without them."
  [:ssl-root-cert :ssl-client-cert :ssl-key :ssl-key-password :ssl-cert :additional-options :auth-provider])

(defn details-supported?
  "Whether the engine's connector can honor every part of a database's connection details:
  no SSH tunnel, no custom certificates, no JDBC-only options."
  [details]
  (and (not (:tunnel-enabled details))
       (not (:ssl-use-client-auth details))
       (not (:use-auth-provider details))
       (every? (fn [k]
                 (let [v (get details k)]
                   (or (nil? v) (and (string? v) (str/blank? v)))))
               unsupported-detail-keys)))

(defn engine-mode
  "The `query-engine` setting for `database`: `:jdbc`, `:datafusion`, or `:shadow`."
  [database]
  (setting/with-database database
    (engine.settings/query-engine)))

(def ^:dynamic *standalone-compile*
  "Bound while a query is compiled without being executed (`qp.compile/compile`, the native
  endpoint, 'convert to SQL'). The result is meant to be run as native SQL by the warehouse
  driver, so it must be in the warehouse's dialect: the engine is not routed to for it."
  false)

(def ^:dynamic *reported-driver*
  "The warehouse's own driver while a query runs through the engine driver, for metrics and logs
  that report per driver. Bound by the query processor's setup when it routes a query."
  nil)

(def ^:dynamic *force*
  "When bound to `:jdbc` or `:datafusion`, overrides the database's `query-engine` setting. Shadow
  mode uses it to run the same query through both paths."
  nil)

(defn- routable?
  "Whether `query` against `warehouse-driver` could run through the engine at all."
  [query metadata-provider database warehouse-driver]
  (and (contains? (engine-warehouses) warehouse-driver)
       ;; a database routed to an engine this process can't start keeps working on its driver
       (engine/available?)
       ;; likewise one reached through the JDBC bridge while the bridge isn't listening
       (or (not (contains? (engine.settings/bridge-warehouses) warehouse-driver))
           (some? (engine.settings/bridge-url)))
       (details-supported? (:details database))
       (not (native-query? query metadata-provider))
       (not (nested-field-column-query? query metadata-provider))))

(defn shadow?
  "Whether `query` should also be run through both paths in the background and compared: the
  database is in shadow mode, nothing forced a path, and the engine could run the query."
  [query metadata-provider database warehouse-driver]
  (and (nil? *force*)
       (= :shadow (engine-mode database))
       (routable? query metadata-provider database warehouse-driver)))

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
             (not *standalone-compile*)
             (routable? query metadata-provider database warehouse-driver))
      :datafusion
      candidate)))

(defn sync-driver
  "The driver sync should use for `database`: `:datafusion` when the database is routed to the
  engine's own connector (not in shadow mode, which keeps the driver as the source of truth), else
  its own driver. A database reached through the JDBC bridge syncs through its own driver: the
  bridge is that driver's pool, so that already is the single connection stack."
  [database warehouse-driver]
  (if (and (contains? (engine-warehouses) warehouse-driver)
           (not (contains? (engine.settings/bridge-warehouses) warehouse-driver))
           (engine/available?)
           (details-supported? (:details database))
           (= :datafusion (engine-mode database)))
    :datafusion
    warehouse-driver))

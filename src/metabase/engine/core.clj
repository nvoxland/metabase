(ns metabase.engine.core
  "Public API of the native query engine module. See dev/notes/datafusion-engine.md.

  Requests are maps that serialize to the engine's JSON envelope (`{:op \"...\" ...}`) and results
  come back as the `result` of an `ok` envelope. Engine errors are thrown as `ex-info` whose
  `:type` is the matching `metabase.query-processor.error-type` keyword."
  (:require
   [metabase.engine.arrow :as engine.arrow]
   [metabase.engine.client :as engine.client]
   [metabase.engine.ffi :as engine.ffi]
   [metabase.engine.protocol :as protocol]
   [metabase.engine.settings :as engine.settings]
   [metabase.util.i18n :refer [tru]]
   [metabase.util.json :as json]
   [metabase.util.log :as log]))

(set! *warn-on-reflection* true)

(defonce ^:private transport* (atom nil))

(defn- make-transport []
  (case (engine.settings/engine-transport)
    :ffi     (engine.ffi/transport (engine.settings/engine-native-library-path) nil)
    :sidecar (engine.client/transport (or (engine.settings/engine-sidecar-url)
                                          (throw (ex-info (tru "engine-sidecar-url is not set.") {}))))))

(defn transport
  "The active transport, started on first use."
  []
  (or @transport*
      (locking transport*
        (or @transport*
            (let [t (make-transport)]
              (log/infof "Native query engine started over %s" (name (engine.settings/engine-transport)))
              (reset! transport* t))))))

(defn stop!
  "Shut the engine down. The next call starts it again."
  []
  (locking transport*
    (when-let [t @transport*]
      (protocol/-close t)
      (reset! transport* nil))))

(defn call!
  "Send `request` (a map with an `:op`) to the engine and return its result, throwing on an `err`
  envelope."
  [request]
  (let [response (json/decode+kw (protocol/-call (transport) (json/encode request)))]
    (case (:status response)
      "ok"  (:result response)
      "err" (throw (protocol/engine-error->exception (:error response)))
      (throw (ex-info (tru "Unexpected response from the query engine.") {:response response})))))

(defn healthy?
  "Whether the engine can be reached."
  []
  (try
    (boolean (:version (call! {:op "version"})))
    (catch Throwable e
      (log/debug e "Native query engine is not available")
      false)))

(defn version
  "The engine's version string."
  []
  (:version (call! {:op "version"})))

(defn register-catalog!
  "Give the engine a database's tables and columns. `snapshot` matches the engine's
  `CatalogSnapshot`: `{:database_id :catalog_version :connector :default_schema :tables [...]}`."
  [snapshot]
  (call! {:op "register_catalog" :snapshot snapshot}))

(defn register-database!
  "Give the engine a database's connection details and pool settings."
  [registration]
  (call! {:op "register_database" :registration registration}))

(defn invalidate-database!
  "Drop the engine's catalog and pools for a database, e.g. after its details change."
  [database-id]
  (call! {:op "invalidate_database" :database_id database-id}))

(defonce ^:private registrations
  ;; database id -> whatever the registering code uses to decide the engine is current for that database (the
  ;; driver keys on a catalog stamp, a details hash, and the transport). Kept here so an event (a deleted
  ;; database) can forget it without the engine module knowing about the driver.
  (atom {}))

(defn registration
  "What was recorded for `database-id` by [[remember-registration!]], or nil."
  [database-id]
  (get @registrations database-id))

(defn remember-registration!
  "Record that `database-id` is registered with the engine under `stamp`."
  [database-id stamp]
  (swap! registrations assoc database-id stamp))

(defn forget-database!
  "Drop what the engine knows about a database: its registration memo and its pools and catalog."
  [database-id]
  (swap! registrations dissoc database-id)
  (invalidate-database! database-id))

(defn create-session!
  "Open a session for `context` (`{:database_id ...}` plus user, timezone, remark, ...). Returns the
  session id; close it with [[destroy-session!]]."
  [context]
  (:session_id (call! {:op "create_session" :context context})))

(defn destroy-session!
  "Close a session opened with [[create-session!]]."
  [session-id]
  (call! {:op "destroy_session" :session_id session-id}))

(defmacro with-session
  "Evaluate `body` with `binding` bound to a session for `context`, destroying it afterwards."
  [[binding context] & body]
  `(let [~binding (create-session! ~context)]
     (try
       ~@body
       (finally
         (destroy-session! ~binding)))))

(defn plan-and-unparse
  "Plan engine-dialect `sql` in `session-id` and return the warehouse SQL with its schema:
  `{:warehouse_sql :warehouse :schema [{:name :type :nullable}] :warnings}`."
  ([session-id sql]
   (plan-and-unparse session-id sql []))
  ([session-id sql params]
   (call! {:op "plan_and_unparse" :session_id session-id :sql sql :params params})))

(defn execute!
  "Plan and start engine-dialect `sql` in `session-id`. Returns `{:reader ArrowReader, :cancel! fn,
  :close! fn}`; call `:close!` exactly once, which also stops the statement if it is still running.
  `params` are `{:type <Arrow type> :value <JSON value>}` maps bound to `$1`, `$2`, ..."
  [session-id sql params]
  (protocol/-execute (transport) session-id sql params))

(defn execute-raw!
  "Run warehouse-dialect `sql` in `session-id`'s database without planning it. For Metabase's own
  metadata queries; never for SQL a person wrote. Same return shape as [[execute!]]."
  [session-id sql params]
  (protocol/-execute-raw (transport) session-id sql params))

(defn query-rows!
  "Run a small internal statement through [[execute-raw!]] and realize every row. Returns
  `{:cols [...] :rows [[...] ...]}`."
  [session-id sql params]
  (let [{:keys [reader close!]} (execute-raw! session-id sql params)]
    (try
      (let [next-row (engine.arrow/row-thunk reader)]
        {:cols (engine.arrow/cols reader)
         :rows (into [] (take-while some?) (repeatedly next-row))})
      (finally
        (close!)))))

(defn pool-stats
  "Connection pool statistics, for all databases or one."
  ([] (pool-stats nil))
  ([database-id]
   (call! (cond-> {:op "pool_stats"} database-id (assoc :database_id database-id)))))

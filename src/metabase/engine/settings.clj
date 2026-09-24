(ns metabase.engine.settings
  "Settings for the native query engine. See dev/notes/datafusion-engine.md."
  (:require
   [clojure.string]
   [metabase.config.core :as config]
   [metabase.settings.core :refer [defsetting]]
   [metabase.util.i18n :refer [deferred-tru]]))

(defsetting engine-transport
  (deferred-tru "How Metabase reaches the native query engine: ffi (in-process) or sidecar (a separate process, for development).")
  :type       :keyword
  :default    :ffi
  :visibility :internal
  :export?    false
  :doc        false
  :encryption :no)

(defsetting engine-sidecar-url
  (deferred-tru "Address of the engine sidecar, used when engine-transport is sidecar.")
  :type       :string
  :default    (when-not config/is-prod? "http://localhost:50051")
  :visibility :internal
  :export?    false
  :doc        false
  :encryption :no)

(defsetting engine-native-library-path
  (deferred-tru "Path to the native engine library. When unset, Metabase looks for the library bundled with it, then for a development build under engine/target.")
  :type       :string
  :default    nil
  :visibility :internal
  :export?    false
  :doc        false
  :encryption :no)

(defsetting query-engine
  (deferred-tru "Which execution path runs queries for a database: jdbc (the database driver), datafusion (the native query engine), or shadow (both, comparing results).")
  :type           :keyword
  :default        :jdbc
  :visibility     :internal
  :export?        false
  :doc            false
  :encryption     :no
  :database-local :allowed)

;;; ------------------------------------------------ the JDBC bridge ---------------------------------------------------
;;;
;;; Warehouses without a good Rust driver are reached through a small Arrow Flight service Metabase hosts over its own
;;; JDBC pools (`metabase.engine-bridge.core`). These settings live here rather than with the bridge so the driver and
;;; routing (which the bridge depends on) can read them without a dependency cycle.

(defsetting engine-bridge-enabled
  (deferred-tru "Whether Metabase hosts the JDBC bridge the native query engine uses for warehouses without a native connector.")
  :type       :boolean
  :default    false
  :visibility :internal
  :export?    false
  :doc        false
  :encryption :no)

(defsetting engine-bridge-port
  (deferred-tru "Port the JDBC bridge listens on (loopback only). 0 picks a free port.")
  :type       :integer
  :default    50052
  :visibility :internal
  :export?    false
  :doc        false
  :encryption :no)

(defsetting engine-bridge-warehouses
  (deferred-tru "Comma-separated database engines the native query engine reaches through the JDBC bridge instead of a native connector, e.g. `oracle,sqlserver`.")
  :type       :string
  :default    ""
  :visibility :internal
  :export?    false
  :doc        false
  :encryption :no)

(defn bridge-warehouses
  "The drivers routed through the JDBC bridge, as keywords."
  []
  (into #{}
        (comp (map clojure.string/trim) (remove clojure.string/blank?) (map keyword))
        (clojure.string/split (or (engine-bridge-warehouses) "") #",")))

(defonce ^:private bridge-state
  ;; {:url "http://127.0.0.1:port" :token "..."} once the bridge is listening; set by the bridge itself
  (atom nil))

(defn bridge-state!
  "Record where the running bridge listens and the token it expects (nil when it stops)."
  [state]
  (reset! bridge-state state))

(defn bridge-url
  "Where the running bridge listens, or nil."
  []
  (:url @bridge-state))

(defn bridge-token
  "The token the running bridge expects, or nil."
  []
  (:token @bridge-state))

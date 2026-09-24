(ns metabase.engine.settings
  "Settings for the native query engine. See dev/notes/datafusion-engine.md."
  (:require
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

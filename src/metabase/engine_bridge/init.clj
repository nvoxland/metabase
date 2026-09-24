(ns metabase.engine-bridge.init
  "Starts the JDBC bridge at boot when it is enabled."
  (:require
   [metabase.engine-bridge.core :as engine-bridge]
   [metabase.engine.settings :as engine.settings]
   [metabase.startup.core :as startup]))

(defmethod startup/def-startup-logic! ::start-bridge [_]
  (when (engine.settings/engine-bridge-enabled)
    (engine-bridge/start!)))

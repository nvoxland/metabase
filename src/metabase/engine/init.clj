(ns metabase.engine.init
  "Loads the engine module's settings and event handlers at startup. The engine itself starts
  lazily on first use so Metabase boots normally when the native library is missing."
  (:require
   [metabase.engine.events]
   [metabase.engine.settings]))

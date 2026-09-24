(ns metabase.engine.test-util
  "Helpers for tests that need the native query engine library."
  (:require
   [clojure.test :refer :all]
   [metabase.engine.core :as engine]
   [metabase.engine.ffi :as engine.ffi]
   [metabase.test :as mt]
   [metabase.util.log :as log]))

(set! *warn-on-reflection* true)

(defn required?
  "Whether the environment insists the engine tests run (`MB_ENGINE_TESTS_REQUIRED=true`, set by the
  engine CI workflow after it builds the library)."
  []
  (= "true" (System/getenv "MB_ENGINE_TESTS_REQUIRED")))

(defn available?
  "Whether the engine library can be found. Tests skip (with a warning) when it can't, unless
  [[required?]]; a plain backend test run doesn't build Rust."
  []
  (or (required?)
      (some? (engine.ffi/resolve-library-path nil))))

(defn do-with-engine!
  "Impl for [[with-engine]]."
  [thunk]
  (if (available?)
    (mt/with-temporary-setting-values [engine-transport :ffi]
      (try
        (thunk)
        (finally
          (engine/stop!))))
    (log/warn "Skipping engine test: the native query engine library was not found (cd engine && cargo build)")))

(defmacro with-engine
  "Run `body` against a freshly started in-process engine, stopping it afterwards. Skips `body` when
  the library isn't built."
  [& body]
  `(do-with-engine! (fn [] ~@body)))

(defn do-with-query-engine!
  "Impl for [[with-query-engine]]."
  [mode thunk]
  (mt/with-temp-vals-in-db :model/Database (mt/id) {:settings (assoc (:settings (mt/db)) :query-engine (name mode))}
    (thunk)))

(defmacro with-query-engine
  "Run `body` with the test database's `query-engine` set to `mode` (`:jdbc`, `:datafusion`, or
  `:shadow`) as a database-local value, which wins over any site-wide or environment default."
  [mode & body]
  `(do-with-query-engine! ~mode (fn [] ~@body)))

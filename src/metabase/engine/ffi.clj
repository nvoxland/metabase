(ns metabase.engine.ffi
  "In-process transport: the engine's shared library loaded through the Foreign Function API via
  coffi. Requiring this namespace loads nothing; [[transport]] loads the library on first use so
  Metabase starts normally when the library is missing.

  The C ABI (engine/include/metabase_engine.h) returns 0 on success, 1 on an engine error, and 2 on
  a contained panic. Errors are JSON on a thread-local slot read with `mb_engine_last_error`, so
  every read happens on the thread that made the call. Strings the engine returns through `char**`
  are freed with `mb_engine_free_string`, never by the JVM. Query results arrive as an Arrow C Data
  Interface stream that Arrow Java imports; releasing the reader releases the native side."
  (:require
   [clojure.java.io :as io]
   [clojure.string :as str]
   [coffi.ffi :as ffi]
   [coffi.mem :as mem]
   [metabase.engine.protocol :as protocol]
   [metabase.util :as u]
   [metabase.util.i18n :refer [tru]]
   [metabase.util.json :as json]
   [metabase.util.log :as log])
  (:import
   (java.io File)
   (java.lang.foreign MemorySegment)
   (java.nio.file Files StandardCopyOption)
   (org.apache.arrow.c ArrowArrayStream Data)
   (org.apache.arrow.memory BufferAllocator RootAllocator)
   (org.apache.arrow.vector.ipc ArrowReader)))

(set! *warn-on-reflection* true)

;;; ------------------------------------------------ locating the library ------------------------------------------------

(defn- os-name []
  (let [os (u/lower-case-en (System/getProperty "os.name" ""))]
    (cond
      (str/includes? os "mac")   "darwin"
      (str/includes? os "win")   "windows"
      :else                      "linux")))

(defn- arch-name []
  (case (System/getProperty "os.arch" "")
    ("aarch64" "arm64") "aarch64"
    ("x86_64" "amd64")  "x86_64"
    (System/getProperty "os.arch")))

(defn- musl? []
  (and (= (os-name) "linux")
       (some (fn [^File f] (str/starts-with? (.getName f) "ld-musl"))
             (.listFiles (io/file "/lib")))))

(defn library-file-name
  "The shared library's file name on this platform."
  []
  (case (os-name)
    "darwin"  "libmetabase_engine.dylib"
    "windows" "metabase_engine.dll"
    "libmetabase_engine.so"))

(defn- platform-dir []
  (str (os-name) "-" (arch-name) (when (musl?) "-musl")))

(defn- bundled-resource-path []
  (str "native/metabase-engine/" (platform-dir) "/" (library-file-name)))

(defn- extract-bundled-library
  "Copy the library out of the jar into a temp file. Returns its path, or nil when not bundled."
  []
  (when-let [resource (io/resource (bundled-resource-path))]
    (let [dir  (Files/createTempDirectory "metabase-engine-" (make-array java.nio.file.attribute.FileAttribute 0))
          file (io/file (.toFile dir) (library-file-name))]
      (with-open [in (io/input-stream resource)]
        (Files/copy in (.toPath file) ^"[Ljava.nio.file.CopyOption;" (into-array java.nio.file.CopyOption [StandardCopyOption/REPLACE_EXISTING])))
      (.deleteOnExit file)
      (.deleteOnExit (.toFile dir))
      (.getAbsolutePath file))))

(defn- development-build-paths []
  (for [profile ["debug" "release"]]
    (.getAbsolutePath (io/file "engine" "target" profile (library-file-name)))))

(defn resolve-library-path
  "Find the engine library: an explicit path or `MB_ENGINE_NATIVE_LIBRARY_PATH`, then the copy
  bundled in the jar, then a development build under `engine/target`. Returns nil when none exists."
  [explicit-path]
  (or (some (fn [path] (when (and (seq path) (.exists (io/file path))) path))
            [explicit-path (System/getenv "MB_ENGINE_NATIVE_LIBRARY_PATH")])
      (extract-bundled-library)
      (some (fn [path] (when (.exists (io/file path)) path)) (development-build-paths))))

;;; --------------------------------------------------- binding ---------------------------------------------------------

(defn- bind-functions
  "Resolve the C ABI. Must run after the library is loaded, which is why nothing binds at namespace
  load time."
  []
  {:version       (ffi/cfn "mb_engine_version" [] ::mem/c-string)
   :init          (ffi/cfn "mb_engine_init" [::mem/c-string ::mem/pointer] ::mem/int)
   :shutdown      (ffi/cfn "mb_engine_shutdown" [::mem/pointer ::mem/int] ::mem/int)
   :call          (ffi/cfn "mb_engine_call" [::mem/pointer ::mem/c-string ::mem/pointer] ::mem/int)
   :last-error    (ffi/cfn "mb_engine_last_error" [] ::mem/c-string)
   :free-string   (ffi/cfn "mb_engine_free_string" [::mem/pointer] ::mem/void)
   :execute       (ffi/cfn "mb_engine_execute" [::mem/pointer ::mem/long ::mem/c-string ::mem/c-string ::mem/pointer] ::mem/int)
   :stream-export (ffi/cfn "mb_engine_stream_export" [::mem/pointer ::mem/long ::mem/pointer] ::mem/int)
   :stream-cancel (ffi/cfn "mb_engine_stream_cancel" [::mem/pointer ::mem/long] ::mem/int)
   :stream-close  (ffi/cfn "mb_engine_stream_close" [::mem/pointer ::mem/long] ::mem/int)})

(defonce ^:private library (atom nil))

(defn load-library!
  "Load the shared library at `path` and bind its functions. Loading twice is a no-op for the same
  path; a different path is refused, since the JVM can't unload native code."
  [path]
  (locking library
    (let [{loaded :path} @library]
      (cond
        (nil? loaded)
        (do
          (log/infof "Loading native query engine from %s" path)
          (ffi/load-library path)
          (reset! library {:path path :fns (bind-functions)}))

        (not= loaded path)
        (throw (ex-info (tru "The native query engine is already loaded from a different path.")
                        {:loaded loaded :requested path}))

        :else
        nil)
      (:fns @library))))

(defn loaded-library-path
  "Where the engine library was loaded from, or nil."
  []
  (:path @library))

;;; ---------------------------------------------------- memory ---------------------------------------------------------

(defonce ^:private root-allocator
  (delay (RootAllocator.)))

(defn- child-allocator ^BufferAllocator [^String name]
  (.newChildAllocator ^RootAllocator @root-allocator name 0 Long/MAX_VALUE))

;;; --------------------------------------------------- transport --------------------------------------------------------

(defn- take-string!
  "Read a `char*` the engine allocated, then free it on the engine's side."
  [fns ^MemorySegment ptr]
  (let [s (mem/deserialize ptr ::mem/c-string)]
    ((:free-string fns) ptr)
    s))

(defn- last-error-json [fns]
  (or ((:last-error fns))
      (json/encode {:kind "internal_panic" :error_type "qp" :message "engine returned an error without details"})))

(defn- last-error-exception [fns]
  (protocol/engine-error->exception (json/decode+kw (last-error-json fns))))

(defn- err-envelope [error-json]
  (str "{\"status\":\"err\",\"error\":" error-json "}"))

(defn- export-stream!
  "Export a started stream as an Arrow reader. On failure the native stream is closed."
  [fns handle stream-id]
  (let [allocator (child-allocator (str "engine-stream-" stream-id))
        stream    (ArrowArrayStream/allocateNew allocator)
        code      (long ((:stream-export fns) handle stream-id (mem/as-segment (.memoryAddress stream))))]
    (if (zero? code)
      (let [reader (Data/importArrayStream allocator stream)]
        {:reader  reader
         :cancel! (fn [] ((:stream-cancel fns) handle stream-id))
         :close!  (fn []
                    (try
                      (.close ^ArrowReader reader)
                      (finally
                        ((:stream-close fns) handle stream-id)
                        (.close allocator))))})
      (let [e (last-error-exception fns)]
        (.close stream)
        (.close allocator)
        ((:stream-close fns) handle stream-id)
        (throw e)))))

(deftype FfiTransport [fns ^MemorySegment handle ^:volatile-mutable closed?]
  protocol/Transport
  (-call [_ request-json]
    (when closed?
      (throw (ex-info (tru "The native query engine has been shut down.") {})))
    (with-open [arena (mem/confined-arena)]
      (let [out  (mem/alloc-instance ::mem/pointer arena)
            code (long ((:call fns) handle request-json out))
            ptr  (mem/read-address out)]
        (cond
          (not (mem/null? ptr)) (take-string! fns ptr)
          (= code 0)            (throw (ex-info (tru "The native query engine returned no response.") {:code code}))
          :else                 (err-envelope (last-error-json fns))))))
  (-execute [_ session-id sql params]
    (when closed?
      (throw (ex-info (tru "The native query engine has been shut down.") {})))
    (let [stream-id (with-open [arena (mem/confined-arena)]
                      (let [out  (mem/alloc-instance ::mem/long arena)
                            code (long ((:execute fns) handle (long session-id) sql (json/encode (or params [])) out))]
                        (when-not (zero? code)
                          (throw (last-error-exception fns)))
                        (mem/read-long out)))]
      (export-stream! fns handle stream-id)))
  (-close [_]
    (when-not closed?
      (set! closed? true)
      (let [code (long ((:shutdown fns) handle (int 5000)))]
        (when-not (zero? code)
          (log/warnf "Native query engine shutdown returned %d: %s" code (last-error-json fns)))))))

(defn transport
  "Load the engine library (see [[resolve-library-path]]) and start an engine. `engine-config` is
  the engine's JSON config map."
  ([] (transport nil nil))
  ([explicit-path engine-config]
   (let [path (or (resolve-library-path explicit-path)
                  (throw (ex-info (tru "The native query engine library was not found.")
                                  {:explicit-path explicit-path
                                   :platform      (platform-dir)
                                   :looked-in     (into [(bundled-resource-path)] (development-build-paths))})))
         fns  (load-library! path)]
     (with-open [arena (mem/confined-arena)]
       (let [out  (mem/alloc-instance ::mem/pointer arena)
             code (long ((:init fns) (json/encode (or engine-config {})) out))]
         (when-not (zero? code)
           (throw (ex-info (tru "The native query engine failed to start.")
                           {:code code :error (json/decode+kw (last-error-json fns))})))
         (->FfiTransport fns (mem/read-address out) false))))))

(defn version
  "The engine library's version, loading it if needed."
  ([] (version nil))
  ([explicit-path]
   (let [fns (load-library! (or (resolve-library-path explicit-path)
                                (throw (ex-info (tru "The native query engine library was not found.") {}))))]
     ((:version fns)))))

(ns build.engine
  "Build step that compiles the native query engine (`engine/`, a Rust cdylib) for the host platform and puts it
  where `metabase.engine.ffi` looks for it inside the jar: `resources/native/metabase-engine/<os>-<arch>[-musl]/`.

  The platform directory name must match `metabase.engine.ffi/platform-dir`."
  (:require
   [clojure.java.io :as io]
   [clojure.string :as str]
   [environ.core :as env]
   [metabuild-common.core :as u])
  (:import
   (java.io File)))

(set! *warn-on-reflection* true)

(def ^:private engine-dir
  (u/filename u/project-root-directory "engine"))

(def ^:private native-resources-dir
  (u/filename u/project-root-directory "resources" "native" "metabase-engine"))

(defn- os-name []
  (let [os (str/lower-case (System/getProperty "os.name" ""))]
    (cond
      (str/includes? os "mac") "darwin"
      (str/includes? os "win") "windows"
      :else                    "linux")))

(defn- arch-name []
  (case (System/getProperty "os.arch" "")
    ("aarch64" "arm64") "aarch64"
    ("x86_64" "amd64")  "x86_64"
    (System/getProperty "os.arch")))

(defn- musl? []
  (and (= (os-name) "linux")
       (some (fn [^File f] (str/starts-with? (.getName f) "ld-musl"))
             (.listFiles (io/file "/lib")))))

(defn- platform-dir []
  (str (os-name) "-" (arch-name) (when (musl?) "-musl")))

(defn- library-file-name []
  (case (os-name)
    "darwin"  "libmetabase_engine.dylib"
    "windows" "metabase_engine.dll"
    "libmetabase_engine.so"))

(defn- cargo-available? []
  (try
    (zero? (:exit (u/sh* {:quiet? true} "cargo" "--version")))
    (catch Exception _
      false)))

(defn- prebuilt-libraries []
  (when (u/file-exists? native-resources-dir)
    (seq (u/find-files native-resources-dir #(re-find #"metabase_engine\.(so|dylib|dll)$" %)))))

(defn build-engine-native!
  "Compile the engine for the host platform into `resources/native/metabase-engine/<platform>/`.

  - `SKIP_ENGINE_NATIVE=true` skips the step entirely (the engine then degrades to the JDBC path at runtime).
  - When CI has already placed per-platform libraries under `resources/native/metabase-engine/` (the `build-native`
    matrix in `.github/workflows/engine.yml`), they are kept and nothing is compiled.
  - Without a Rust toolchain the step warns and continues: the engine is optional at runtime."
  [_edition]
  (cond
    (= (env/env :skip-engine-native) "true")
    (u/announce "Skipping native query engine (SKIP_ENGINE_NATIVE=true)")

    (prebuilt-libraries)
    (u/announce "Using prebuilt native query engine libraries: %s" (str/join ", " (prebuilt-libraries)))

    (not (cargo-available?))
    (u/announce "WARNING: cargo not found; the uberjar will not include the native query engine for %s" (platform-dir))

    :else
    (u/step (format "Build native query engine for %s" (platform-dir))
      (u/sh {:dir engine-dir} "cargo" "build" "--release" "-p" "engine-ffi")
      (let [source (u/filename engine-dir "target" "release" (library-file-name))
            dir    (u/filename native-resources-dir (platform-dir))
            dest   (u/filename dir (library-file-name))]
        (u/create-directory-unless-exists! dir)
        (u/delete-file-if-exists! dest)
        (u/copy-file! source dest)
        (u/announce "Native query engine built: %s" dest)))))

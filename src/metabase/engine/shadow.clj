(ns metabase.engine.shadow
  "Shadow mode: a database whose `query-engine` is `shadow` answers queries from its driver as
  usual, and each MBQL query is also re-run in the background through both paths and compared.
  Mismatches are logged and kept in [[recent]] so the gap between the engine and the warehouse
  driver can be measured on real traffic before switching a database over."
  (:require
   [metabase.util :as u]
   [metabase.util.log :as log]))

(set! *warn-on-reflection* true)

(def ^:private max-recent 200)

(defonce ^:private recent-comparisons
  ;; newest first
  (atom []))

(defonce ^:private pending
  (atom #{}))

(defn recent
  "The most recent comparisons, newest first: `{:match? :query :jdbc :engine :error :elapsed-ms}`."
  []
  @recent-comparisons)

(defn clear!
  "Forget recorded comparisons."
  []
  (reset! recent-comparisons []))

(defn- same-value? [a b]
  (if (and (number? a) (number? b))
    (== (double a) (double b))
    (= a b)))

(defn same-rows?
  "Row-by-row equality that ignores numeric representation (the engine types some aggregates as
  doubles where a warehouse returns decimals)."
  [expected actual]
  (and (= (count expected) (count actual))
       (every? true? (map (fn [row-a row-b]
                            (and (= (count row-a) (count row-b))
                                 (every? true? (map same-value? row-a row-b))))
                          expected actual))))

(defn- completed? [result]
  (contains? #{:completed "completed"} (:status result)))

(defn compare-results
  "Compare two query results (as returned by `process-query`) and describe the outcome."
  [jdbc engine]
  (let [jdbc-rows   (-> jdbc :data :rows)
        engine-rows (-> engine :data :rows)
        jdbc-cols   (map :name (-> jdbc :data :cols))
        engine-cols (map :name (-> engine :data :cols))]
    (cond
      ;; userland results say "completed"; internal ones say :completed
      (not (completed? jdbc))   {:match? false :error :jdbc-failed :detail (or (:error jdbc) (:status jdbc))}
      (not (completed? engine)) {:match? false :error :engine-failed :detail (or (:error engine) (:status engine))}
      (not= jdbc-cols engine-cols)        {:match? false :error :columns-differ :detail {:jdbc jdbc-cols :engine engine-cols}}
      (not (same-rows? jdbc-rows engine-rows)) {:match? false :error :rows-differ
                                                :detail {:jdbc-count (count jdbc-rows) :engine-count (count engine-rows)
                                                         :first-difference (first (keep (fn [[i a b]] (when-not (and a b (same-rows? [a] [b])) {:row i :jdbc a :engine b}))
                                                                                        (map vector (range) (concat jdbc-rows (repeat nil)) (concat engine-rows (repeat nil)))))}}
      :else                               {:match? true})))

(defn record!
  "Record a comparison for `query`, logging mismatches."
  [query outcome elapsed-ms]
  (let [entry (assoc outcome :query query :elapsed-ms elapsed-ms)]
    (swap! recent-comparisons (fn [entries] (vec (take max-recent (cons entry entries)))))
    (if (:match? outcome)
      (log/debugf "Shadow comparison matched (%d ms)" elapsed-ms)
      (log/warnf "Shadow comparison mismatch (%s): %s" (:error outcome) (pr-str (:detail outcome))))
    entry))

(defn run-in-background!
  "Run `compare-fn` (a no-arg function producing a comparison outcome) on a fresh thread and record
  its result. A plain thread, not a future, so the query processor's dynamic bindings (the bound
  driver, the metadata provider, the setup marker) don't carry over and the re-runs get their own
  setup. Returns a promise of the recorded entry; [[await-pending!]] waits for all of them."
  [query compare-fn]
  (let [timer   (u/start-timer)
        done    (promise)
        thread  (Thread.
                 (fn []
                   (try
                     (let [outcome (try
                                     (compare-fn)
                                     (catch Throwable e
                                       {:match? false :error :comparison-threw :detail (ex-message e)}))]
                       (deliver done (record! query outcome (u/since-ms timer))))
                     (catch Throwable e
                       (deliver done {:match? false :error :comparison-threw :detail (ex-message e)}))
                     (finally
                       (swap! pending disj done))))
                 "engine-shadow-comparison")]
    (swap! pending conj done)
    (.setDaemon thread true)
    (.start thread)
    done))

(defn await-pending!
  "Wait for background comparisons to finish (tests)."
  []
  (doseq [p @pending]
    (deref p 60000 nil))
  (reset! pending #{}))

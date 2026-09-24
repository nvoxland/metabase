(ns metabase.engine.shadow
  "Shadow mode: a database whose `query-engine` is `shadow` answers queries from its driver as
  usual, and each MBQL query is also re-run in the background through both paths and compared.
  Mismatches are logged and kept in [[recent]] so the gap between the engine and the warehouse
  driver can be measured on real traffic before switching a database over."
  (:require
   [metabase.util :as u]
   [metabase.util.log :as log])
  (:import
   (java.util.concurrent ArrayBlockingQueue RejectedExecutionException ThreadFactory ThreadPoolExecutor TimeUnit)))

(set! *warn-on-reflection* true)

(def ^:private max-recent 200)

(defonce ^:private recent-comparisons
  ;; newest first
  (atom []))

(defonce ^:private pending
  (atom #{}))

(defonce ^:private dropped
  ;; comparisons not run because the executor was saturated
  (atom 0))

(defonce ^:private executor
  ;; A small pool with a bounded queue: shadow mode re-runs every query twice, so under load it
  ;; samples rather than doubling the warehouse's work. Plain threads (not futures) so the query
  ;; processor's dynamic bindings don't carry over and each rerun gets its own setup.
  (ThreadPoolExecutor. 2 2 30 TimeUnit/SECONDS
                       (ArrayBlockingQueue. 64)
                       (reify ThreadFactory
                         (newThread [_ runnable]
                           (doto (Thread. runnable "engine-shadow-comparison")
                             (.setDaemon true))))))

(defn recent
  "The most recent comparisons, newest first: `{:match? :query :jdbc :engine :error :elapsed-ms}`."
  []
  @recent-comparisons)

(defn dropped-count
  "How many comparisons were skipped because the background executor was saturated."
  []
  @dropped)

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
  ;; the attached metadata provider (every table and field the request fetched) would otherwise be pinned for as long
  ;; as the comparison is kept
  (let [entry (assoc outcome :query (dissoc query :lib/metadata) :elapsed-ms elapsed-ms)]
    (swap! recent-comparisons (fn [entries] (vec (take max-recent (cons entry entries)))))
    (if (:match? outcome)
      (log/debugf "Shadow comparison matched (%d ms)" elapsed-ms)
      ;; row values stay out of the log; [[recent]] keeps them for whoever is measuring the gap
      (log/warnf "Shadow comparison mismatch for database %s (%s): %s"
                 (:database query)
                 (:error outcome)
                 (pr-str (cond-> (:detail outcome)
                           (map? (:detail outcome)) (update :first-difference #(some-> % (select-keys [:row])))))))
    entry))

(defn run-in-background!
  "Run `compare-fn` (a no-arg function producing a comparison outcome) on the shadow executor and
  record its result. Returns a promise of the recorded entry, or nil when the executor is saturated
  and the comparison is dropped; [[await-pending!]] waits for all of them."
  [query compare-fn]
  (let [timer (u/start-timer)
        done  (promise)
        task  (fn []
                (try
                  (let [outcome (try
                                  (compare-fn)
                                  (catch Throwable e
                                    {:match? false :error :comparison-threw :detail (ex-message e)}))]
                    (deliver done (record! query outcome (u/since-ms timer))))
                  (catch Throwable e
                    (deliver done {:match? false :error :comparison-threw :detail (ex-message e)}))
                  (finally
                    (swap! pending disj done))))]
    (swap! pending conj done)
    (try
      (.execute ^ThreadPoolExecutor executor ^Runnable task)
      done
      (catch RejectedExecutionException _
        (swap! pending disj done)
        (swap! dropped inc)
        (log/debug "Shadow comparison dropped: the background executor is saturated")
        nil))))

(defn await-pending!
  "Wait for background comparisons to finish (tests)."
  []
  (doseq [p @pending]
    (deref p 60000 nil))
  (reset! pending #{}))

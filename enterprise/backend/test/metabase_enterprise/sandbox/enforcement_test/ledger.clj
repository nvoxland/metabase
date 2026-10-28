(ns metabase-enterprise.sandbox.enforcement-test.ledger
  "The known findings, so that a run of the enforcement tests says what changed instead of how much is red.

  `ledger.edn` next to this file lists, per test and driver, every state that is expected to fail: the state (a map
  such as `{:policy :mode :probe :transport :format}`), its outcome, and the ticket that tracks it. A test reports each
  of its states through [[check!]] and ends with [[finish!]] (or wraps its body in [[with-ledger]]). The run fails on:

  * a new failure: a failing state the ledger does not list;
  * a changed finding: a listed state with another outcome than recorded, a fixed one included;
  * a stale entry: a listed state the run no longer produces, among the policies and modes it ran.

  So a fix PR turns its states green and must delete them from the ledger, and a regression shows up as the one new
  failure it is. Every entry carries a ticket from [[classify]]; one that matches no rule is `:unclassified`, which also
  fails the run, so a new failure cannot be recorded without someone deciding what it is.

  To rewrite a test's section from a run (after a fix, or to record a new finding once it is filed), set
  `MB_SANDBOX_LEDGER_WRITE=true`: [[check!]] then asserts nothing and [[finish!]] replaces the section for the
  drivers that ran."
  (:require
   [clojure.edn :as edn]
   [clojure.java.io :as io]
   [clojure.pprint :as pprint]
   [clojure.test :refer [is testing]]
   [metabase.driver :as driver]))

(set! *warn-on-reflection* true)

(def ^:private path
  "The ledger file, relative to the repository root that tests run from."
  "enterprise/backend/test/metabase_enterprise/sandbox/enforcement_test/ledger.edn")

(defn- write-mode? []
  (= "true" (System/getenv "MB_SANDBOX_LEDGER_WRITE")))

(defonce ^:private cache
  ;; [last-modified ledger], so a run reads the file once
  (atom nil))

(declare parse-ledger)

(defn- read-ledger
  "test -> driver -> state -> `{:outcome :ticket}`."
  []
  (let [f (io/file path)
        t (.lastModified f)]
    (if-not (.exists f)
      {}
      (let [[cached-t ledger] @cache]
        (if (= cached-t t)
          ledger
          (second (reset! cache [t (parse-ledger f)])))))))

(defn- parse-ledger [f]
  (into {}
        (for [[test-k drivers] (edn/read-string (slurp f))]
          [test-k (into {} (for [[driver-k entries] drivers]
                             [driver-k (into {} (for [{:keys [state outcome ticket]} entries]
                                                  [state {:outcome outcome :ticket ticket}]))]))])))

(defn- write-ledger!
  [ledger]
  (with-open [^java.io.Writer w (io/writer path)]
    (.write w ";; Known sandbox enforcement findings: see metabase-enterprise.sandbox.enforcement-test.ledger.\n")
    (.write w ";; Rewrite a test's section with MB_SANDBOX_LEDGER_WRITE=true; never edit outcomes by hand.\n")
    (pprint/write (into (sorted-map)
                        (for [[test-k drivers] ledger]
                          [test-k (into (sorted-map)
                                        (for [[driver-k entries] drivers]
                                          [driver-k (vec (sort-by pr-str
                                                                  (for [[state {:keys [outcome ticket]}] entries]
                                                                    {:state state :outcome outcome :ticket ticket})))]))]))
                  :stream w)
    (.write w "\n")))

;;; ------------------------------------------------ Classification ------------------------------------------------

(def ^:private attribute-lists
  "Attribute variants whose value is a list or a map."
  #{:attr-list :attr-map :card-list :rating-list :native-list})

(def ^:private column-policies
  "Hand-written policies that hide columns."
  #{:card-hide-price :card-rows+hide :native-hide-price :card-hide-price+persisted})

(defn- hides-columns?
  "Does `policy`, a hand-written policy key or a generated spec, hide columns?"
  [policy]
  (if (map? policy)
    (boolean (some (comp seq :hidden) (vals policy)))
    (contains? column-policies policy)))

(defn classify
  "The ticket that tracks a failing `state` of `test-k` with `outcome`, or `:unclassified`. Rules are checked in order;
  the first that matches wins."
  [test-k {:keys [policy probe mode] :as state} outcome]
  (let [probe-name (if (vector? probe) (first probe) probe)
        forged     (when (and (vector? probe) (= :forged-metadata (first probe))) (second probe))]
    (cond
      (#{:scanned-entry-points :two-users-scanned} test-k)
      (if (and (= :canary (:check state)) (= policy :table-tier+value-remap)) "SEC-1333" "SEC-1335")

      (= policy :table-tier+value-remap)                                  "SEC-1333"
      (attribute-lists policy)                                            "SEC-1334"
      (and (= :test mode) (#{:model-bogus-join-alias :card-bogus-join-alias} probe-name)) "SEC-1340"
      (= :hygiene test-k)                                                 "SEC-1337"
      (= outcome :raw-ref)                                                "SEC-1336"
      (= forged :lib-names-disagree)                                      (if (= outcome :hidden-column-values) "SEC-1343" "SEC-1338")
      (#{:accidental-rejection :hidden-column-in-sql :dangling-ref} outcome) "SEC-1338"
      (#{:shops-out-to-regions :regions-into-shops} probe-name)           "SEC-1342"
      (and (= :generated test-k) (hides-columns? policy))                "SEC-1339"
      (= outcome :not-meaningful)                                         "SEC-1338"
      :else                                                               :unclassified)))

;;; ---------------------------------------------------- Checks ----------------------------------------------------

(defonce ^:private observed
  ;; [test driver] -> state -> {:outcome :failing?}, for the run in progress
  (atom {}))

(defn check!
  "Report one state of `test-k`: `outcome`, and whether the test's own rule calls it a failure. Asserts it against the
  ledger, unless writing it."
  [test-k state outcome failing?]
  (let [k [test-k driver/*driver*]]
    (swap! observed assoc-in [k state] {:outcome outcome :failing? (boolean failing?)})
    (when-not (write-mode?)
      (let [known (get-in (read-ledger) [test-k driver/*driver* state])]
        (testing (pr-str state)
          (if known
            (is (= (:outcome known) outcome)
                (str "A known finding (" (:ticket known) ") changed: expected " (:outcome known) ", got " outcome
                     ". If it was fixed, rewrite the ledger with MB_SANDBOX_LEDGER_WRITE=true."))
            (is (not failing?)
                (str "A new failure, " outcome ", that the ledger does not list. File or classify it, then rewrite the "
                     "ledger with MB_SANDBOX_LEDGER_WRITE=true."))))))))

(defn- scope [state] (select-keys state [:policy :mode]))

(defn finish!
  "End `test-k`'s run on the current driver: write its section in write mode, otherwise fail on ledger entries the run
  no longer produces (within the policies and modes it ran) and on unclassified entries."
  [test-k]
  (let [k     [test-k driver/*driver*]
        seen  (get @observed k {})
        known (get-in (read-ledger) [test-k driver/*driver*] {})]
    (swap! observed dissoc k)
    (if (write-mode?)
      (let [failing (into {}
                          (for [[state {:keys [outcome failing?]}] seen
                                :when failing?]
                            [state {:outcome outcome :ticket (classify test-k state outcome)}]))]
        (write-ledger! (assoc-in (read-ledger) [test-k driver/*driver*] failing)))
      (let [ran (into #{} (map scope) (keys seen))]
        (doseq [[state {:keys [ticket]}] known
                :when (contains? ran (scope state))]
          (testing (pr-str state)
            (is (contains? seen state)
                "The ledger lists a state this run no longer produces; rewrite the ledger with MB_SANDBOX_LEDGER_WRITE=true.")
            (is (not= :unclassified ticket)
                "A ledger entry with no ticket: file or classify the finding.")))))))

(defmacro with-ledger
  "Run `body`, which reports its states with [[check!]] under `test-k`, then [[finish!]] it."
  [test-k & body]
  `(try
     ~@body
     (finally
       (finish! ~test-k))))

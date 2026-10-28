(ns metabase-enterprise.sandbox.enforcement-test.invariants
  "What must hold of a sandboxed user's response. Each invariant is a fn returning a verdict map, and every verdict is
  recorded on every result:

  * [[differential]] -- the user's columns and rows equal an admin's on the restricted variant (a sandbox is a view),
    or the user's query fails in a declared way.
  * [[canary]] -- no canary of a row the sandbox hides appears anywhere in the user's response.
  * [[structural]] -- in the preprocessed MBQL after alias resolution, no field ref outside a sandbox stage has a
    sandboxed table as its physical source, and no stage reads it as its source.
  * [[sql]] -- in the compiled SQL, the sandboxed table is read only inside the sandbox subquery
    (see [[invariants.sql/structural-sql]]).
  * [[hygiene]] -- the response carries no fingerprint, native or preprocessed form, internal stage, or SQL text.
  * [[error-secrets]] -- an error the user gets carries no value the sandbox hides and names no hidden column.
  * [[metadata]] -- the result columns' types and binning equal the admin's.
  * [[http-equivalence]] -- an HTTP entry point returns the rows and columns the processor returns in-process.
  * [[sql-dangling-reads]] -- in the compiled SQL, every column read through a subquery is one it returns.
  * [[sql-hidden-columns]] -- the compiled SQL reads no hidden column through a sandbox subquery.
  * [[failure-layer]] -- where a refused query failed; the differential requires a column sandbox to refuse on purpose.

  [[registry]] lists them; [[judge]] applies them all to one state.

  Each docstring says what the invariant looks at, and so where a failure lies: `differential`, `canary` and
  `metadata` read the executed result, so a failure is anywhere in the stack; `structural` reads the preprocessed
  query, so a failure is in the query processor's middleware; `sql` reads the compiled query, so a failure is in the
  compiler; `hygiene` and `error-secrets` read the response text, so a failure is in error handling or the driver.

  [[outcomes]] lists every outcome with its meaning and whether it fails a test; [[outcome]] folds a result's verdicts
  into one, worst first.
  [[disagreements]] finds results where two invariants contradict each other, which is a bug in an invariant.
  [[mask-response]] prepares a response for the noninterference comparison."
  (:require
   [clojure.string :as str]
   [clojure.walk :as walk]
   [metabase-enterprise.sandbox.enforcement-test.dataset :as dataset]
   [metabase-enterprise.sandbox.enforcement-test.invariants.sql :as invariants.sql]
   [metabase-enterprise.sandbox.enforcement-test.queries.hand-written :as hand-written]
   [metabase.lib.walk :as lib.walk]
   [metabase.query-processor.error-type :as qp.error-type]
   [metabase.query-processor.util.add-alias-info :as add]
   [metabase.test :as mt]
   [metabase.util :as u]
   [metabase.util.match :as match]))

(set! *warn-on-reflection* true)

;;; -------------------------------------------------- Detectors ---------------------------------------------------

(defn- hidden-indexes
  "Indexes of `result`'s columns that a sandbox hides. A column is identified by its field id when it has one, so a
  REGIONS.SECRET is visible while SHOPS.SECRET is hidden; a column without an id (an aggregation, a native column) by
  its name in `:hidden-cols`."
  [{:keys [hidden-cols hidden-field-ids]} {:keys [cols col-ids]}]
  (into #{}
        (keep-indexed (fn [i col-name]
                        (when (if-let [id (get col-ids i)]
                                (contains? (or hidden-field-ids #{}) id)
                                (contains? (or hidden-cols #{}) col-name))
                          i)))
        cols))

(defn- visible-names
  "`result`'s column names, hidden ones left out, in order."
  [hidden result]
  (vec (keep-indexed (fn [i c] (when-not (hidden i) c)) (:cols result))))

(defn- project
  "The rows of `result` restricted to the visible columns named `col-names`, as a multiset."
  [hidden {:keys [rows cols]} col-names]
  (let [index-of (reduce (fn [m [i c]] (if (or (hidden i) (contains? m c)) m (assoc m c i)))
                         {}
                         (map-indexed vector cols))
        idxs     (keep index-of col-names)]
    (frequencies (map (fn [row] (mapv #(nth row %) idxs)) rows))))

(defn- column-verdict
  "The columns the user got against the expected ones, as multisets of names with the hidden columns left out:
  `:extra-columns` when the user has a column (or a copy of one) the expected result lacks, `:missing-columns` when
  an expected column is gone, nil when they agree. The row comparison only means something once they agree."
  [user-names expected-names]
  (let [user-m     (frequencies user-names)
        expected-m (frequencies expected-names)]
    (cond
      (some (fn [[col n]] (> n (get expected-m col 0))) user-m) :extra-columns
      (not= user-m expected-m)                                 :missing-columns)))

(defn- hidden-column-values?
  "Does `result` carry a value in a column the sandbox hides?"
  [hidden {:keys [rows]}]
  (boolean (some (fn [i] (some #(some? (nth % i)) rows)) hidden)))

(defn failure-layer
  "Where the user's query failed, from the harness's observations: `:deliberate` when something refused it on
  purpose (a permission error, HTTP 403), `:preprocess` when preprocessing threw anything else, `:compile` when it
  preprocessed but did not compile, `:warehouse` when it compiled but the database refused it; nil when it ran."
  [{:keys [preprocessed compiled executed]}]
  (let [permission? #(some-> % qp.error-type/permission-error?)]
    (cond
      (not (:error executed))                                    nil
      (or (= 403 (:status executed))
          (permission? (get-in preprocessed [:error :type]))
          (permission? (some-> executed :body :error_type keyword)))
      :deliberate
      (:error preprocessed)                                      :preprocess
      (:error compiled)                                          :compile
      :else                                                      :warehouse)))

(defn differential
  "Compare the sandboxed user's result with the expected one (an admin on the restricted variant, or on the base
  database when nothing should be restricted). `admin-base` tells whether the probe could tell restricted data from
  unrestricted data at all. Column names are compared first, so a column that goes missing or appears cannot hide
  behind a row comparison; hidden columns (see [[hidden-indexes]]) may be omitted or returned empty.

  A failure of the user's query alone is judged by where it happened (`:failure-layer` on `user`, see
  [[failure-layer]]): a policy that hides columns must refuse such a query on purpose (`:refused`); failing anywhere
  else is `:accidental-rejection`, a query nothing guarded. Otherwise it is `:known-rejection` when the policy's
  `:accepted-rejections` patterns match it, else `:user-error`."
  [{:keys [expect hidden-cols accepted-rejections] :as ctx} user expected admin-base]
  (cond
    ;; a probe that cannot tell the restricted data from the rest may succeed; one that can must fail
    (= expect :error)                     (if (:error user)
                                            :failed-as-expected
                                            (let [v (differential (assoc ctx :expect :restricted) user expected admin-base)]
                                              (if (= v :consistent) :missing-error v)))
    (and (:error expected) (:error user)) :rejected
    (:error expected)                     :expected-error
    (:error user)                         (cond
                                            (= expect :unrestricted)                              :over-restricted
                                            (and (seq hidden-cols) (= :deliberate (:failure-layer user))) :refused
                                            (some #(re-find % (:error user)) accepted-rejections) :known-rejection
                                            (seq hidden-cols)                                     :accidental-rejection
                                            :else                                                 :user-error)
    :else
    (let [ctx           (cond-> ctx (= expect :unrestricted) (assoc :hidden-cols #{} :hidden-field-ids #{}))
          user-hidden   (hidden-indexes ctx user)
          exp-hidden    (hidden-indexes ctx expected)
          base-hidden   (hidden-indexes ctx admin-base)
          common        (visible-names exp-hidden expected)]
      (or (column-verdict (visible-names user-hidden user) common)
          (when (hidden-column-values? user-hidden user) :hidden-column-values)
          (let [user-m     (project user-hidden user common)
                expected-m (project exp-hidden expected common)]
            (cond
              (= expect :unrestricted)
              (if (= user-m expected-m) :consistent :over-restricted)

              (= user-m expected-m)
              ;; the probe told restricted data from the rest when the rows differ from the unrestricted ones, or
              ;; when the unrestricted result showed a hidden column's values
              (if (and (= (project base-hidden admin-base common) expected-m)
                       (not (hidden-column-values? base-hidden admin-base)))
                :vacuous
                :consistent)

              (some (fn [[row n]] (> n (get expected-m row 0))) user-m)
              :extra-rows

              :else
              :missing-rows))))))

(defn canary
  "Canaries of hidden rows anywhere in the user's response. A number that the expected response carries too is no
  leak: it was computed from allowed rows alone, as a weighted average of allowed canaries can land on a hidden one."
  [{:keys [allowed]} user expected]
  (let [derivable (into #{} (comp (filter #(= :number (:kind %))) (map (juxt :table :id))) (dataset/canary-hits (:body expected)))
        leaked    (into (sorted-set)
                        (keep (fn [{:keys [table id kind]}]
                                (let [hit [table id]]
                                  (when-not (or (allowed hit) (and (= kind :number) (derivable hit)))
                                    hit))))
                        (dataset/canary-hits (:body user)))]
    (if (seq leaked) {:verdict :leak :leaked leaked} {:verdict :clean})))

(defn- raw-refs-to-tables
  "Field refs in `preprocessed` (the user's query after preprocessing and alias resolution), outside any sandbox
  stage, whose physical source is one of `table-ids`; and stages that still read one of them as their source."
  [preprocessed table-ids]
  (let [acc (volatile! [])]
    (lib.walk/walk-stages
     preprocessed
     (fn [_query path stage]
       (when-not (:query-permissions/sandboxed-table stage)
         ;; a stage still reading the raw table is the plainest failure of the rewrite
         (when (contains? table-ids (:source-table stage))
           (vswap! acc conj {:path path :unmarked-source-table (:source-table stage)}))
         (let [own (-> stage
                       (dissoc :lib/stage-metadata)
                       (update :joins (partial mapv #(dissoc % :stages :lib/stage-metadata))))]
           (vswap! acc into
                   (map (fn [ref] {:path path :ref ref}))
                   (match/match-many own
                     [:field (opts :guard (contains? table-ids (::add/source-table opts))) _] &match))))
       nil))
    @acc))

(defn structural
  "Looks at the preprocessed query: field refs that read a sandboxed table outside a sandbox stage, or a stage that
  still reads it as its source. A failure here is in the query processor's rewrite or field resolution.
  `preprocessed` is the harness's observation, `{:value query}` or `{:error ..}`."
  [{:keys [ids sandboxed]} {:keys [value error]}]
  (let [table-ids (into #{} (map #(dataset/tid ids %)) sandboxed)]
    (cond
      error {:verdict :error :error (:message error)}
      :else (let [refs (raw-refs-to-tables value table-ids)]
              (if (seq refs) {:verdict :raw-ref :refs refs} {:verdict :clean})))))

(defn sql
  "Looks at the compiled SQL: the sandboxed tables must be read only inside the sandbox subquery. A failure here is in
  the compiler. See [[invariants.sql/structural-sql]]."
  [{:keys [ids sandboxed]} preprocessed sql]
  (invariants.sql/structural-sql {:names     (into {} (for [t sandboxed] [t (mt/format-name (name t))]))
                                  :table-ids (into {} (for [t sandboxed] [t (dataset/tid ids t)]))}
                                 preprocessed
                                 sql))

;;; ------------------------------------------------- Sync checks --------------------------------------------------
;;;
;;; The sandbox rewrite swaps a stage's source table for the sandbox subquery. Whatever else in the query still points
;;; at what that source used to return has to be reconciled with what the sandbox returns now; a ref that is not
;;; is caught, if at all, by alias resolution or the warehouse, by accident. These invariants measure that on the
;;; compiled SQL, the ground truth: Lib's view of what a stage returns differs from the compiler's for remapped joins
;;; and stages added on top, so a check on the preprocessed query reports reads that run fine.

(defn sql-dangling-reads
  "Looks at the compiled SQL: every column read through a subquery's alias must be one that subquery returns. The
  compiler emitting such a read means a ref the rewrite should have reconciled reached it; only the warehouse can
  refuse it. Reads of hidden columns are [[sql-hidden-columns]]'s. See [[invariants.sql/dangling-reads]]."
  [{:keys [hidden-cols]} sql]
  (if-not (string? sql)
    {:verdict :skipped}
    (let [reads (remove :hidden? (invariants.sql/dangling-reads sql (or hidden-cols #{})))]
      (if (seq reads) {:verdict :dangling :reads (vec (take 6 reads))} {:verdict :clean}))))

(defn sql-hidden-columns
  "Looks at the compiled SQL: no hidden column may be read through a sandbox subquery's alias that does not return it.
  The compiler emitting such a read means a ref to a hidden column reached it. See
  [[invariants.sql/hidden-column-reads]]."
  [{:keys [hidden-cols]} sql]
  (cond
    (empty? hidden-cols) {:verdict :n/a}
    (not (string? sql))  {:verdict :skipped}
    :else (let [reads (invariants.sql/hidden-column-reads sql hidden-cols)]
            (if (seq reads) {:verdict :hidden-column-read :reads (vec (take 6 reads))} {:verdict :clean}))))

;;; ------------------------------------------- Detector cross-checks ----------------------------------------------

(defn disagreements
  "Results where two detectors contradict each other. A non-empty answer is a bug in a detector, not in sandboxing:
  the differential oracle saying the rows are right while a canary of a hidden row got through, or the rows holding
  extra data from a canary column without the canary scan noticing."
  [results]
  (for [{:keys [differential canary user probe config] :as r} results
        :let [secret-col? (some #(str/includes? (u/lower-case-en (str %)) "secret") (:cols user))
              reason (cond
                       (and (#{:consistent :vacuous} differential) (= :leak (:verdict canary)))
                       :rows-right-but-canary-leaked

                       (and (= :extra-rows differential) secret-col? (= :clean (:verdict canary)))
                       :extra-rows-with-canary-column-but-no-canary)]
        :when reason]
    {:policy config :probe probe :reason reason :result (select-keys r [:differential :canary :user])}))

(defn- scan-paths
  "Seq of `[path value]` for every scalar in `x`."
  [x]
  (let [acc (volatile! [])]
    (letfn [(scan [path v]
              (cond
                (map? v)        (doseq [[k child] v] (scan (conj path k) child))
                (sequential? v) (doall (map-indexed (fn [i child] (scan (conj path i) child)) v))
                :else           (vswap! acc conj [path v])))]
      (scan [] x))
    @acc))

(def ^:private sql-text-pattern #"(?i)\bSELECT\b|\bFROM\b.*\bJOIN\b")

(defn- fingerprinted-maps
  "`[path m]` for every map in `x` that carries a non-nil `:fingerprint`."
  [x]
  (let [acc (volatile! [])]
    (letfn [(scan [path v]
              (cond
                (map? v)        (do (when (some? (:fingerprint v)) (vswap! acc conj [path v]))
                                    (doseq [[k child] v] (scan (conj path k) child)))
                (sequential? v) (doall (map-indexed (fn [i child] (scan (conj path i) child)) v))))]
      (scan [] x))
    @acc))

(defn hygiene
  "Looks at the response text, wherever it sits: a fingerprint of a sandboxed table's column (a column of another
  table is the user's to see; an aggregation's fingerprint comes from the sandboxed rows), a native or
  preprocessed form, internal query stages, the physical name of a sandboxed table, or SQL text in an error. A failure
  here is in error handling, metadata stripping or the driver. `:json_query` echoes the user's own input and is
  skipped."
  [{:keys [ids sandboxed]} user]
  (let [body        (let [b (:body user)] (if (map? b) (dissoc b :json_query :dataset_query) b))
        table-names (into #{} (map #(mt/format-name (name %))) sandboxed)
        table-ids   (into #{} (map #(dataset/tid ids %)) sandboxed)
        problems    (concat
                     (for [[path m] (fingerprinted-maps body)
                           :let [table-id (or (:table_id m) (:table-id m))]
                           ;; a column that names a field but no table is treated as sandboxed; one that names
                           ;; neither (an aggregation, an expression) was fingerprinted from the sandboxed rows
                           :when (if table-id (contains? table-ids table-id) (some? (:id m)))]
                       {:kind :internal-metadata :path (conj path :fingerprint)})
                     (for [[path v] (scan-paths body)
                           :when (and (some? v)
                                      (some #{:native :preprocessed :native_form "native_form"} path))]
                       {:kind :internal-metadata :path path})
                     (for [[path v] (scan-paths body)
                           :when (and (string? v)
                                      (or (some #(str/includes? v (str "\"" % "\"")) table-names)
                                          (re-find sql-text-pattern v)))]
                       {:kind :sql-text :path path :value (u/truncate v 120)})
                     (for [[path _] (scan-paths body)
                           :when (some #{:stages "stages"} path)]
                       {:kind :internal-query :path path}))
        problems    (vec (distinct (map #(update % :path (partial take 4)) problems)))]
    (if (seq problems) {:verdict :dirty :problems (vec (take 6 problems))} {:verdict :clean})))

(defn- whole-word
  "A pattern matching `s` where it is not part of a longer word."
  [s]
  (re-pattern (str "(?<![A-Za-z0-9_])" (java.util.regex.Pattern/quote s) "(?![A-Za-z0-9_])")))

(defn- error-text
  "The failed `result`'s error text: its message and every scalar of its body, but not the user's query it echoes."
  [result]
  (when (:error result)
    (cons (:error result)
          (map second (scan-paths (let [b (:body result)] (if (map? b) (dissoc b :json_query :dataset_query) b)))))))

(defn error-secrets
  "Looks at the error the user got, in-process and through HTTP: SQL that a driver echoes, or a value it failed to
  convert, may carry what the sandbox hides. Finds the text values and canaries the restricted data does not show
  (`:secrets`, see [[dataset/secrets]]) and the names of hidden columns as identifiers (`:hidden-names`, name -> its
  field ids), except what the user's own `query` holds: its values, the fields it names, and the columns of the saved
  questions it reads (`:card-columns`, card id -> names), whose stored metadata the user sees. `:n/a` when neither
  failed."
  [{:keys [secrets hidden-names card-columns]} query user http]
  (let [texts      (concat (error-text user) (error-text http))
        held       (pr-str query)
        held-ids   (into #{} (comp (map second) (filter integer?)) (scan-paths query))
        held-names (into #{}
                         (mapcat #(get card-columns %))
                         (concat held-ids (map (comp parse-long second) (re-seq #"card__(\d+)" held))))
        held-canaries (into #{} (map (juxt :table :id :kind)) (dataset/canary-hits query))
        found-in   (fn [re] (some #(when (string? %) (re-find re %)) texts))
        found      (concat
                    (for [{:keys [table column value why]} (:values secrets)
                          :when (and (not (str/includes? held value)) (found-in (whole-word value)))]
                      {:kind why :table table :column column :value value})
                    (for [{:keys [table id kind]} (dataset/canary-hits texts)
                          :let  [hit [table id kind]]
                          :when (not (or ((:visible-canaries secrets) hit) (held-canaries hit)))]
                      {:kind  :canary :table table :id id
                       :value (if (= kind :text) (dataset/canary-text table id) (str (dataset/canary-num table id)))})
                    (for [[col-name field-ids] hidden-names
                          :when (and (not (some held-ids field-ids))
                                     (not (held-names col-name))
                                     (not (str/includes? (u/upper-case-en held) (u/upper-case-en col-name)))
                                     (found-in (re-pattern (str "[\"`.]" (java.util.regex.Pattern/quote col-name) "(?![A-Za-z0-9_])"))))]
                      {:kind :hidden-column-name :value col-name}))]
    (cond
      (empty? texts) {:verdict :n/a}
      (seq found)    {:verdict :exposed :found (vec (take 6 (distinct found)))}
      :else          {:verdict :clean})))

(def ^:private compared-col-keys [:base_type :effective_type :semantic_type :binning_info])

(defn metadata
  "Compare the result columns the user got with the expected ones, by name, on the keys that describe a column's
  type and binning. Differences point at metadata that was merged in from the unsandboxed query."
  [user expected]
  (if (or (:error user) (:error expected))
    {:verdict :skipped}
    (let [by-name (fn [{:keys [body]}] (into {} (map (juxt :name #(select-keys % compared-col-keys))) (get-in body [:data :cols])))
          u       (by-name user)
          e       (by-name expected)
          diffs   (for [col (keys u) :when (and (contains? e col) (not= (u col) (e col)))]
                    {:col col :user (u col) :expected (e col)})]
      (if (seq diffs) {:verdict :differs :diffs (vec diffs)} {:verdict :same}))))

(def ^:private equivalence-col-keys [:name :base_type :effective_type :semantic_type :binning_info])

(defn http-equivalence
  "Looks at an HTTP entry point's response next to the processor's in-process result for the same query: the rows (as
  strings, since JSON numbers and processor numbers differ in type) and the columns' names and types must agree, and
  so must whether the query failed. A failure here is in the entry point's own context or post-processing, not in the
  processor. `:n/a` when the state had no HTTP transport."
  [http executed]
  (if (nil? http)
    {:verdict :n/a}
    (let [norm (fn [{:keys [rows body error]}]
                 (if error
                   {:error? true}
                   {:rows (frequencies (map #(mapv str %) rows))
                    :cols (mapv #(select-keys % equivalence-col-keys) (get-in body [:data :cols]))}))
          h    (norm http)
          e    (norm executed)]
      (if (= h e)
        {:verdict :same}
        {:verdict :differs :http (select-keys h [:error? :cols]) :executed (select-keys e [:error? :cols])
         :rows-differ? (not= (:rows h) (:rows e))}))))

(def outcomes
  "Every outcome a result can have, worst first, with its meaning and whether it fails a test. `:rejected` is judged by
  [[unexpected-outcome?]]: a hand-written probe that both users fail is a finding unless it is invalid on purpose."
  (array-map
   :canary-leak            {:fails? true  :doc "a canary of a hidden row is somewhere in the user's response"}
   :secret-in-error        {:fails? true  :doc "the user's error message carries a value the sandbox hides"}
   :raw-ref                {:fails? true  :doc "the preprocessed query reads the sandboxed table outside a sandbox stage"}
   :raw-table-in-sql       {:fails? true  :doc "the compiled SQL reads the sandboxed table outside the sandbox subquery"}
   :hidden-column-in-sql   {:fails? true  :doc "the compiled SQL reads a hidden column through a sandbox subquery that does not return it"}
   :dangling-ref           {:fails? true  :doc "the compiled SQL reads a column its subquery does not return: the rewrite left a ref unreconciled"}
   :binning-bounds-leak    {:fails? true  :doc "binning bounds come from the whole table's fingerprint, not the sandboxed rows"}
   :http-differs           {:fails? true  :doc "an HTTP entry point returned other rows or columns than the processor in-process"}
   :extra-rows             {:fails? true  :doc "the user got rows the admin on the restricted variant did not"}
   :missing-rows           {:fails? true  :doc "the user got fewer rows than the admin on the restricted variant"}
   :extra-columns          {:fails? true  :doc "the user got a column the admin on the restricted variant did not"}
   :missing-columns        {:fails? true  :doc "the user lost a column the admin on the restricted variant has"}
   :hidden-column-values   {:fails? true  :doc "a column the sandbox hides carries a value"}
   :missing-error          {:fails? true  :doc "a policy that must fail the query let it succeed with restricted data"}
   :over-restricted        {:fails? true  :doc "nothing should be restricted, yet the user got less than the admin"}
   :accidental-rejection   {:fails? true  :doc "a policy that hides columns failed the user's query, but not on purpose: nothing guarded it"}
   :user-error             {:fails? true  :doc "only the user's query failed, in a way no policy declares acceptable"}
   :hidden-name-in-error   {:fails? true  :doc "the user's error message names a column the sandbox hides, though the state passes otherwise"}
   :refused                {:fails? false :doc "a policy that hides columns refused the user's query on purpose"}
   :known-rejection        {:fails? false :doc "only the user's query failed, in a way the policy declares as failing closed"}
   :failed-as-expected     {:fails? false :doc "a policy that must fail the query did"}
   :rejected               {:fails? false :doc "both the user's and the admin's query failed: the probe is invalid"}
   :expected-error         {:fails? false :doc "only the admin's query on the restricted variant failed: the spec cannot answer"}
   :vacuous                {:fails? false :doc "right, but the sandbox could not have changed this answer"}
   :consistent             {:fails? false :doc "right, and the sandbox changed the answer"}
   :sandbox-count-mismatch {:fails? false :doc "the compiled SQL has another number of sandbox subqueries than sandbox stages (a warning)"}
   :unrenderable           {:fails? false :doc "the probe has no form in the requested query format"}
   :generator-error        {:fails? false :doc "the query generator failed, before any query ran"}))

(def failing-outcomes
  "The outcomes that fail a test; see [[outcomes]]."
  (into #{} (keep (fn [[k {:keys [fails?]}]] (when fails? k))) outcomes))

(defn outcome
  "One keyword for a result, worst first, from its separate verdicts; see [[outcomes]]."
  [{:keys [differential canary error-secrets structural sql sql-hidden-columns dangling-reads metadata-diff http-equivalence]}]
  (cond
    (= :leak (:verdict canary))                            :canary-leak
    (some #(not= :hidden-column-name (:kind %)) (:found error-secrets)) :secret-in-error
    (= :raw-ref (:verdict structural))                     :raw-ref
    (= :raw-table-in-sql (:verdict sql))                   :raw-table-in-sql
    ;; a probe the admin cannot run either is invalid (a forged model naming columns its query lacks), and its reads
    ;; are its own, not the rewrite's
    (and (= :hidden-column-read (:verdict sql-hidden-columns))
         (not (#{:rejected :expected-error} differential)))    :hidden-column-in-sql
    ;; a probe the admin cannot run either is invalid, and its dangling reads are its own, not the rewrite's
    (and (= :dangling (:verdict dangling-reads))
         (not (#{:rejected :expected-error} differential)))  :dangling-ref
    ;; bin bounds come from fingerprints; bounds that differ from the restricted data's reveal the hidden range
    (some (fn [{u :user e :expected}]
            (let [bounds #(select-keys (:binning_info %) [:min_value :max_value])]
              (and (seq (bounds u)) (seq (bounds e)) (not= (bounds u) (bounds e)))))
          (:diffs metadata-diff))
    :binning-bounds-leak
    (= :differs (:verdict http-equivalence))               :http-differs
    ;; a name, not data: it does not mask how the query failed
    (and (= :exposed (:verdict error-secrets))
         (not (failing-outcomes differential)))            :hidden-name-in-error
    :else                                                  differential))

;;; ---------------------------------------------------- Judging ---------------------------------------------------

(def registry
  "Invariant name -> fn of a judgement map, returning its verdict. The judgement map is everything known about one
  state:

    {:ctx          the policy context (see the harness namespace)
     :observations {:preprocessed {:value|:error} :compiled {:value|:error} :executed result}
     :query        the user's query, as sent
     :user         the user's executed result, with :failure-layer
     :expected     the admin's result on the restricted variant (or the base, for an :unrestricted policy)
     :base         the admin's result on the base dataset
     :http         an HTTP entry point's result, or nil}

  To add an invariant: write it as a fn of what it looks at, register it here, give its failing verdict an outcome in
  [[outcomes]], and place that outcome in [[outcome]]'s precedence."
  (array-map
   :differential       (fn [{:keys [ctx user expected base]}] (differential ctx user expected base))
   :canary             (fn [{:keys [ctx user expected]}] (canary ctx user expected))
   :error-secrets      (fn [{:keys [ctx query user http]}] (error-secrets ctx query user http))
   :structural         (fn [{:keys [ctx observations]}] (structural ctx (:preprocessed observations)))
   :dangling-reads     (fn [{:keys [ctx observations]}]
                         (sql-dangling-reads ctx (get-in observations [:compiled :value :query])))
   :sql                (fn [{:keys [ctx observations]}]
                         (sql ctx (:preprocessed observations) (get-in observations [:compiled :value :query])))
   :sql-hidden-columns (fn [{:keys [ctx observations]}]
                         (sql-hidden-columns ctx (get-in observations [:compiled :value :query])))
   ;; the API's response when there is one; the processor's result is a weaker stand-in
   :hygiene            (fn [{:keys [ctx user http]}] (hygiene ctx (or http user)))
   :metadata-diff      (fn [{:keys [user expected]}] (metadata user expected))
   :http-equivalence   (fn [{:keys [user http]}] (http-equivalence http user))))

(defn judge
  "Every registered invariant's verdict on `judgement`, plus the `:outcome` they fold into."
  [{:keys [observations] :as judgement}]
  (let [judgement (assoc-in judgement [:user :failure-layer] (failure-layer observations))
        verdicts  (into {} (for [[k f] registry] [k (f judgement)]))]
    (assoc verdicts :outcome (outcome verdicts) :failure-layer (get-in judgement [:user :failure-layer]))))

(defn unexpected-outcome?
  "A failing outcome, or a probe that both users fail without being invalid on purpose ([[hand-written/invalid?]])."
  [probe outcome]
  (boolean (or (failing-outcomes outcome)
               (and (= :rejected outcome) (not (hand-written/invalid? probe))))))

(def leak-outcomes
  "The failing outcomes that put data the sandbox hides, or a read of the raw table, in front of the user. A test that
  changes the policy itself out of band (a forged sandbox card) asserts only these: refusing or failing the query is
  the right answer to a broken policy."
  #{:canary-leak :secret-in-error :raw-ref :raw-table-in-sql :binning-bounds-leak :extra-rows :extra-columns :hidden-column-values})

(def ^:private masked-response-keys
  "Response keys whose values differ between two databases with the same data, or between two runs: ids, uuids,
  timings and the echoed query."
  #{:id :table_id :table-id :fk_target_field_id :fk-target-field-id :fk_field_id :fk-field-id :database_id :database
    :card-id :card_id :field-id :source-field :lib/card-id :lib/original-fk-field-id :source-table :source-card
    :field_ref :json_query :started_at :running_time :lib/uuid :query-hash
    ;; a failed result's exception details: frames and classes, not data
    :stacktrace :class :ex-data
    :metabase.query-processor.middleware.add-remaps/original-field-dimension-id
    :metabase.query-processor.middleware.add-remaps/new-field-dimension-id})

(defn- mask-ids-in-text
  "`s` with the ids and uuids that error messages echo replaced by `_`: the database id and query hash in SQL comments,
  and in printed queries uuids, source table and card ids, and the id ending an MBQL 5 field ref. Other numbers stay,
  since they may be leaked data."
  [s]
  (-> s
      (str/replace #"(databaseID|queryHash): \w+" "$1: _")
      ;; MariaDB prefixes its error messages with the connection id
      (str/replace #"\(conn=\d+\)" "(conn=_)")
      (str/replace #"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}" "_")
      (str/replace #"(:source-table|:source-card|:table-id|:card-id) \d+" "$1 _")
      (str/replace #"\} \d+\]" "} _]")))

(defn mask-response
  "`body` without [[masked-response-keys]], with the ids in field refs replaced by `_`, and with [[mask-ids-in-text]]
  applied to every string."
  [body]
  (walk/postwalk (fn [x]
                   (cond
                     (map? x)                                        (apply dissoc x masked-response-keys)
                     (and (sequential? x) (#{:field "field"} (first x))) (mapv #(if (int? %) '_ %) x)
                     (string? x)                                     (mask-ids-in-text x)
                     :else                                           x))
                 body))

(defn meaningful?
  "At least some of the cases compared rows that could have differed and found them right: 10 of them, or 30% of a
  shorter run (a run that stops at its first failure). A run that only ever errors, or only asks what the sandbox
  cannot change (`:vacuous`), proves nothing about it. The floor is a count rather than a share because how many
  cases are vacuous moves with every change to the dataset's columns."
  [outcomes]
  (let [consistent (count (filter #(= :consistent (:outcome %)) outcomes))
        total      (count outcomes)]
    (and (pos? total)
         (or (>= consistent 10)
             (>= consistent (* 0.3 total))))))

(ns metabase-enterprise.sandbox.enforcement-test.invariants
  "What must hold of a sandboxed user's response. Each invariant is a fn returning a verdict map, and every verdict is
  recorded on every result:

  * [[differential]] -- the user's columns and rows equal an admin's on the restricted variant (a sandbox is a view),
    or the user's query fails in a declared way.
  * [[canary]] -- no canary of a row the sandbox hides appears anywhere in the user's response.
  * [[structural]] -- after preprocessing as the user and alias resolution, no field ref outside a sandbox stage has a
    sandboxed table as its physical source.
  * [[hygiene]] -- the response carries no fingerprint, native or preprocessed form, internal stage, or SQL text.
  * [[metadata]] -- the result columns' types and binning equal the admin's.

  [[outcome]] folds the verdicts into one keyword, worst first; [[failing-outcomes]] says which fail a test.
  [[disagreements]] finds results where two invariants contradict each other, which is a bug in an invariant.
  [[mask-response]] prepares a response for the noninterference comparison."
  (:require
   [clojure.string :as str]
   [clojure.walk :as walk]
   [metabase-enterprise.sandbox.enforcement-test.dataset :as dataset]
   [metabase-enterprise.sandbox.enforcement-test.queries.formats :as formats]
   [metabase-enterprise.sandbox.enforcement-test.queries.hand-written :as hand-written]
   [metabase.lib.walk :as lib.walk]
   [metabase.query-processor.preprocess :as qp.preprocess]
   [metabase.query-processor.setup :as qp.setup]
   [metabase.query-processor.util.add-alias-info :as add]
   [metabase.test :as mt]
   [metabase.util :as u]
   [metabase.util.json :as json]
   [metabase.util.match :as match]))

(set! *warn-on-reflection* true)

;;; -------------------------------------------------- Detectors ---------------------------------------------------

(defn- project
  "The rows of `result` restricted to `col-names`, as a multiset."
  [{:keys [rows cols]} col-names]
  (let [idxs (keep #(let [i (.indexOf ^java.util.List cols %)] (when (nat-int? i) i)) col-names)]
    (frequencies (map (fn [row] (mapv #(nth row %) idxs)) rows))))

(defn- column-verdict
  "The columns the user got against the expected ones, as multisets of names with the hidden columns left out:
  `:extra-columns` when the user has a column (or a copy of one) the expected result lacks, `:missing-columns` when
  an expected column is gone, nil when they agree. The row comparison only means something once they agree."
  [hidden-names user expected]
  (let [visible    #(frequencies (remove hidden-names (:cols %)))
        user-m     (visible user)
        expected-m (visible expected)]
    (cond
      (some (fn [[col n]] (> n (get expected-m col 0))) user-m) :extra-columns
      (not= user-m expected-m)                                 :missing-columns)))

(defn- hidden-column-values?
  "Does `result` carry a value in a column the sandbox hides?"
  [hidden-names {:keys [cols rows]}]
  (boolean (some (fn [[i col]] (and (hidden-names col) (some #(some? (nth % i)) rows)))
                 (map-indexed vector cols))))

(defn differential
  "Compare the sandboxed user's result with the expected one (an admin on the restricted variant, or on the base
  database when nothing should be restricted). `admin-base` tells whether the probe could tell restricted data from
  unrestricted data at all. Column names are compared first, so a column that goes missing or appears cannot hide
  behind a row comparison; `:hidden-cols` holds the names of the columns the sandbox leaves out, which the user may
  omit or return empty. A failure of the user's query alone is `:user-error`, or `:known-rejection` when the config's
  `:accepted-rejections` patterns match it; only the latter is an accepted way to fail closed."
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
                                            (= expect :unrestricted)                                   :over-restricted
                                            (some #(re-find % (:error user)) accepted-rejections)      :known-rejection
                                            :else                                                      :user-error)
    :else
    (let [hidden-names (if (= expect :unrestricted) #{} hidden-cols)]
      (or (column-verdict hidden-names user expected)
          (when (hidden-column-values? hidden-names user) :hidden-column-values)
          (let [common     (remove hidden-names (:cols expected))
                user-m     (project user common)
                expected-m (project expected common)]
            (cond
              (= expect :unrestricted)
              (if (= user-m expected-m) :consistent :over-restricted)

              (= user-m expected-m)
              ;; the probe told restricted data from the rest when the rows differ from the unrestricted ones, or
              ;; when the unrestricted result showed a hidden column's values
              (if (and (= (project admin-base common) expected-m)
                       (not (hidden-column-values? hidden-names admin-base)))
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
  "Field refs in the user's query, outside any sandbox stage, whose physical source after alias resolution is one of
  `table-ids`. Runs as the current user. `{:refs [...]}` or `{:error msg}` when the query does not preprocess."
  [ids probe table-ids]
  (let [outer (json/decode+kw (json/encode (or (:outer probe) (formats/legacy-outer ids probe))))
        acc   (volatile! [])]
    (try
      (qp.setup/with-qp-setup [outer outer]
        (lib.walk/walk-stages
         (-> outer qp.preprocess/preprocess add/add-alias-info)
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
           nil)))
      {:refs @acc}
      (catch Throwable e
        {:error (u/truncate (ex-message e) 300)}))))

(defn structural
  "Field refs in the user's query that read a sandboxed table outside a sandbox stage; see [[raw-refs-to-tables]]."
  [{:keys [ids sandboxed]} probe]
  (let [table-ids (into #{} (map #(dataset/tid ids %)) sandboxed)
        {:keys [refs error]} (raw-refs-to-tables ids probe table-ids)]
    (cond
      error      {:verdict :error :error error}
      (seq refs) {:verdict :raw-ref :refs refs}
      :else      {:verdict :clean})))

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

(defn hygiene
  "What a sandboxed user's response must never carry, wherever it sits: a fingerprint, a native or preprocessed form,
  internal query stages, the physical name of a sandboxed table, or SQL text in an error. `:json_query` echoes the
  user's own input and is skipped."
  [{:keys [sandboxed]} user]
  (let [body        (let [b (:body user)] (if (map? b) (dissoc b :json_query :dataset_query) b))
        table-names (into #{} (map #(mt/format-name (name %))) sandboxed)
        problems    (concat
                     (for [[path v] (scan-paths body)
                           :when (and (some? v)
                                      (or (some #{:fingerprint "fingerprint"} path)
                                          (some #{:native :preprocessed :native_form "native_form"} path)))]
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

(defn outcome
  "One keyword for a result, worst first, from its separate verdicts."
  [{:keys [differential canary structural metadata-diff]}]
  (cond
    (= :leak (:verdict canary))         :canary-leak
    (= :raw-ref (:verdict structural))  :raw-ref
    ;; bin bounds come from fingerprints; bounds that differ from the restricted data's reveal the hidden range
    (some (fn [{u :user e :expected}]
            (let [bounds #(select-keys (:binning_info %) [:min_value :max_value])]
              (and (seq (bounds u)) (seq (bounds e)) (not= (bounds u) (bounds e)))))
          (:diffs metadata-diff))
    :binning-bounds-leak
    :else                               differential))

(def failing-outcomes
  "The outcomes that fail a test. `:rejected` is not among them: a hand-written probe that both users fail is judged
  by [[unexpected-outcome?]]."
  #{:canary-leak :raw-ref :binning-bounds-leak :extra-rows :missing-rows :extra-columns :missing-columns
    :hidden-column-values :missing-error :over-restricted :user-error})

(defn unexpected-outcome?
  "A failing outcome, or a hand-written probe that both users fail without being written to."
  [probe outcome]
  (boolean (or (failing-outcomes outcome)
               (and (= :rejected outcome) (not (hand-written/invalid probe))))))

(def ^:private masked-response-keys
  "Response keys whose values differ between two databases with the same data, or between two runs: ids, uuids,
  timings and the echoed query."
  #{:id :table_id :table-id :fk_target_field_id :fk-target-field-id :fk_field_id :fk-field-id :database_id :database
    :card-id :card_id :field-id :source-field :lib/card-id :lib/original-fk-field-id :source-table :source-card
    :field_ref :json_query :started_at :running_time :lib/uuid :query-hash
    :metabase.query-processor.middleware.add-remaps/original-field-dimension-id
    :metabase.query-processor.middleware.add-remaps/new-field-dimension-id})

(defn- mask-ids-in-text
  "`s` with the ids and uuids that error messages echo replaced by `_`: the database id and query hash in SQL comments,
  and in printed queries uuids, source table and card ids, and the id ending an MBQL 5 field ref. Other numbers stay,
  since they may be leaked data."
  [s]
  (-> s
      (str/replace #"(databaseID|queryHash): \w+" "$1: _")
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
  "At least some of the cases compared rows that could have differed and found them right. A run that only ever
  errors, or only asks what the sandbox cannot change (`:vacuous`), proves nothing about it."
  [outcomes]
  (let [counts (frequencies (map :outcome outcomes))
        total  (count outcomes)]
    (and (pos? total)
         (>= (get counts :consistent 0) (* 0.3 total)))))

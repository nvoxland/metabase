(ns metabase-enterprise.sandbox.enforcement-test.invariants.sql
  "The compiled-SQL structural invariant: a sandboxed table's physical name may appear in the SQL the driver gets only
  inside a derived table, the sandbox subquery, never where the outer query could read the raw table.

  [[placement]] is the rule on a SQL string: every occurrence of the table name as an identifier token, outside
  string literals and comments, must sit inside a parenthesized group that follows `FROM` or `JOIN` and opens with
  `SELECT`. [[structural-sql]] applies it to a result's compiled SQL and cross-checks the number of such subqueries
  against the number of sandbox stages in the preprocessed query.

  What it proves and what it does not: placement, not the filter. A derived table `(SELECT * FROM \"SHOPS\")` with no
  WHERE passes here and is caught by the differential and canary invariants. Known limitations: a string literal
  containing the table name is skipped by the literal scanner only for single-quoted literals; a quoted identifier
  containing a parenthesis would confuse the depth scan (none in the dataset); a persisted model's cache table has
  another name, so a leak through it is invisible here."
  (:require
   [clojure.string :as str]
   [metabase-enterprise.sandbox.query-processor.middleware.sandboxing :as sandboxing]
   [metabase.driver :as driver]
   [metabase.lib.walk :as lib.walk]
   [metabase.util :as u]))

(set! *warn-on-reflection* true)

(defn- strip-comments
  "SQL without its `-- ...` comment lines (Metabase prepends a userID/queryHash comment)."
  [sql]
  (str/replace sql #"(?m)^\s*--[^\n]*\n?" ""))

(defn- derived-depth
  "For each character offset of `sql`, how many enclosing parenthesized groups are derived tables: a group that follows
  `FROM` or `JOIN` and opens with `SELECT`. Single-quoted string literals are skipped. Returns a vector of ints and the
  open offsets of the derived groups, so an occurrence can name its innermost derived group."
  [^String sql]
  (let [n      (count sql)
        depths (int-array n)
        word-before (fn [i] ; the word that ends just before offset i, skipping whitespace
                      (let [j (loop [j (dec i)] (if (and (>= j 0) (Character/isWhitespace (.charAt sql j))) (recur (dec j)) j))
                            k (loop [k j] (if (and (>= k 0) (Character/isLetter (.charAt sql k))) (recur (dec k)) k))]
                        (u/upper-case-en (subs sql (inc k) (inc j)))))
        word-after  (fn [i] ; the word that starts at or after offset i, skipping whitespace
                      (let [j (loop [j i] (if (and (< j n) (Character/isWhitespace (.charAt sql j))) (recur (inc j)) j))
                            k (loop [k j] (if (and (< k n) (Character/isLetter (.charAt sql k))) (recur (inc k)) k))]
                        (u/upper-case-en (subs sql j k))))]
    (loop [i 0, stack [], groups [], in-string? false]
      (if (>= i n)
        {:depths (vec depths) :derived-groups groups}
        (let [c (.charAt sql i)]
          (aset depths i (int (count (filter :derived? stack))))
          (cond
            in-string?           (recur (inc i) stack groups (not= c \'))
            (= c \')             (recur (inc i) stack groups true)
            (= c \()             (let [before   (word-before i)
                                       after    (word-after (inc i))
                                       ;; a derived table, a CTE body, or a derived table that opens with a CTE
                                       derived? (or (and (#{"FROM" "JOIN"} before) (#{"SELECT" "WITH"} after))
                                                    (and (= "AS" before) (= "SELECT" after)))]
                                   (recur (inc i) (conj stack {:open i :derived? derived?}) (cond-> groups derived? (conj i)) false))
            (= c \))             (recur (inc i) (if (seq stack) (pop stack) stack) groups false)
            :else                (recur (inc i) stack groups false)))))))

(defn- innermost-derived-open
  "The open offset of the innermost derived group enclosing offset `i`, or nil."
  [^String sql i]
  ;; walk back counting parens until the enclosing derived group's `(` is found
  (loop [j (dec i), depth 0]
    (when (>= j 0)
      (let [c (.charAt sql j)]
        (cond
          (= c \)) (recur (dec j) (inc depth))
          (= c \() (if (pos? depth)
                     (recur (dec j) (dec depth))
                     (let [before (subs sql (max 0 (- j 8)) j)
                           after  (subs sql (inc j) (min (count sql) (+ j 12)))]
                       (if (or (and (re-find #"(?i)(FROM|JOIN)\s*$" before) (re-find #"(?i)^\s*(SELECT|WITH)" after))
                               (and (re-find #"(?i)\bAS\s*$" before) (re-find #"(?i)^\s*SELECT" after)))
                         j
                         (recur (dec j) 0))))
          :else    (recur (dec j) depth))))))

(defn placement
  "Where `table-name` (the driver's physical name, any quoting) appears in `sql` as an identifier token, outside
  comments and string literals: `{:inside n-derived-groups :raw [{:offset :context} ...]}`, where `:raw` lists the
  occurrences no derived table encloses. An occurrence right after `AS` is an alias, not a read, and is ignored."
  [sql table-name]
  (let [sql     (strip-comments sql)
        {:keys [depths]} (derived-depth sql)
        ;; the name as a whole identifier: bare, or exactly the contents of a quoted identifier. A quoted identifier that
        ;; merely contains it, like a join alias `"Shops - Parent Shop"`, is another name.
        quoted  (java.util.regex.Pattern/quote table-name)
        pattern (re-pattern (str "(?i)(?:(?<=[\"`\\[])" quoted "(?=[\"`\\]])|(?<![\\w\"`\\[])" quoted "(?![\\w\"`\\]]))"))
        m       (re-matcher pattern sql)
        in-literal? (fn [i] (odd? (count (filter #{\'} (subs sql 0 i)))))
        ;; a table read names the table in FROM or JOIN position, or qualifies it with a schema; an occurrence that
        ;; qualifies a column (`"Shops"."ID"`) names whatever `"Shops"` is in scope, usually a join alias
        table-read? (fn [i] (let [before (str/replace (subs sql 0 i) #"[\"`\[]$" "")]
                              (boolean (or (re-find #"(?i)(FROM|JOIN)\s+$" before)
                                           (re-find #"[\"`\]]\.$" before)))))
        hits    (loop [acc []]
                  (if (.find m)
                    (let [i (.start m)]
                      (recur (if (and (table-read? i) (not (in-literal? i)))
                               (conj acc i)
                               acc)))
                    acc))
        inside  (filter #(pos? (nth depths %)) hits)
        raw     (remove #(pos? (nth depths %)) hits)]
    {:inside (count (distinct (keep #(innermost-derived-open sql %) inside)))
     :raw    (vec (for [i raw]
                    {:offset i :context (subs sql (max 0 (- i 30)) (min (count sql) (+ i 30)))}))}))

(defn- sandbox-stage-count
  "How many stages of the preprocessed query are sandbox applications on `table-id`."
  [preprocessed table-id]
  (let [n (volatile! 0)]
    (lib.walk/walk-stages preprocessed
                          (fn [_query _path stage]
                            (when (and (::sandboxing/sandbox? stage)
                                       (= table-id (:query-permissions/sandboxed-table stage)))
                              (vswap! n inc))
                            nil))
    @n))

(defn structural-sql
  "Verdict on a result's compiled SQL for the sandboxed tables `sandboxed` (table keyword -> physical name -> table id):
  `:n/a` on a non-SQL driver, `:skipped` without SQL (the differential invariant reports the error),
  `:raw-table-in-sql` when the table is read outside a derived table, `:sandbox-count-mismatch` when the number of
  derived tables reading it differs from the number of sandbox stages (a warning, not a failure), else `:clean`."
  [{:keys [names table-ids]} preprocessed sql]
  (cond
    (not (isa? driver/hierarchy driver/*driver* :sql)) {:verdict :n/a}
    (not (string? sql))                                 {:verdict :skipped}
    :else
    (let [per-table (for [[table physical] names
                          :let [{:keys [inside raw]} (placement sql physical)
                                stages (when (:value preprocessed) (sandbox-stage-count (:value preprocessed) (table-ids table)))]]
                      {:table table :raw raw :inside inside :stages stages})
          raw       (mapcat :raw per-table)
          mismatch  (filter #(and (some? (:stages %)) (not= (:inside %) (:stages %))) per-table)]
      (cond
        (seq raw)      {:verdict :raw-table-in-sql :raw (vec raw)}
        (seq mismatch) {:verdict :sandbox-count-mismatch :tables (vec mismatch)}
        :else          {:verdict :clean}))))

;;; ------------------------------------------- Dangling reads in SQL ----------------------------------------------
;;;
;;; Ground truth for "is the rewrite in sync with what reads it": the compiled SQL. A qualified read `"alias"."COL"`
;;; through a derived table or CTE that does not return COL is a read the compiler emitted for a column its source no
;;; longer has; only the warehouse can refuse it.

(defn- matching-close
  "Offset of the `)` that closes the `(` at `open` in `sql`, skipping single-quoted literals."
  [^String sql open]
  (loop [i (inc open), depth 1, in-string? false]
    (when (< i (count sql))
      (let [c (.charAt sql i)]
        (cond
          in-string?                 (recur (inc i) depth (not= c \'))
          (= c \')                   (recur (inc i) depth true)
          (= c \()                   (recur (inc i) (inc depth) false)
          (and (= c \)) (= 1 depth)) i
          (= c \))                   (recur (inc i) (dec depth) false)
          :else                      (recur (inc i) depth false))))))

(defn- top-level-depths
  "Paren depth at each offset of `s`, single-quoted literals ignored."
  [^String s]
  (loop [i 0, depth 0, in-string? false, acc (transient [])]
    (if (>= i (count s))
      (persistent! acc)
      (let [c (.charAt s i)]
        (cond
          in-string? (recur (inc i) depth (not= c \') (conj! acc depth))
          (= c \')   (recur (inc i) depth true (conj! acc depth))
          (= c \()   (recur (inc i) (inc depth) false (conj! acc depth))
          (= c \))   (recur (inc i) (dec depth) false (conj! acc (dec depth)))
          :else      (recur (inc i) depth false (conj! acc depth)))))))

(defn- top-level-matches
  "Start offsets of `re` matches in `s` at paren depth zero."
  [re ^String s]
  (let [depths (top-level-depths s)
        m      (re-matcher re s)]
    (loop [acc []]
      (if (.find m)
        (recur (if (zero? (nth depths (.start m))) (conj acc (.start m)) acc))
        acc))))

(defn- item-name
  "The column name a select-list item produces: its alias, else its last identifier; nil for `*` (unknown)."
  [item]
  (let [item (str/trim item)]
    (cond
      (re-find #"\*\s*$" item) nil
      :else (let [[_ q u] (or (re-find #"(?i)\bAS\s+(?:[\"`]([^\"`]+)[\"`]|(\w+))\s*$" item)
                              (re-find #"(?:[\"`]([^\"`]+)[\"`]|(\w+))\s*$" item))]
              (or q u)))))

(defn- select-list-names
  "The upper-cased names the final top-level SELECT of `body` returns, or `:unknown` when its list has a `*`."
  [^String body]
  (let [select (last (top-level-matches #"(?i)\bSELECT\b" body))
        from   (when select (first (filter #(> % select) (top-level-matches #"(?i)\bFROM\b" body))))]
    (if-not select
      :unknown
      (let [list-text (subs body (+ select 6) (or from (count body)))
            commas    (top-level-matches #"," list-text)
            items     (map (fn [start end] (subs list-text start end))
                           (cons 0 (map inc commas))
                           (concat commas [(count list-text)]))
            names     (map item-name items)]
        (if (some nil? names)
          :unknown
          (into #{} (map u/upper-case-en) names))))))

(defn- re-seq-pos
  "Matches of `re` in `s` as maps with `:start`, `:end`, `:group` (group 1) and `:group3` (group 3, if any)."
  [re s]
  (let [m (re-matcher re s)]
    (loop [acc []]
      (if (.find m)
        (recur (conj acc {:start  (.start m)
                          :end    (.end m)
                          :group  (.group m 1)
                          :group3 (when (>= (.groupCount m) 3) (.group m 3))}))
        acc))))

(defn- alias-projections
  "Alias -> upper-cased set of column names it returns, or `:unknown`, for every derived table `(...) AS \"alias\"`
  and every CTE `\"alias\" (\"A\", \"B\") AS (...)` or `\"alias\" AS (...)` in `sql`. An alias defined more than once
  (nested `__mb_source`s) gets the union, or `:unknown` if any definition is; that can only hide a read, never invent
  one."
  [^String sql]
  (let [derived (for [m (re-seq-pos #"\)\s+AS\s+[\"`]([^\"`]+)[\"`]" sql)
                      :let [close (:start m)
                            open  (loop [i (dec close), depth 0]
                                    (when (>= i 0)
                                      (let [c (.charAt sql i)]
                                        (cond (= c \)) (recur (dec i) (inc depth))
                                              (= c \() (if (zero? depth) i (recur (dec i) (dec depth)))
                                              :else    (recur (dec i) depth)))))]
                      :when open]
                  [(:group m) (select-list-names (subs sql (inc open) close))])
        ctes    (for [m (re-seq-pos #"(?i)[\"`]([^\"`]+)[\"`]\s*(\(([^()]*)\))?\s+AS\s+\(" sql)
                      :let [open  (dec (:end m))
                            close (matching-close sql open)]
                      :when close]
                  [(:group m) (if-let [cols (:group3 m)]
                                (into #{} (map (comp u/upper-case-en second)) (re-seq #"[\"`]([^\"`]+)[\"`]" cols))
                                (select-list-names (subs sql (inc open) close)))])
        merge-def (fn [acc [a cols]]
                    (assoc acc a (if (or (= :unknown cols) (= :unknown (get acc a)))
                                   :unknown
                                   (into (get acc a #{}) cols))))
        defined   (reduce merge-def {} (concat derived ctes))
        ;; `FROM "__mb_stage_1" AS "__mb_source"`: an alias of a CTE returns what the CTE returns (Postgres)
        renamed   (for [[_ table alias] (re-seq #"(?i)\b(?:FROM|JOIN)\s+[\"`]([^\"`]+)[\"`]\s+AS\s+[\"`]([^\"`]+)[\"`]" sql)
                        :when (contains? defined table)]
                    [alias (get defined table)])]
    (reduce merge-def defined renamed)))

(defn dangling-reads
  "Qualified reads `\"alias\".\"COL\"` in `sql` through a derived table or CTE `alias` that does not return `COL`, each
  `{:alias :column :hidden?}`, `:hidden?` when `COL`, or its suffix after a `__` join-alias prefix, is one of
  `hidden-names`. Aliases whose column list cannot be read (a `*`) are skipped; reads through the raw table are
  [[placement]]'s business."
  [sql hidden-names]
  (let [sql         (strip-comments sql)
        projections (alias-projections sql)
        hidden      (into #{} (map u/upper-case-en) hidden-names)
        hidden?     (fn [col] (let [c (u/upper-case-en col)]
                                (boolean (or (contains? hidden c) (some #(str/ends-with? c (str "__" %)) hidden)))))]
    (vec (distinct
          (for [[_ alias col] (re-seq #"[\"`]([^\"`]+)[\"`]\.[\"`]([^\"`]+)[\"`]" sql)
                :let  [returned (get projections alias)]
                :when (and (set? returned) (not (contains? returned (u/upper-case-en col))))]
            {:alias alias :column col :hidden? (hidden? col)})))))

(defn hidden-column-reads
  "The [[dangling-reads]] of hidden columns, as `{:alias :column}`."
  [sql hidden-names]
  (into [] (comp (filter :hidden?) (map #(dissoc % :hidden?))) (dangling-reads sql hidden-names)))

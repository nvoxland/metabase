(ns metabase-enterprise.sandbox.corpus-test-dataset
  "A small synthetic dataset for sandboxing tests, built so that the sandboxed table SHOPS plays every role a table can
  play in a query: query source, explicit join target, implicit-join target through one FK (ORDERS.SHOP_ID), target of
  two FKs from one table (ORDERS.SHOP_ID and ORDERS.RETURN_SHOP_ID), end of a multi-hop chain (ITEMS -> ORDERS -> SHOPS),
  self-referencing (SHOPS.PARENT_SHOP_ID), source of an FK out (SHOPS.REGION_ID), and one half of a two-table cycle
  (REGIONS.HQ_SHOP_ID). Every table has ID and NAME, so name refs are ambiguous on purpose.

  Every SHOPS and REGIONS row carries two canary values that decode back to the row: a text token in SECRET and a
  number in SECRET_NUM. A canary from a row the sandbox hides, anywhere in a response the sandboxed user gets, is a
  leak. See [[canary-hits]].

  The dataset declares no foreign keys: the SQL test loader adds FK constraints before it loads rows, which would
  reject the cycle and the dangling keys of the restricted variants. [[ensure-fk-metadata!]] sets the FK metadata by
  hand after a dataset is loaded.

  The restricted variants ([[sandbox-corpus-tier1]] and friends) are the same dataset with a sandbox applied by hand:
  rows deleted or columns nulled. A query run as a sandboxed user on the base dataset must return what an admin gets
  from the matching variant."
  (:require
   [clojure.walk :as walk]
   [metabase.test :as mt]
   [metabase.test.data.interface :as tx]
   [toucan2.core :as t2]))

(set! *warn-on-reflection* true)

;;; ------------------------------------------------- Canaries -----------------------------------------------------

(def ^:private canary-nonce "5b0dca7e")
(def ^:private canary-num-step 7919)

(def ^:private canary-table-codes
  "Table -> [text code, numeric base]. The numeric base keeps the two tables' canaries apart and recognizable."
  {:shops   ["S" 900000000000]
   :regions ["R" 800000000000]})

(defn canary-text
  "The SECRET value of row `id` of `table`."
  [table id]
  (format "CANARY-%s%d-%s" (first (canary-table-codes table)) id canary-nonce))

(defn canary-num
  "The SECRET_NUM value of row `id` of `table`: the table's base plus the step times 2^id. Powers of two keep sums and
  averages of valid canaries (which result metadata computes over the returned rows) from decoding as another row."
  [table id]
  (+ (second (canary-table-codes table)) (* canary-num-step (bit-shift-left 1 id))))

(def ^:private canary-text-pattern (re-pattern (str "CANARY-([SR])(\\d+)-" canary-nonce)))

;; 12-digit numbers starting with 8 or 9, or the same printed as a double such as 9.00000007919E11
(def ^:private canary-num-pattern #"(?<![\d.])[89](?:\d{11}|\.\d+E11)(?![\d.])")

(def ^:private code->table (into {} (map (fn [[table [code _]]] [code table])) canary-table-codes))

(defn- decode-canary-num [s]
  (let [n (long (bigdec s))]
    (some (fn [[table [_ base]]]
            (let [d (- n base)]
              (when (and (pos? d) (< d (* 1000000 canary-num-step)) (zero? (mod d canary-num-step)))
                (let [q (quot d canary-num-step)]
                  (when (= 1 (Long/bitCount q))
                    [table (Long/numberOfTrailingZeros q)])))))
          canary-table-codes)))

(defn- canaries-in-string [s]
  (concat (for [[_ code id] (re-seq canary-text-pattern s)]
            [(code->table code) (parse-long id)])
          (keep decode-canary-num (re-seq canary-num-pattern s))))

(defn canary-hits
  "Every canary in `x`, a response body or any other value: a seq of `{:table t, :id n, :path p}` where `p` is the
  path to the string or number that held it. Scalars are checked as strings, so numbers printed as doubles and
  canaries inside error text or metadata are found too."
  [x]
  (let [hits (volatile! [])]
    (letfn [(scan [path v]
              (cond
                (map? v)        (doseq [[k child] v] (scan (conj path k) child))
                (sequential? v) (doall (map-indexed (fn [i child] (scan (conj path i) child)) v))
                (or (string? v) (number? v))
                (doseq [[table id] (canaries-in-string (str v))]
                  (vswap! hits conj {:table table :id id :path path}))
                :else           nil))]
      (scan [] (walk/keywordize-keys x)))
    @hits))

(defn canary-ids
  "The set of `[table id]` whose canaries appear anywhere in `x`."
  [x]
  (into #{} (map (juxt :table :id)) (canary-hits x)))

;;; -------------------------------------------------- Dataset -----------------------------------------------------

(defn- text [n] {:field-name n :base-type :type/Text})
(defn- int-col [n] {:field-name n :base-type :type/Integer})
(defn- pk [] {:field-name "id" :base-type :type/Integer :pk? true})
(defn- big [n] {:field-name n :base-type :type/BigInteger})

(defn- with-canaries
  "Append the canary columns' values to each row of `table`, where the row's id is its first value."
  [table rows]
  (mapv (fn [[id :as row]] (conj row (canary-text table id) (canary-num table id))) rows))

(mt/defdataset sandbox-corpus
  "See the namespace docstring. Columns in row order."
  [["regions"
    [(pk) (text "name") (int-col "tier") (int-col "hq_shop_id") (text "secret") (big "secret_num")]
    (with-canaries :regions
      [[1 "North" 1 1]
       [2 "South" 2 3]
       [3 "East"  1 5]
       [4 "West"  2 nil]])]
   ["shops"
    [(pk) (text "name") (int-col "tier") (int-col "price") (int-col "region_id") (int-col "parent_shop_id")
     (text "secret") (big "secret_num")]
    (with-canaries :shops
      [[1 "Alpha" 1 1 1   nil]
       [2 "Beta"  2 2 1   1]
       [3 "Gamma" 1 3 2   1]
       [4 "Delta" 2 1 2   2]
       [5 "Alpha" 1 2 3   3]
       [6 "Zeta"  2 3 3   4]
       [7 "Eta"   1 1 4   nil]
       [8 "Theta" 2 2 nil 7]])]
   ["orders"
    [(pk) (text "name") (int-col "shop_id") (int-col "return_shop_id") (int-col "amount")]
    [[1  "o1"  1 nil 10]
     [2  "o2"  2 1   20]
     [3  "o3"  3 nil 30]
     [4  "o4"  4 2   40]
     [5  "o5"  5 6   50]
     [6  "o6"  6 nil 60]
     [7  "o7"  7 8   70]
     [8  "o8"  8 nil 80]
     [9  "o9"  1 3   90]
     [10 "o10" 2 4   15]
     [11 "o11" 3 5   25]
     [12 "o12" 4 7   35]]]
   ["items"
    [(pk) (text "name") (int-col "order_id") (int-col "qty")]
    (mapv (fn [id order-id] [id (str "i" id) order-id id])
          (range 1 13)
          [1 1 2 3 4 5 6 7 8 9 10 12])]])

(def fks
  "Foreign keys of the dataset as `[from-table from-column to-table to-column]`, in the form [[ensure-fk-metadata!]]
  and query builders need."
  [[:orders  :shop_id        :shops   :id]
   [:orders  :return_shop_id :shops   :id]
   [:items   :order_id       :orders  :id]
   [:shops   :region_id      :regions :id]
   [:shops   :parent_shop_id :shops   :id]
   [:regions :hq_shop_id     :shops   :id]])

(defn ensure-fk-metadata!
  "Mark every entry of [[fks]] as a foreign key in the metadata of the current test database, the way sync would from
  a real constraint. Idempotent. Call it inside `mt/dataset` right after loading any variant of the corpus."
  []
  (doseq [[from-table from-col to-table to-col] fks]
    (t2/update! :model/Field (mt/id from-table from-col)
                {:semantic_type      :type/FK
                 :fk_target_field_id (mt/id to-table to-col)})))

;;; -------------------------------------------- Restricted variants -----------------------------------------------

(defn- row->map [{:keys [field-definitions]} row]
  (zipmap (map (comp keyword :field-name) field-definitions) row))

(defn- map->row [{:keys [field-definitions]} m]
  (mapv (comp m keyword :field-name) field-definitions))

(defn- keep-rows
  "Transform fn: keep only the rows of `table-name` for which `pred` of the row as a map is truthy."
  [table-name pred]
  (tx/transform-dataset-update-table
   table-name
   :table (fn [tabledef]
            (update tabledef :rows #(filterv (comp pred (partial row->map tabledef)) %)))))

(defn- null-columns
  "Transform fn: set `columns` to nil in every row of `table-name`."
  [table-name columns]
  (tx/transform-dataset-update-table
   table-name
   :table (fn [tabledef]
            (update tabledef :rows
                    #(mapv (fn [row] (map->row tabledef (merge (row->map tabledef row) (zipmap columns (repeat nil)))))
                           %)))))

(def tier-1-regions
  "IDs of the REGIONS rows with tier 1."
  #{1 3})

(def sandbox-corpus-tier1
  "SHOPS restricted to tier 1 rows."
  (tx/transformed-dataset-definition "sandbox-corpus-tier1" sandbox-corpus
                                     (keep-rows "shops" #(= 1 (:tier %)))))

(def sandbox-corpus-tier1-both
  "SHOPS and REGIONS both restricted to tier 1 rows."
  (tx/transformed-dataset-definition "sandbox-corpus-tier1-both" sandbox-corpus
                                     (keep-rows "shops" #(= 1 (:tier %)))
                                     (keep-rows "regions" #(= 1 (:tier %)))))

(def sandbox-corpus-region-tier1
  "SHOPS restricted to the shops whose region is tier 1, as a sandbox card that joins REGIONS would."
  (tx/transformed-dataset-definition "sandbox-corpus-region-tier1" sandbox-corpus
                                     (keep-rows "shops" #(contains? tier-1-regions (:region_id %)))))

(def ^:private hidden-shop-columns [:price :secret :secret_num])

(def sandbox-corpus-hide-price
  "Every SHOPS row, with PRICE and the canary columns hidden, as a sandbox card that leaves them out would. A hidden
  column reads as NULL here; Metabase may instead drop it or fail, which the classifier must allow for."
  (tx/transformed-dataset-definition "sandbox-corpus-hide-price" sandbox-corpus
                                     (null-columns "shops" hidden-shop-columns)))

(def sandbox-corpus-tier1-hide-price
  "Tier 1 SHOPS rows with PRICE and the canary columns hidden."
  (tx/transformed-dataset-definition "sandbox-corpus-tier1-hide-price" sandbox-corpus
                                     (keep-rows "shops" #(= 1 (:tier %)))
                                     (null-columns "shops" hidden-shop-columns)))

(defn shop-names-for-driver
  "`SHOPS` column names as the current driver stores them, keyed by their lower-case name. Postgres lower-cases,
  H2 upper-cases."
  []
  (into {} (for [col ["id" "name" "tier" "price" "region_id" "parent_shop_id" "secret" "secret_num"]]
             [(keyword col) (mt/format-name col)])))

(defn describe
  "A one-line summary of the loaded dataset, for log sections: table -> row count."
  []
  (into (sorted-map)
        (for [table [:regions :shops :orders :items]]
          [table (-> (mt/user-http-request :crowberto :post 202 "dataset"
                                           {:database (mt/id)
                                            :type     "query"
                                            :query    {:source-table (mt/id table) :aggregation [["count"]]}})
                     :data :rows ffirst)])))

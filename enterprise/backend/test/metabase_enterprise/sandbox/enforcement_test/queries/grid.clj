(ns metabase-enterprise.sandbox.enforcement-test.queries.grid
  "Every way to write a ref to a SHOPS column, in every clause position, from every stage that can reach SHOPS. A cell
  is `[edge ref-shape position target]`; [[probe-fn]] builds its probe and [[probe-names]] enumerates a selection. The
  runner is in the harness."
  (:require
   [metabase-enterprise.sandbox.enforcement-test.dataset :as dataset]
   [metabase.test :as mt]))

(set! *warn-on-reflection* true)

;;; ------------------------------------------------ Field-ref grid ------------------------------------------------
;;;
;;; Every way to write a ref to a SHOPS column, in every clause position, from every stage that can reach SHOPS.

(def edges
  "Edge name -> how a stage reaches SHOPS: the source table and the FK column, plus for the two-hop case the explicit
  join that brings the FK's table into scope."
  (array-map
   :orders/shop_id        {:source :orders  :fk-table :orders  :fk :shop_id}
   :orders/return_shop_id {:source :orders  :fk-table :orders  :fk :return_shop_id}
   :regions/hq_shop_id    {:source :regions :fk-table :regions :fk :hq_shop_id}
   :shops/parent_shop_id  {:source :shops   :fk-table :shops   :fk :parent_shop_id}
   :items->orders/shop_id {:source :items   :fk-table :orders  :fk :shop_id :via-join "O"}))

(defn- edge-joins
  "Explicit joins an edge's stage needs before the FK column is in scope."
  [ids {:keys [via-join]}]
  (when via-join
    [{:source-table (dataset/tid ids :orders)
      :alias        via-join
      :condition    [:= [:field (dataset/fid ids :items :order_id) nil] [:field (dataset/fid ids :orders :id) {:join-alias via-join}]]
      :fields       :none}]))

(defn- fk-opts [ids {:keys [fk-table fk via-join]}]
  (cond-> {:source-field (dataset/fid ids fk-table fk)}
    via-join (assoc :source-field-join-alias via-join)))

(def ^:private targets
  {:price      {:base-type :type/Integer    :param {:type "number/!=" :value [-1]}}
   :secret     {:base-type :type/Text       :param {:type "string/!=" :value ["__none__"]}}
   :rating     {:base-type :type/Float      :param {:type "number/!=" :value [-1]}}
   ;; the numeric canary: its hidden rows extend the column's range, which binning bounds would reveal
   :secret_num {:base-type :type/BigInteger :param {:type "number/!=" :value [-1]}}})

(def ref-shapes
  "Ref shape name -> fn of ids, edge and target returning a legacy ref to SHOPS.<target> from the edge's stage."
  (array-map
   :canonical                 (fn [ids e t] [:field (dataset/fid ids :shops t) (fk-opts ids e)])
   :legacy-fk->               (fn [ids e t] [:fk-> [:field-id (dataset/fid ids (:fk-table e) (:fk e))] [:field-id (dataset/fid ids :shops t)]])
   :name-ref+source-field     (fn [ids e t] [:field (dataset/fname ids :shops t) (merge {:base-type (get-in targets [t :base-type])} (fk-opts ids e))])
   :source-field-name-real    (fn [ids e t] [:field (dataset/fid ids :shops t) (assoc (fk-opts ids e) :source-field-name (dataset/fname ids (:fk-table e) (:fk e)))])
   :source-field-name-bogus   (fn [ids e t] [:field (dataset/fid ids :shops t) (assoc (fk-opts ids e) :source-field-name "NOPE")])
   :source-field-join-bogus   (fn [ids e t] [:field (dataset/fid ids :shops t) (assoc (fk-opts ids e) :source-field-join-alias "Nope")])
   :bare-id                   (fn [ids _ t] [:field (dataset/fid ids :shops t) nil])
   :bare-id+base-type         (fn [ids _ t] [:field (dataset/fid ids :shops t) {:base-type (get-in targets [t :base-type])}])
   :implicit-alias-no-join    (fn [ids e t] [:field (dataset/fid ids :shops t) {:join-alias (str (mt/format-name "shops") "__via__" (dataset/fname ids (:fk-table e) (:fk e)))}])
   :field-literal             (fn [ids _ t] [:field-literal (dataset/fname ids :shops t) (get-in targets [t :base-type])])
   :source-field-is-pk        (fn [ids e t] [:field (dataset/fid ids :shops t) {:source-field (dataset/fid ids (:fk-table e) :id)}])
   :source-field-from-shops   (fn [ids _ t] [:field (dataset/fid ids :shops t) {:source-field (dataset/fid ids :shops :region_id)}])
   ;; only ORDERS has a second FK into SHOPS; nil elsewhere, and the probe is skipped
   :source-field-other-fk     (fn [ids e t] (when (= :orders (:fk-table e))
                                              [:field (dataset/fid ids :shops t) {:source-field (dataset/fid ids :orders (if (= (:fk e) :shop_id) :return_shop_id :shop_id))}]))))

(defn- lib-binned
  "`r` with default binning, when it is a `:field` ref whose options can carry it."
  [[tag id-or-name opts :as r]]
  (if (and (= tag :field) ((some-fn nil? map?) opts))
    [tag id-or-name (assoc opts :binning {:strategy :default})]
    r))

(defn positions
  "Position name -> `{:query inner, :parameters ...}` for ids, edge, target and ref. Every query starts from the
  edge's source table with the edge's joins."
  [ids e t r]
  (let [src    (dataset/tid ids (:source e))
        base   (cond-> {:source-table src} (edge-joins ids e) (assoc :joins (edge-joins ids e)))
        canon  ((ref-shapes :canonical) ids e :name)
        own-id [:field (dataset/fid ids (:source e) :id) nil]]
    (array-map
     :fields            {:query (assoc base :fields [own-id r] :limit 50)}
     :breakout          {:query (assoc base :breakout [r] :aggregation [[:count]])}
     :filter            {:query (assoc base :aggregation [[:count]] :filter [:not-null r])}
     :aggregation       {:query (assoc base :aggregation [[:max r]])}
     :expression        {:query (assoc base :expressions {"x" [:coalesce r r]} :fields [own-id [:expression "x"]] :limit 50)}
     :case-expression   {:query (assoc base :expressions {"x" [:case [[[:not-null r] r]] {:default r}]} :fields [own-id [:expression "x"]] :limit 50)}
     :order-by          {:query (assoc base :fields [own-id r] :order-by [[:desc r]] :limit 50)}
     :mixed-after-canon {:query (assoc base :fields [canon r] :limit 50)}
     :mixed-before-canon {:query (assoc base :fields [r canon] :limit 50)}
     :second-stage      {:query {:source-query base :fields [r] :limit 50}}
     :explicit-join-present
     {:query (-> base
                 (update :joins (fnil conj [])
                         {:source-table (dataset/tid ids :shops)
                          :alias        "S"
                          :condition    [:= [:field (dataset/fid ids (:fk-table e) (:fk e)) (when (:via-join e) {:join-alias (:via-join e)})]
                                         [:field (dataset/fid ids :shops :id) {:join-alias "S"}]]
                          :fields       :none})
                 (assoc :fields [own-id r] :limit 50))
      :params-note "ref does not name the join"}
     :right-join-shops-preserved
     {:query (assoc base
                    :joins (conj (vec (:joins base))
                                 {:source-table (dataset/tid ids :shops)
                                  :alias        "S"
                                  :strategy     :right-join
                                  :condition    [:= [:field (dataset/fid ids (:fk-table e) (:fk e)) (when (:via-join e) {:join-alias (:via-join e)})]
                                                 [:field (dataset/fid ids :shops :id) {:join-alias "S"}]]
                                  :fields       :none})
                    :fields [own-id r [:field (dataset/fid ids :shops :secret) {:join-alias "S"}]]
                    :limit 50)}
     :join-condition
     {:query (assoc base
                    :joins (conj (vec (:joins base))
                                 {:source-table (dataset/tid ids :regions)
                                  :alias        "RG"
                                  :strategy     :inner-join
                                  :condition    [:and [:= [:field (dataset/fid ids :regions :id) {:join-alias "RG"}] [:field (dataset/fid ids :regions :id) {:join-alias "RG"}]]
                                                 [:not-null r]]
                                  :fields       :none})
                    :aggregation [[:count]])}
     :cumulative        {:query (assoc base :aggregation [[:cum-count]] :breakout [r])}
     :binning           {:query (assoc base :breakout [(if (#{:price :secret_num} t) (lib-binned r) r)] :aggregation [[:count]])}
     :parameter         {:query      (assoc base :aggregation [[:count]])
                         :parameters [(assoc (get-in targets [t :param]) :target [:dimension r])]})))

(defn probe-fn
  "A probe fn for one grid cell, or nil when the ref shape does not apply to the edge."
  [edge-k shape-k position-k target]
  (when ((ref-shapes shape-k) {:tables {} :fields {} :names {}} (edges edge-k) target)
    (fn [ids]
      (let [e (edges edge-k)
            r ((ref-shapes shape-k) ids e target)]
        (-> (positions ids e target r)
            (get position-k)
            (dissoc :params-note))))))

(def position-names
  "Every clause position of [[positions]], in the order the grid enumerates them."
  [:fields :breakout :filter :aggregation :expression :case-expression :order-by :mixed-after-canon
   :mixed-before-canon :second-stage :explicit-join-present :right-join-shops-preserved :join-condition :cumulative
   :binning :parameter])

(defn probe-names
  "Every `[edge shape position target]` of the grid, or those matching `selection` (a map of key -> seq)."
  ([] (probe-names {}))
  ([{:keys [edges* shapes positions* targets*]}]
   (for [edge-k     (or edges* (keys edges))
         shape-k    (or shapes (keys ref-shapes))
         position-k (or positions* position-names)
         target     (or targets* [:price :secret :rating])]
     [edge-k shape-k position-k target])))

(def ci-selection
  "The slice of the grid that runs in CI: every edge and ref shape, in the positions that take different code paths,
  for the numeric target."
  {:positions* [:fields :join-condition :second-stage :parameter]
   :targets*   [:price]})

(ns metabase-enterprise.sandbox.enforcement-test.queries.formats
  "The encodings a client can send one query in: legacy MBQL, MBQL 5 as Lib produces it, and MBQL 5 with legacy-ordered
  refs, which no client produces. [[render]] turns a probe into the request body for one format."
  (:require
   [clojure.walk :as walk]
   [metabase.legacy-mbql.normalize :as mbql.normalize]
   [metabase.lib.convert :as lib.convert]
   [metabase.util.json :as json]))

(set! *warn-on-reflection* true)

(defn legacy-outer
  "The legacy outer query for a probe's inner query and parameters against `ids`' database."
  [ids {:keys [query parameters]}]
  (cond-> {:database (:db ids) :type "query" :query query}
    parameters (assoc :parameters parameters)))

(defn- legacy->mbql5
  "`outer`, a legacy query as a client would send it, converted to MBQL 5 the way a client using Lib would send it."
  [outer]
  (-> (json/decode+kw (json/encode outer))
      mbql.normalize/normalize
      lib.convert/->mbql5
      (dissoc :lib.convert/converted?)))

(defn- legacy-ref-order
  "Every MBQL 5 `[:field opts id-or-name]` in `query` turned into the legacy order `[:field id-or-name opts]`: an MBQL 5
  query carrying legacy-shaped refs, which no client produces."
  [query]
  (walk/postwalk (fn [x]
                   (if (and (vector? x) (= 3 (count x)) (#{:field "field"} (first x)) (map? (second x)))
                     [(first x) (nth x 2) (second x)]
                     x))
                 query))

(def query-formats
  "Format name -> fn turning a legacy outer query into the request body sent in that format."
  (array-map
   :legacy      identity
   :mbql5       legacy->mbql5
   :mbql5-mixed (comp legacy-ref-order legacy->mbql5)))

(defn render
  "`probe` with `:outer`, its request body in query format `fmt`, or nil when it cannot be rendered in that format."
  [fmt ids probe]
  (if (= fmt :legacy)
    probe
    (try
      (assoc probe :outer ((query-formats fmt) (legacy-outer ids probe)))
      (catch Throwable _
        nil))))

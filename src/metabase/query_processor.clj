;; grandfathered two-segment ns; the canonical QP entrypoint name predates the module layout
#_{:clj-kondo/ignore [:metabase/namespace-name]}
(ns metabase.query-processor
  "Primary entrypoints to running Metabase (MBQL) queries.

    (metabase.query-processor/process-query {:type :query, :database 1, :query {:source-table 2}})

  Various REST API endpoints, such as `POST /api/dataset`, return the results of queries; they usually
  use [[userland-query]] or [[userland-query-with-default-constraints]] (see below)."
  (:refer-clojure :exclude [select-keys])
  (:require
   [medley.core :as m]
   [metabase.api.common :as api]
   [metabase.driver.util :as driver.u]
   [metabase.engine.routing :as engine.routing]
   [metabase.engine.shadow :as engine.shadow]
   [metabase.lib-be.core :as lib-be]
   [metabase.lib.metadata :as lib.metadata]
   [metabase.lib.metadata.protocols :as lib.metadata.protocols]
   [metabase.lib.schema.info :as lib.schema.info]
   [metabase.query-processor.compile :as qp.compile]
   [metabase.query-processor.debug :as qp.debug]
   [metabase.query-processor.execute :as qp.execute]
   [metabase.query-processor.middleware.catch-exceptions :as qp.catch-exceptions]
   [metabase.query-processor.middleware.enterprise :as qp.middleware.enterprise]
   [metabase.query-processor.middleware.process-userland-query :as qp.process-userland-query]
   [metabase.query-processor.postprocess :as qp.postprocess]
   [metabase.query-processor.preprocess :as qp.preprocess]
   [metabase.query-processor.reducible :as qp.reducible]
   [metabase.query-processor.schema :as qp.schema]
   [metabase.query-processor.setup :as qp.setup]
   [metabase.request.core :as request]
   [metabase.tracing.core :as tracing]
   [metabase.util.log :as log]
   [metabase.util.malli :as mu]
   [metabase.util.performance :refer [select-keys]]))

(declare process-query)

(defn- shadow-rerun
  "Run `query` again from scratch through one path, on the calling (fresh) thread, as the user who
  ran it (so permissions and sandboxing apply) and not as a userland query (so no QueryExecution
  row is saved for the rerun)."
  [query user-id force-mode]
  (binding [engine.routing/*force* force-mode]
    (request/with-current-user user-id
      (process-query (-> query
                         ;; a rerun must neither be served from the cache (the comparison would be vacuous) nor fill it
                         ;; (an engine result would then answer real requests while the database is only shadowed)
                         (dissoc :cache-strategy)
                         (assoc-in [:middleware :ignore-cached-results?] true)
                         (assoc-in [:middleware :userland-query?] false))))))

(defn- shadow-database
  "The query's database and a metadata provider for it: the one the query processor's setup already
  attached to the query when there is one, else the request's (cached) application-database one."
  [query]
  (let [mp (if (lib.metadata.protocols/metadata-providerable? query)
             (lib.metadata/->metadata-provider query)
             (lib-be/application-database-metadata-provider (:database query)))]
    [mp (lib.metadata/database mp)]))

(defn- engine-shadow-middleware
  "For databases in `shadow` query-engine mode: answer the query normally, then compare the JDBC
  and engine paths in the background. See `metabase.engine.shadow`."
  [qp]
  (fn [query rff]
    (let [result (qp query rff)]
      (when (and (not= (:type query) :internal)
                 (integer? (:database query)))
        (let [[mp database] (shadow-database query)
              user-id       api/*current-user-id*]
          (when (engine.routing/shadow? query mp database (driver.u/database->driver (:database query)))
            (engine.shadow/run-in-background!
             query
             (fn []
               (engine.shadow/compare-results (shadow-rerun query user-id :jdbc)
                                              (shadow-rerun query user-id :datafusion)))))))
      result)))

(def around-middleware
  "Middleware that goes AROUND [[process-query]]. Does extra stuff like handling `:internal` Audit v1 queries or saving
  QueryExecutions (userland queries only)

    (f qp) -> qp

  Where `qp` has the form

    (f query rff)"
  ;; think of the direction stuff happens in as if you were throwing a ball up in the air; as the query-ball goes up the
  ;; around middleware pre-processing stuff happens; then the query is executed, as the "ball of results" comes back
  ;; down any post-processing these around middlewares might do happens in reversed order.
  ;;
  ;; ↓↓↓ POST-PROCESSING ↓↓↓ happens from TOP TO BOTTOM
  [#'qp.middleware.enterprise/handle-audit-app-internal-queries-middleware
   #'engine-shadow-middleware
   #'qp.process-userland-query/process-userland-query-middleware
   ;; userland queries only: catch Exceptions and return a special error response
   #'qp.catch-exceptions/catch-exceptions])
;; ↑↑↑ PRE-PROCESSING ↑↑↑ happens from BOTTOM TO TOP

(defn- process-query** [query rff]
  (qp.debug/debug> (list `process-query query))
  (tracing/with-span :qp "qp.pipeline" {:query/type (some-> (:type query) name)
                                        :db/id      (:database query)}
    (let [preprocessed (tracing/with-span :qp "qp.preprocess" {}
                         (qp.preprocess/preprocess query))
          compiled     (qp.compile/attach-compiled-query preprocessed)
          rff          (qp.postprocess/post-processing-rff preprocessed rff)]
      (tracing/with-span :qp "qp.execute" {}
        (qp.execute/execute compiled rff)))))

(def ^:private ^{:arglists '([query rff])} process-query* nil)

(defn- rebuild-process-query-fn! []
  (alter-var-root #'process-query* (constantly
                                    (reduce
                                     (fn [qp middleware]
                                       (if middleware
                                         (middleware qp)
                                         qp))
                                     process-query**
                                     around-middleware))))

(rebuild-process-query-fn!)

(doseq [varr  around-middleware
        :when varr]
  (add-watch varr ::reload (fn [_key _ref _old-state _new-state]
                             (log/infof "%s changed, rebuilding %s" varr `process-query*)
                             (rebuild-process-query-fn!))))

(mu/defn process-query :- [:fn {:error/message "process-query unexpectedly returned nil."} some?]
  "Process an MBQL query. This is the main entrypoint to the magical realm of the Query Processor."
  ([query :- ::qp.schema/any-query]
   (process-query query nil))

  ([query :- ::qp.schema/any-query
    rff   :- [:maybe ::qp.schema/rff]]
   (tracing/with-span :qp "qp.process-query" {:query/type (some-> (:type query) name)
                                              :db/id      (:database query)}
     (qp.setup/with-qp-setup [query query]
       (let [rff (or rff qp.reducible/default-rff)]
         (process-query* query rff))))))

(mu/defn userland-query :- ::qp.schema/any-query
  "Add middleware options and `:info` to a `query` so it is ran as a 'userland' query, which slightly changes the QP
  behavior:

  1. Exceptions are caught, and a special error shape is returned (see [[catch-exceptions/catch-exceptions]])

  2. A `QueryExecution` is saved in the application database (see
     [[process-userland-query/process-userland-query-middleware]])

  3. A few extra keys like `:running_time` and `:started_at` are added to the QP
     response (see [[process-userland-query/process-userland-query-middleware]])"
  ([query :- ::qp.schema/any-query]
   (userland-query query nil))

  ([query :- ::qp.schema/any-query
    info  :- [:maybe ::lib.schema.info/info]]
   (-> query
       (assoc-in [:middleware :userland-query?] true)
       (cond-> info (update :info merge info)))))

(def ^:private userland-query-middleware-options
  #{:js-int-to-string? :ignore-cached-results?})

(mu/defn userland-query-with-default-constraints :- ::qp.schema/any-query
  "Add middleware options and `:info` to a `query` so it is ran as a 'userland' query. QP behavior changes are the same
  as those for [[userland-query]], *plus* the default userland constraints (limits) are applied --
  see [[qp.constraints/add-default-userland-constraints]].

  This ultimately powers most of the REST API entrypoints into the QP."
  ([query :- ::qp.schema/any-query]
   (userland-query-with-default-constraints query nil))

  ([query :- ::qp.schema/any-query
    info  :- [:maybe ::lib.schema.info/info]]
   (-> query
       (dissoc :constraints)
       (m/update-existing :middleware select-keys userland-query-middleware-options)
       (userland-query info)
       (assoc-in [:middleware :add-default-userland-constraints?] true))))
;; test

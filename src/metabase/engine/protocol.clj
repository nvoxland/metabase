(ns metabase.engine.protocol
  "The transport between Metabase and the native query engine. In production the engine is
  loaded in-process and called through the Foreign Function API; in development it runs as a
  sidecar. Both carry the same JSON request and response envelopes, so callers never know which
  one they are talking to."
  (:require
   [metabase.util.i18n :refer [tru]]))

(defprotocol Transport
  (-call [this request-json]
    "Send one JSON request envelope and return the JSON response envelope as a string. Never throws
    for engine-reported errors: those come back as an `err` envelope.")
  (-execute [this session-id sql params]
    "Plan and start `sql` in a session. Returns `{:reader ArrowReader, :cancel! fn, :close! fn}`.
    Throws for engine-reported errors. The caller must call `:close!` exactly once.")
  (-execute-raw [this session-id sql params]
    "Run warehouse-dialect `sql` in the session's database without planning it (Metabase's own
    metadata queries). Same return shape as `-execute`.")
  (-close [this]
    "Release whatever the transport holds (native handles, connections)."))

(defn engine-error->exception
  "An `ex-info` for an engine error map (`{:kind :error_type :message ...}`) whose `:type` is the
  matching `metabase.query-processor.error-type` keyword."
  [{:keys [kind error_type message] :as error}]
  (ex-info (or message (tru "The query engine reported an error."))
           (cond-> (assoc error
                          :type (keyword (or error_type "qp"))
                          :engine/error error)
             ;; the query processor reports a cancelled query quietly, as the JDBC path does
             (= kind "cancelled") (assoc :query/query-canceled? true))))

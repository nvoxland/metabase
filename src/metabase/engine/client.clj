(ns metabase.engine.client
  "Sidecar transport: the engine running as a separate process, reached over Arrow Flight. The
  `call` action carries the same JSON envelope as the in-process C ABI; `DoGet` streams a
  statement's batches for a session created through `call`."
  (:require
   [metabase.engine.arrow :as engine.arrow]
   [metabase.engine.protocol :as protocol]
   [metabase.util.i18n :refer [tru]]
   [metabase.util.json :as json])
  (:import
   (java.net URI)
   (java.nio.charset StandardCharsets)
   (org.apache.arrow.flight Action CallOption FlightClient FlightRuntimeException FlightStream Location Result Ticket)
   (org.apache.arrow.memory RootAllocator)))

(set! *warn-on-reflection* true)

(def ^:private no-options
  (into-array CallOption []))

(defn- location ^Location [url]
  (let [uri (URI. url)]
    (Location/forGrpcInsecure (.getHost uri) (if (neg? (.getPort uri)) 50051 (.getPort uri)))))

(defn- flight-error->exception
  "Engine errors travel as JSON in the gRPC status description."
  [^FlightRuntimeException e]
  (let [description (some-> e .status .description)
        error       (try
                      (when (and description (.startsWith ^String description "{"))
                        (json/decode+kw description))
                      (catch Exception _ nil))]
    (if (:error_type error)
      (protocol/engine-error->exception error)
      (ex-info (tru "The engine sidecar returned an error: {0}" (ex-message e)) {:status (str (.status e))} e))))

(defn- flight-batches
  "Batches from a `FlightStream`, with statement failures (which Flight reports on the first read)
  surfaced as engine errors."
  [^FlightStream stream]
  (reify engine.arrow/Batches
    (-schema [_] (.getSchema stream))
    (-root [_] (.getRoot stream))
    (-load-next! [_]
      (try
        (.next stream)
        (catch FlightRuntimeException e
          (throw (flight-error->exception e)))))))

(deftype FlightTransport [^FlightClient client ^RootAllocator allocator ^:volatile-mutable closed?]
  protocol/Transport
  (-call [_ request-json]
    (when closed?
      (throw (ex-info (tru "The engine sidecar connection has been closed.") {})))
    (try
      (let [results (.doAction client (Action. "call" (.getBytes ^String request-json StandardCharsets/UTF_8)) no-options)]
        (if (.hasNext results)
          (String. (.getBody ^Result (.next results)) StandardCharsets/UTF_8)
          (throw (ex-info (tru "The engine sidecar returned no response.") {}))))
      (catch FlightRuntimeException e
        (throw (flight-error->exception e)))))
  (-execute [_ session-id sql params]
    (when closed?
      (throw (ex-info (tru "The engine sidecar connection has been closed.") {})))
    (let [ticket (json/encode {:session_id session-id :sql sql :params (or params [])})
          stream (try
                   (.getStream client (Ticket. (.getBytes ^String ticket StandardCharsets/UTF_8)) no-options)
                   (catch FlightRuntimeException e
                     (throw (flight-error->exception e))))]
      {:reader  (flight-batches stream)
       :cancel! (fn [] (.cancel ^FlightStream stream "cancelled by Metabase" nil))
       :close!  (fn [] (.close ^FlightStream stream))}))
  (-close [_]
    (when-not closed?
      (set! closed? true)
      (.close client)
      (.close allocator))))

(defn transport
  "Connect to the engine sidecar at `url` (e.g. `http://localhost:50051`)."
  [url]
  (let [allocator (RootAllocator.)
        client    (.build (FlightClient/builder allocator (location url)))]
    (->FlightTransport client allocator false)))

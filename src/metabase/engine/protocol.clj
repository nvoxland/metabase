(ns metabase.engine.protocol
  "The transport between Metabase and the native query engine. In production the engine is
  loaded in-process and called through the Foreign Function API; in development it runs as a
  sidecar. Both carry the same JSON request and response envelopes, so callers never know which
  one they are talking to.")

(defprotocol Transport
  (-call [this request-json]
    "Send one JSON request envelope and return the JSON response envelope as a string. Never throws
    for engine-reported errors: those come back as an `err` envelope.")
  (-close [this]
    "Release whatever the transport holds (native handles, connections)."))

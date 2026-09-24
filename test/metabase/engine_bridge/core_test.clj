(ns metabase.engine-bridge.core-test
  "The JDBC bridge on its own: a Flight client fetching a ticket gets the JDBC rows as Arrow batches."
  (:require
   [clojure.test :refer :all]
   [metabase.engine-bridge.core :as engine-bridge]
   [metabase.engine.arrow :as engine.arrow]
   [metabase.test :as mt]
   [metabase.util.json :as json])
  (:import
   (java.nio.charset StandardCharsets)
   (org.apache.arrow.flight CallOption FlightClient FlightRuntimeException FlightStream Location Ticket)
   (org.apache.arrow.memory RootAllocator)))

(set! *warn-on-reflection* true)

(defn- do-with-bridge! [f]
  (engine-bridge/stop!)
  (let [{:keys [url token]} (engine-bridge/start! 0 "test-token")]
    (try
      (f url token)
      (finally
        (engine-bridge/stop!)))))

(defn- fetch-rows!
  "DoGet `ticket` from the bridge at `url`, returning `{:cols [names] :rows [...]}`."
  [url ticket]
  (let [uri (java.net.URI. url)]
    (with-open [allocator (RootAllocator.)
                client    (.build (FlightClient/builder allocator (Location/forGrpcInsecure (.getHost uri) (.getPort uri))))
                stream    (.getStream client (Ticket. (.getBytes ^String (json/encode ticket) StandardCharsets/UTF_8))
                                      (into-array CallOption []))]
      (let [batches  (reify engine.arrow/Batches
                       (-schema [_] (.getSchema ^FlightStream stream))
                       (-root [_] (.getRoot ^FlightStream stream))
                       (-load-next! [_] (.next ^FlightStream stream)))
            next-row (engine.arrow/row-thunk batches)]
        ;; the schema is only known after the first read
        {:rows (vec (take-while some? (repeatedly next-row)))
         :cols (mapv #(.getName ^org.apache.arrow.vector.types.pojo.Field %) (.getFields (.getSchema ^FlightStream stream)))}))))

(deftest ^:mb/driver-tests bridge-streams-jdbc-rows-test
  (mt/test-drivers #{:postgres}
    (do-with-bridge!
     (fn [url token]
       (testing "rows and column names come back, parameters are bound"
         (is (= {:cols ["id" "name"]
                 :rows [[1 "Red Medicine"] [2 "Stout Burgers & Beers"]]}
                (fetch-rows! url {:token       token
                                  :database_id (mt/id)
                                  :sql         "SELECT id, name FROM venues WHERE id <= ? ORDER BY id"
                                  :params      [{:type "Int64" :value 2}]
                                  :settings    {:time_zone "UTC" :read_only true}}))))
       (testing "temporal parameters bind as their java.time types"
         (is (= {:cols ["id"]
                 :rows [[1]]}
                (fetch-rows! url {:token       token
                                  :database_id (mt/id)
                                  :sql         "SELECT id FROM users WHERE last_login = ? AND CAST(last_login AS date) = ? AND CAST(last_login AS time) = ? ORDER BY id"
                                  :params      [{:type "Timestamp(Microsecond, None)" :value "2014-04-01T08:30:00"}
                                                {:type "Date32" :value "2014-04-01"}
                                                {:type "Time64(Microsecond)" :value "08:30:00"}]
                                  :settings    {:time_zone "UTC" :read_only true}}))))
       (testing "a bad token is refused with the engine's error envelope"
         (let [e (try (fetch-rows! url {:token "nope" :database_id (mt/id) :sql "SELECT 1" :params []})
                      nil
                      (catch FlightRuntimeException e e))]
           (is (some? e))
           (is (= "JDBC bridge token mismatch" (:message (json/decode+kw (.. ^FlightRuntimeException e status description)))))))
       (testing "a warehouse error is reported the same way"
         (let [e (try (fetch-rows! url {:token token :database_id (mt/id) :sql "SELECT * FROM no_such_table" :params []})
                      nil
                      (catch FlightRuntimeException e e))]
           (is (some? e))
           (is (re-find #"no_such_table" (:message (json/decode+kw (.. ^FlightRuntimeException e status description)))))))))))

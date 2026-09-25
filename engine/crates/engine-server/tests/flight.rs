//! Start the sidecar on an ephemeral port and talk to it the way Metabase's Flight client will.

use std::sync::Arc;

use arrow_flight::{Action, FlightClient};
use engine_core::{Engine, EngineConfig};
use futures::TryStreamExt;
use tokio::sync::oneshot;

async fn call(client: &mut FlightClient, json: &str) -> serde_json::Value {
    let action = Action::new("call", json.to_string());
    let results: Vec<_> = client
        .do_action(action)
        .await
        .unwrap()
        .try_collect()
        .await
        .unwrap();
    assert_eq!(results.len(), 1);
    serde_json::from_slice(&results[0]).unwrap()
}

/// The engine owns its runtime, so drive the test from that runtime (as `main` does) and let
/// the engine drop in a synchronous context afterwards.
#[test]
fn call_action_carries_the_same_envelope_as_the_c_abi() {
    let engine = Engine::new(EngineConfig::default()).unwrap();
    let test_engine = Arc::clone(&engine);
    engine
        .runtime()
        .block_on(async move { run(test_engine).await });
}

async fn run(engine: Arc<Engine>) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let (stop_tx, stop_rx) = oneshot::channel::<()>();
    let server = tokio::spawn(engine_server::serve(Arc::clone(&engine), listener, async {
        let _ = stop_rx.await;
    }));

    let channel = tonic::transport::Channel::from_shared(format!("http://{addr}"))
        .unwrap()
        .connect()
        .await
        .unwrap();
    let mut client = FlightClient::new(channel);

    let version = call(&mut client, r#"{"op":"version"}"#).await;
    assert_eq!(version["status"], "ok");
    assert_eq!(version["result"]["version"], env!("CARGO_PKG_VERSION"));

    let registered = call(
        &mut client,
        r#"{"op":"register_catalog","snapshot":{"database_id":42,"catalog_version":"v1","connector":"postgres",
            "tables":[{"schema":"public","name":"Orders","columns":[{"name":"ID","type":"Int64","nullable":false}]}]}}"#,
    )
    .await;
    assert_eq!(registered["status"], "ok");
    assert_eq!(registered["result"]["tables"], 1);

    let session = call(
        &mut client,
        r#"{"op":"create_session","context":{"database_id":42}}"#,
    )
    .await;
    let session_id = session["result"]["session_id"].as_u64().unwrap();

    let planned = call(
        &mut client,
        &format!(r#"{{"op":"plan_and_unparse","session_id":{session_id},"sql":"SELECT \"ID\" FROM \"public\".\"Orders\" LIMIT 1"}}"#),
    )
    .await;
    assert_eq!(planned["status"], "ok", "{planned}");
    assert_eq!(
        planned["result"]["warehouse_sql"],
        r#"SELECT "Orders"."ID" FROM "public"."Orders" LIMIT 1"#
    );

    let bad = call(
        &mut client,
        r#"{"op":"create_session","context":{"database_id":1}}"#,
    )
    .await;
    assert_eq!(bad["status"], "err");
    assert_eq!(bad["error"]["error_type"], "bad-configuration");

    let unknown = client.do_action(Action::new("nope", "")).await;
    assert!(unknown.is_err());

    let _ = stop_tx.send(());
    server.await.unwrap().unwrap();
}

/// `DoGet` end to end against the local development Postgres (skipped when unreachable).
#[test]
fn do_get_streams_query_results_for_a_session() {
    let engine =
        Engine::with_connectors(EngineConfig::default(), engine_server::connectors()).unwrap();
    let test_engine = Arc::clone(&engine);
    engine
        .runtime()
        .block_on(async move { run_do_get(test_engine).await });
}

async fn run_do_get(engine: Arc<Engine>) {
    use arrow_flight::Ticket;
    use futures::StreamExt;

    let details = serde_json::json!({"host": "localhost", "port": 5432, "dbname": "metabase_1",
                                     "user": "metabase", "password": "password", "ssl": false});
    let connector = engine_connector_postgres::PostgresConnector;
    {
        use engine_core::connector::Connector;
        let pool = connector
            .connect(&details, &Default::default())
            .await
            .unwrap();
        let probe = engine_core::connector::Query {
            sql: "SELECT 1".into(),
            params: vec![],
            settings: Default::default(),
            max_rows: None,
        };
        if connector.execute(pool.as_ref(), probe).await.is_err() {
            eprintln!("skipping: no Postgres reachable");
            return;
        }
    }

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let (stop_tx, stop_rx) = oneshot::channel::<()>();
    let server = tokio::spawn(engine_server::serve(Arc::clone(&engine), listener, async {
        let _ = stop_rx.await;
    }));
    let channel = tonic::transport::Channel::from_shared(format!("http://{addr}"))
        .unwrap()
        .connect()
        .await
        .unwrap();
    let mut client = FlightClient::new(channel);

    let registered = call(
        &mut client,
        &serde_json::json!({"op": "register_database", "registration": {
            "database_id": 7, "connector": "postgres", "details": details}})
        .to_string(),
    )
    .await;
    assert_eq!(registered["status"], "ok", "{registered}");
    let catalog = call(
        &mut client,
        r#"{"op":"register_catalog","snapshot":{"database_id":7,"catalog_version":"v1","connector":"postgres",
            "tables":[{"schema":"pg_catalog","name":"pg_tables","columns":[
              {"name":"schemaname","type":"Utf8"},{"name":"tablename","type":"Utf8"}]}]}}"#,
    )
    .await;
    assert_eq!(catalog["status"], "ok", "{catalog}");
    let session = call(
        &mut client,
        r#"{"op":"create_session","context":{"database_id":7}}"#,
    )
    .await;
    let session_id = session["result"]["session_id"].as_u64().unwrap();

    let ticket = serde_json::json!({"session_id": session_id,
        "sql": "SELECT \"schemaname\", \"tablename\" FROM \"pg_catalog\".\"pg_tables\" WHERE \"schemaname\" = $1 ORDER BY \"tablename\" LIMIT 3",
        "params": [{"type": "Utf8", "value": "pg_catalog"}]});
    let mut stream = client
        .do_get(Ticket::new(ticket.to_string()))
        .await
        .unwrap();
    let mut rows = 0;
    while let Some(batch) = stream.next().await {
        let batch = batch.unwrap();
        assert_eq!(batch.num_columns(), 2);
        rows += batch.num_rows();
    }
    assert_eq!(rows, 3);

    // A bad statement fails at DoGet with the engine's structured error in the status.
    let bad = serde_json::json!({"session_id": session_id, "sql": "SELECT nope FROM \"pg_catalog\".\"pg_tables\""});
    let err = match client.do_get(Ticket::new(bad.to_string())).await {
        Err(e) => e,
        Ok(_) => panic!("expected a plan error"),
    };
    assert!(err.to_string().contains("invalid-query"), "{err}");

    assert_eq!(
        engine.stream_count(),
        0,
        "streams are released when the Flight stream ends"
    );
    let _ = stop_tx.send(());
    server.await.unwrap().unwrap();
}

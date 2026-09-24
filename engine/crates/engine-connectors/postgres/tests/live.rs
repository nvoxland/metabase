//! Against a real Postgres. Set `ENGINE_TEST_POSTGRES_DETAILS` to a Metabase-style details JSON
//! to point elsewhere; the default is the local development server.

use std::sync::Arc;

use arrow::array::{Array, AsArray};
use arrow::datatypes::{DataType, Int64Type, TimeUnit};
use engine_connector_postgres::PostgresConnector;
use engine_core::api::{ParamValue, PoolConfig};
use engine_core::connector::{Connector, Query, SessionSettings};
use engine_core::error::ErrorKind;
use futures::TryStreamExt;

fn details() -> serde_json::Value {
    match std::env::var("ENGINE_TEST_POSTGRES_DETAILS") {
        Ok(json) => serde_json::from_str(&json).expect("ENGINE_TEST_POSTGRES_DETAILS is JSON"),
        Err(_) => serde_json::json!({"host": "localhost", "port": 5432, "dbname": "metabase_1",
                                     "user": "metabase", "password": "password", "ssl": false}),
    }
}

async fn pool() -> Option<Arc<dyn engine_core::connector::Pool>> {
    let connector = PostgresConnector;
    let pool = connector
        .connect(&details(), &PoolConfig::default())
        .await
        .unwrap();
    match connector
        .execute(pool.as_ref(), query("SELECT 1", vec![]))
        .await
    {
        Ok(_) => Some(pool),
        Err(e) => {
            eprintln!("skipping: no Postgres reachable ({})", e.message);
            None
        }
    }
}

fn query(sql: &str, params: Vec<ParamValue>) -> Query {
    Query {
        sql: sql.into(),
        params,
        settings: SessionSettings {
            time_zone: Some("UTC".into()),
            statement_timeout_ms: Some(30_000),
            role: None,
            read_only: true,
            application_name: "engine-test".into(),
        },
    }
}

#[tokio::test]
async fn types_decode_to_arrow_and_parameters_bind() {
    let Some(pool) = pool().await else { return };
    let connector = PostgresConnector;
    let sql = "SELECT 42::int8 AS big, 7::int4 AS small, 1.5::float8 AS f, 12.345::numeric AS n, 'hi'::text AS t,
                      true AS b, DATE '2024-02-03' AS d, TIMESTAMP '2024-02-03 04:05:06.123456' AS ts,
                      TIMESTAMPTZ '2024-02-03 04:05:06+02' AS tstz, TIME '04:05:06' AS tm,
                      '{\"a\":1}'::jsonb AS j, NULL::int8 AS nothing,
                      INTERVAL '1 day' AS iv, ARRAY[1,2]::int4[] AS arr, $1::int8 + 1 AS p";
    let handle = connector
        .execute(
            pool.as_ref(),
            query(
                sql,
                vec![ParamValue {
                    arrow_type: "Int64".into(),
                    value: 9.into(),
                }],
            ),
        )
        .await
        .unwrap();
    let schema = handle.stream.schema();
    let batches: Vec<_> = handle.stream.try_collect().await.unwrap();
    assert_eq!(batches.len(), 1);
    let batch = &batches[0];
    assert_eq!(batch.num_rows(), 1);
    let field = |name: &str| schema.field_with_name(name).unwrap().data_type().clone();
    assert_eq!(field("big"), DataType::Int64);
    assert_eq!(field("small"), DataType::Int32);
    assert_eq!(field("f"), DataType::Float64);
    assert_eq!(field("n"), DataType::Decimal128(38, 10));
    assert_eq!(field("t"), DataType::Utf8);
    assert_eq!(field("b"), DataType::Boolean);
    assert_eq!(field("d"), DataType::Date32);
    assert_eq!(
        field("ts"),
        DataType::Timestamp(TimeUnit::Microsecond, None)
    );
    assert_eq!(
        field("tstz"),
        DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()))
    );
    assert_eq!(field("tm"), DataType::Time64(TimeUnit::Microsecond));
    assert_eq!(field("j"), DataType::Utf8);
    assert_eq!(field("iv"), DataType::Utf8, "interval is cast to text");
    assert_eq!(field("arr"), DataType::Utf8, "arrays are cast to text");
    assert_eq!(field("p"), DataType::Int64);

    let col = |name: &str| batch.column(schema.index_of(name).unwrap()).clone();
    assert_eq!(col("big").as_primitive::<Int64Type>().value(0), 42);
    assert_eq!(col("p").as_primitive::<Int64Type>().value(0), 10);
    assert!(col("nothing").is_null(0));
    assert_eq!(col("t").as_string::<i32>().value(0), "hi");
    assert_eq!(col("iv").as_string::<i32>().value(0), "1 day");
    assert_eq!(col("arr").as_string::<i32>().value(0), "{1,2}");
    let tstz = col("tstz");
    let micros = tstz
        .as_primitive::<arrow::datatypes::TimestampMicrosecondType>()
        .value(0);
    // 2024-02-03T02:05:06Z
    assert_eq!(micros, 1_706_925_906_000_000);
    let n = col("n");
    let mantissa = n
        .as_primitive::<arrow::datatypes::Decimal128Type>()
        .value(0);
    assert_eq!(mantissa, 123_450_000_000);
}

#[tokio::test]
async fn read_only_is_enforced_by_the_server() {
    let Some(pool) = pool().await else { return };
    let connector = PostgresConnector;
    let err = match connector
        .execute(
            pool.as_ref(),
            query("CREATE TEMP TABLE engine_should_not (x int)", vec![]),
        )
        .await
    {
        Err(e) => e,
        Ok(handle) => handle
            .stream
            .try_collect::<Vec<_>>()
            .await
            .unwrap_err()
            .into(),
    };
    let err: engine_core::EngineError = err;
    assert_eq!(err.sql_state.as_deref(), Some("25006"), "{}", err.message);
}

#[tokio::test]
async fn statement_timeout_maps_to_timed_out() {
    let Some(pool) = pool().await else { return };
    let connector = PostgresConnector;
    let mut q = query("SELECT pg_sleep(5)", vec![]);
    q.settings.statement_timeout_ms = Some(200);
    let err = match connector.execute(pool.as_ref(), q).await {
        Err(e) => e,
        Ok(handle) => handle
            .stream
            .try_collect::<Vec<_>>()
            .await
            .unwrap_err()
            .into(),
    };
    let err: engine_core::EngineError = err;
    assert_eq!(err.kind, ErrorKind::Timeout, "{}", err.message);
    assert_eq!(err.error_type, "timed-out");
}

#[tokio::test]
async fn cancel_stops_a_running_statement_and_returns_the_connection() {
    let Some(pool) = pool().await else { return };
    let connector = PostgresConnector;
    let handle = connector
        .execute(
            pool.as_ref(),
            query(
                "SELECT n FROM generate_series(1, 5000000) AS n, pg_sleep(2)",
                vec![],
            ),
        )
        .await
        .unwrap();
    let cancel = handle.cancel.clone();
    tokio::spawn(async move {
        tokio::time::sleep(std::time::Duration::from_millis(300)).await;
        cancel.cancel();
    });
    let result: Result<Vec<_>, _> = handle.stream.try_collect().await;
    let err: engine_core::EngineError = result.unwrap_err().into();
    assert_eq!(err.kind, ErrorKind::Cancelled, "{}", err.message);
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    assert_eq!(
        pool.stats().available,
        pool.stats().size,
        "connection returned to the pool"
    );
}

#[tokio::test]
async fn streams_a_million_rows_in_batches() {
    let Some(pool) = pool().await else { return };
    let connector = PostgresConnector;
    let handle = connector
        .execute(
            pool.as_ref(),
            query(
                "SELECT n, n * 2 AS twice FROM generate_series(1, 1000000) AS n",
                vec![],
            ),
        )
        .await
        .unwrap();
    let mut rows = 0usize;
    let mut batches = 0usize;
    let mut stream = handle.stream;
    while let Some(batch) = stream.try_next().await.unwrap() {
        rows += batch.num_rows();
        batches += 1;
        assert!(batch.num_rows() <= 8192);
    }
    assert_eq!(rows, 1_000_000);
    assert!(batches >= 122, "{batches} batches");
}

#[tokio::test]
async fn dropping_a_running_stream_from_a_plain_thread_is_safe() {
    let Some(pool) = pool().await else { return };
    let connector = PostgresConnector;
    let handle = connector
        .execute(
            pool.as_ref(),
            query("SELECT n FROM generate_series(1, 1000000) AS n", vec![]),
        )
        .await
        .unwrap();
    let mut stream = handle.stream;
    let first = stream.try_next().await.unwrap();
    assert!(first.is_some());
    // The JVM releases Arrow streams from its own threads, which have no tokio context.
    std::thread::spawn(move || drop(stream)).join().unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;
    assert_eq!(
        pool.stats().available,
        pool.stats().size,
        "connection returned to the pool"
    );
}

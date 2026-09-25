//! Against a real MySQL or MariaDB: `ENGINE_TEST_MYSQL_DETAILS` (the connector's JSON details), or
//! the local development server (`localhost:3306`, database `lakes`, user `root`, no password).
//! Skipped when none is reachable, unless `MB_ENGINE_TESTS_REQUIRED=true`.

use std::sync::Arc;

use arrow::array::{Array, AsArray};
use arrow::datatypes::{DataType, TimeUnit, TimestampMicrosecondType};
use engine_connector_mysql::MySqlConnector;
use engine_core::api::{ParamValue, PoolConfig};
use engine_core::connector::{Connector, Query, SessionSettings};
use engine_core::error::ErrorKind;
use futures::TryStreamExt;

fn details() -> serde_json::Value {
    match std::env::var("ENGINE_TEST_MYSQL_DETAILS") {
        Ok(json) => serde_json::from_str(&json).expect("ENGINE_TEST_MYSQL_DETAILS is JSON"),
        Err(_) => serde_json::json!({"host": "localhost", "port": 3306, "dbname": "lakes",
                                     "user": "root", "password": "", "ssl": false}),
    }
}

async fn pool() -> Option<Arc<dyn engine_core::connector::Pool>> {
    let connector = MySqlConnector;
    match connector.connect(&details(), &PoolConfig::default()).await {
        Ok(pool) => Some(pool),
        Err(e) if std::env::var("MB_ENGINE_TESTS_REQUIRED").is_ok_and(|v| v == "true") => {
            panic!(
                "MB_ENGINE_TESTS_REQUIRED but no MySQL reachable ({})",
                e.message
            )
        }
        Err(e) => {
            eprintln!("skipping: no MySQL reachable ({})", e.message);
            None
        }
    }
}

fn query(sql: &str, params: Vec<ParamValue>) -> Query {
    Query {
        sql: sql.to_string(),
        params,
        settings: SessionSettings {
            time_zone: Some("America/Los_Angeles".into()),
            statement_timeout_ms: Some(30_000),
            role: None,
            read_only: true,
            application_name: "metabase-test".into(),
        },
        max_rows: None,
    }
}

#[tokio::test]
async fn a_row_cap_stops_the_statement_and_keeps_the_connection() {
    let Some(pool) = pool().await else { return };
    let mut q = query(
        "SELECT c1.column_name FROM information_schema.columns c1, information_schema.columns c2, information_schema.columns c3",
        vec![],
    );
    q.max_rows = Some(10);
    let started = std::time::Instant::now();
    let handle = MySqlConnector.execute(pool.as_ref(), q).await.unwrap();
    let batches: Vec<_> = handle.stream.try_collect().await.unwrap();
    assert_eq!(batches.iter().map(|b| b.num_rows()).sum::<usize>(), 10);
    assert!(
        started.elapsed() < std::time::Duration::from_secs(5),
        "took {:?}",
        started.elapsed()
    );
    // the connection goes back to the pool and serves the next statement
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    let before = pool.stats();
    let again = MySqlConnector
        .execute(pool.as_ref(), query("SELECT 1 AS one", vec![]))
        .await
        .unwrap();
    let batches: Vec<_> = again.stream.try_collect().await.unwrap();
    assert_eq!(batches[0].num_rows(), 1);
    assert!(before.available >= 1, "{before:?}");
}

fn display(column: &dyn Array, row: usize) -> String {
    arrow::util::display::array_value_to_string(column, row).unwrap()
}

fn param(arrow_type: &str, value: serde_json::Value) -> ParamValue {
    ParamValue {
        arrow_type: arrow_type.into(),
        value,
    }
}

async fn rows(
    pool: &dyn engine_core::connector::Pool,
    q: Query,
) -> Vec<arrow::record_batch::RecordBatch> {
    let handle = MySqlConnector.execute(pool, q).await.unwrap();
    handle.stream.try_collect().await.unwrap()
}

#[tokio::test]
async fn types_come_back_as_arrow() {
    let Some(pool) = pool().await else { return };
    let batches = rows(
        pool.as_ref(),
        query(
            "SELECT 1 AS i, CAST(1.5 AS DECIMAL(10, 2)) AS d, 2.5 AS f, 'x' AS s, TRUE AS b, \
             DATE '2024-02-03' AS dt, TIMESTAMP '2024-02-03 04:05:06' AS ts, TIME '13:30:00' AS t, \
             CAST(NULL AS CHAR) AS n, FROM_UNIXTIME(86400) AS epoch",
            vec![],
        ),
    )
    .await;
    let batch = &batches[0];
    let schema = batch.schema();
    assert!(matches!(
        schema.field(0).data_type(),
        DataType::Int32 | DataType::Int64
    ));
    assert_eq!(schema.field(1).data_type(), &DataType::Decimal128(38, 2));
    assert_eq!(schema.field(2).data_type(), &DataType::Decimal128(38, 1));
    assert_eq!(schema.field(3).data_type(), &DataType::Utf8);
    assert_eq!(schema.field(5).data_type(), &DataType::Date32);
    assert_eq!(
        schema.field(6).data_type(),
        &DataType::Timestamp(TimeUnit::Microsecond, None)
    );
    assert_eq!(
        schema.field(7).data_type(),
        &DataType::Time64(TimeUnit::Microsecond)
    );
    assert_eq!(display(batch.column(0), 0), "1");
    assert_eq!(batch.column(3).as_string::<i32>().value(0), "x");
    assert!(batch.column(8).is_null(0));
    // FROM_UNIXTIME is shown in the session zone (Los Angeles, 1970-01-01 16:00) and typed
    // TIMESTAMP, which decodes back to the instant
    assert_eq!(
        schema.field(9).data_type(),
        &DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()))
    );
    let epoch = batch.column(9).as_primitive::<TimestampMicrosecondType>();
    assert_eq!(epoch.value(0), 86_400_000_000);
}

#[tokio::test]
async fn parameters_bind_in_order_with_repeats() {
    let Some(pool) = pool().await else { return };
    let batches = rows(
        pool.as_ref(),
        query(
            "SELECT $2 + $1 + $2 AS s, $3 AS t",
            vec![
                param("Int64", serde_json::json!(1)),
                param("Int64", serde_json::json!(10)),
                param("Utf8", serde_json::json!("it's")),
            ],
        ),
    )
    .await;
    let batch = &batches[0];
    assert_eq!(display(batch.column(0), 0), "21");
    assert_eq!(batch.column(1).as_string::<i32>().value(0), "it's");
}

#[tokio::test]
async fn backslashes_in_literals_are_literal() {
    let Some(pool) = pool().await else { return };
    let batches = rows(
        pool.as_ref(),
        query(r#"SELECT 'a\' AS s, CHAR_LENGTH('\n') AS n"#, vec![]),
    )
    .await;
    let batch = &batches[0];
    assert_eq!(batch.column(0).as_string::<i32>().value(0), "a\\");
    assert_eq!(display(batch.column(1), 0), "2");
}

#[tokio::test]
async fn writes_are_refused_and_errors_carry_the_server_message() {
    let Some(pool) = pool().await else { return };
    // the failure may surface when the statement starts or on its first batch
    let err = match MySqlConnector
        .execute(
            pool.as_ref(),
            query("CREATE TABLE engine_should_not_create (x INT)", vec![]),
        )
        .await
    {
        Err(e) => e,
        Ok(handle) => {
            let e = handle
                .stream
                .try_collect::<Vec<_>>()
                .await
                .expect_err("a write in a read-only session");
            engine_core::error::EngineError::from(e)
        }
    };
    assert_eq!(err.kind, ErrorKind::Warehouse, "{}", err.message);
    assert!(
        err.message.to_lowercase().contains("read only")
            || err.message.to_lowercase().contains("read-only"),
        "{}",
        err.message
    );
}

#[tokio::test]
async fn a_long_statement_is_stopped_by_the_time_limit() {
    let Some(pool) = pool().await else { return };
    let mut q = query("SELECT SLEEP(5)", vec![]);
    q.settings.statement_timeout_ms = Some(500);
    let outcome = match MySqlConnector.execute(pool.as_ref(), q).await {
        Err(e) => e,
        Ok(handle) => {
            let e = handle
                .stream
                .try_collect::<Vec<_>>()
                .await
                .expect_err("should time out");
            engine_core::error::EngineError::from(e)
        }
    };
    assert_eq!(outcome.kind, ErrorKind::Timeout, "{}", outcome.message);
}

#[tokio::test]
async fn cancelling_stops_a_streaming_statement() {
    // A statement can only be cancelled once its first result packet has arrived: MySQL sends the
    // column definitions after it starts executing, and `execute` waits for them. A statement
    // that spends its time before that (a long `SLEEP`) runs to its time limit; the engine has to
    // hand out stream ids before the connector's start completes to do better.
    use futures::StreamExt;
    let Some(pool) = pool().await else { return };
    let handle = MySqlConnector
        .execute(
            pool.as_ref(),
            query(
                "SELECT c1.column_name, c2.column_name, c3.table_name \
                 FROM information_schema.columns c1, information_schema.columns c2, information_schema.columns c3 \
                 LIMIT 50000000",
                vec![],
            ),
        )
        .await
        .unwrap();
    let mut stream = handle.stream;
    let first = stream.next().await.expect("a first batch");
    assert!(first.is_ok(), "{first:?}");
    handle.cancel.cancel();
    let started = std::time::Instant::now();
    let mut ended_with_error = false;
    while let Some(item) = stream.next().await {
        if item.is_err() {
            ended_with_error = true;
            break;
        }
    }
    assert!(ended_with_error, "a cancelled statement ends with an error");
    assert!(started.elapsed() < std::time::Duration::from_secs(5));
}

#[tokio::test]
async fn an_unknown_report_zone_runs_the_session_in_utc() {
    let Some(pool) = pool().await else { return };
    let mut q = query("SELECT @@session.time_zone AS tz", vec![]);
    q.settings.time_zone = Some("Nowhere/Land".into());
    let batches = rows(pool.as_ref(), q).await;
    assert_eq!(batches[0].column(0).as_string::<i32>().value(0), "+00:00");
}

//! The sqllogictest harness: the same `.slt` files run against DataFusion executing locally (the
//! oracle: what the engine's dialect *means*) and against the engine planning, unparsing, and
//! executing on a warehouse (the candidate). A difference is a semantic drift between the two,
//! which is exactly the class of bug the unparser can introduce without any error.
//!
//! Tables come from CSV files under `slt/data/` with an explicit schema (`Dataset`), loaded into
//! DataFusion as in-memory tables and into the warehouse under a schema of their own, so the
//! files can name tables bare.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use arrow::array::{RecordBatch, RecordBatchReader};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use arrow::util::display::{ArrayFormatter, FormatOptions};
use async_trait::async_trait;
use datafusion::prelude::{CsvReadOptions, SessionConfig, SessionContext};
use engine_core::api::{
    CatalogSnapshot, ColumnSnapshot, DatabaseRegistration, PoolConfig, Request, Response,
    TableSnapshot,
};
use engine_core::engine::{Engine, EngineConfig};
use engine_core::error::EngineError;
use sqllogictest::{AsyncDB, DBOutput, DefaultColumnType};

/// The schema the warehouse tables live in, and the engine's default schema for the session.
pub const SCHEMA: &str = "slt";

/// One table of the dataset: its CSV file and the type of every column, in both worlds.
pub struct TableDef {
    pub name: &'static str,
    pub columns: Vec<ColumnDef>,
}

pub struct ColumnDef {
    pub name: &'static str,
    pub arrow: DataType,
    /// The Postgres column type.
    pub postgres: &'static str,
}

fn col(name: &'static str, arrow: DataType, postgres: &'static str) -> ColumnDef {
    ColumnDef {
        name,
        arrow,
        postgres,
    }
}

/// The dataset every `.slt` file is written against.
pub fn dataset() -> Vec<TableDef> {
    let ts = DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()));
    vec![
        TableDef {
            name: "products",
            columns: vec![
                col("id", DataType::Int64, "bigint"),
                col("title", DataType::Utf8, "text"),
                col("category", DataType::Utf8, "text"),
                col("price", DataType::Decimal128(10, 2), "numeric(10, 2)"),
                col("rating", DataType::Float64, "double precision"),
                col("active", DataType::Boolean, "boolean"),
                col("created_at", ts.clone(), "timestamptz"),
            ],
        },
        TableDef {
            name: "orders",
            columns: vec![
                col("id", DataType::Int64, "bigint"),
                col("product_id", DataType::Int64, "bigint"),
                col("quantity", DataType::Int64, "bigint"),
                col("total", DataType::Decimal128(10, 2), "numeric(10, 2)"),
                col("note", DataType::Utf8, "text"),
                col("ordered_on", DataType::Date32, "date"),
                col("created_at", ts, "timestamptz"),
            ],
        },
    ]
}

impl TableDef {
    pub fn arrow_schema(&self) -> SchemaRef {
        Arc::new(Schema::new(
            self.columns
                .iter()
                .map(|c| Field::new(c.name, c.arrow.clone(), true))
                .collect::<Vec<_>>(),
        ))
    }
}

/// The directory holding `data/` and the `.slt` files.
pub fn slt_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../../slt")
}

// ------------------------------------------------------------------ formatting

/// Rows as sqllogictest sees them: one string per cell, NULL spelled out, and the column types.
pub fn format_batches(schema: &Schema, batches: &[RecordBatch]) -> DBOutput<DefaultColumnType> {
    let types = schema
        .fields()
        .iter()
        .map(|f| match f.data_type() {
            DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64 => {
                DefaultColumnType::Integer
            }
            DataType::Float32 | DataType::Float64 | DataType::Decimal128(_, _) => {
                DefaultColumnType::FloatingPoint
            }
            DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => DefaultColumnType::Text,
            _ => DefaultColumnType::Any,
        })
        .collect();
    let options = FormatOptions::default().with_null("NULL");
    let mut rows = Vec::new();
    for batch in batches {
        let formatters: Vec<ArrayFormatter> = batch
            .columns()
            .iter()
            .map(|c| ArrayFormatter::try_new(c.as_ref(), &options).expect("formatter"))
            .collect();
        for i in 0..batch.num_rows() {
            rows.push(formatters.iter().map(|f| f.value(i).to_string()).collect());
        }
    }
    if rows.is_empty() && schema.fields().is_empty() {
        return DBOutput::StatementComplete(0);
    }
    DBOutput::Rows { types, rows }
}

// ------------------------------------------------------------------ the oracle

/// DataFusion executing the SQL itself over the CSV-backed tables.
pub struct DataFusionDb {
    ctx: SessionContext,
}

#[derive(Debug)]
pub struct HarnessError(pub String);

impl std::fmt::Display for HarnessError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for HarnessError {}

impl From<datafusion::error::DataFusionError> for HarnessError {
    fn from(e: datafusion::error::DataFusionError) -> Self {
        HarnessError(e.to_string())
    }
}

impl From<EngineError> for HarnessError {
    fn from(e: EngineError) -> Self {
        HarnessError(format!("{} ({})", e.message, e.error_type))
    }
}

impl DataFusionDb {
    pub async fn new() -> Result<DataFusionDb, HarnessError> {
        let mut config = SessionConfig::new();
        {
            let options = config.options_mut();
            // the same parser settings the engine plans with
            options.sql_parser.enable_ident_normalization = false;
            options.sql_parser.dialect = datafusion::common::config::Dialect::PostgreSQL;
            options.catalog.default_schema = SCHEMA.to_string();
            options.execution.time_zone = Some("UTC".into());
        }
        let ctx = SessionContext::new_with_config(config);
        let catalog = ctx.catalog("datafusion").expect("default catalog");
        catalog.register_schema(
            SCHEMA,
            Arc::new(datafusion::catalog::MemorySchemaProvider::new()),
        )?;
        for table in dataset() {
            let path = slt_dir().join("data").join(format!("{}.csv", table.name));
            let schema = table.arrow_schema();
            let df = ctx
                .read_csv(
                    path.to_string_lossy().to_string(),
                    CsvReadOptions::new().schema(&schema).has_header(true),
                )
                .await?;
            let batches = df.collect().await?;
            let mem = datafusion::datasource::MemTable::try_new(schema, vec![batches])?;
            ctx.register_table(format!("{SCHEMA}.{}", table.name), Arc::new(mem))?;
        }
        Ok(DataFusionDb { ctx })
    }
}

#[async_trait]
impl AsyncDB for DataFusionDb {
    type Error = HarnessError;
    type ColumnType = DefaultColumnType;

    async fn run(&mut self, sql: &str) -> Result<DBOutput<DefaultColumnType>, HarnessError> {
        let df = self.ctx.sql(sql).await?;
        let schema: Schema = df.schema().as_arrow().clone();
        let batches = df.collect().await?;
        Ok(format_batches(&schema, &batches))
    }

    async fn shutdown(&mut self) {}

    fn engine_name(&self) -> &str {
        "datafusion"
    }
}

// ------------------------------------------------------------------ the candidate

/// The engine planning and unparsing for Postgres, and Postgres executing.
pub struct EngineDb {
    /// `None` once shut down. The engine owns a tokio runtime, which must be dropped off any
    /// runtime thread; `shutdown` and `Drop` move it to a blocking thread.
    engine: Option<Arc<Engine>>,
    session: u64,
}

impl Drop for EngineDb {
    fn drop(&mut self) {
        if let Some(engine) = self.engine.take() {
            std::thread::spawn(move || drop(engine));
        }
    }
}

fn arrow_type_name(t: &DataType) -> String {
    // the catalog takes the `Display` form the Clojure side sends (`Timestamp(Microsecond, Some("UTC"))`)
    format!("{t:?}")
}

fn postgres_type_name(pg: &str) -> String {
    pg.split('(').next().unwrap_or(pg).trim().to_string()
}

impl EngineDb {
    /// Start an engine over `postgres_details` (the connector's JSON details) with the dataset's
    /// catalog registered. Runs off the caller's runtime: the engine owns a runtime of its own
    /// and blocks the calling thread, as it does under the JVM.
    pub async fn connect(postgres_details: serde_json::Value) -> Result<EngineDb, HarnessError> {
        tokio::task::spawn_blocking(move || EngineDb::new(postgres_details))
            .await
            .map_err(|e| HarnessError(e.to_string()))?
    }

    /// Synchronous construction; must not run on a tokio worker thread (see `connect`).
    pub fn new(postgres_details: serde_json::Value) -> Result<EngineDb, HarnessError> {
        let engine = Engine::with_connectors(
            EngineConfig::default(),
            vec![Arc::new(engine_connector_postgres::PostgresConnector)],
        )?;
        let snapshot = CatalogSnapshot {
            database_id: 1,
            catalog_version: "slt".into(),
            connector: "postgres".into(),
            default_schema: Some(SCHEMA.into()),
            tables: dataset()
                .iter()
                .map(|t| TableSnapshot {
                    schema: Some(SCHEMA.into()),
                    name: t.name.into(),
                    columns: t
                        .columns
                        .iter()
                        .map(|c| ColumnSnapshot {
                            name: c.name.into(),
                            arrow_type: arrow_type_name(&c.arrow),
                            nullable: true,
                            db_type: Some(postgres_type_name(c.postgres)),
                        })
                        .collect(),
                })
                .collect(),
        };
        expect_ok(engine.handle(Request::RegisterCatalog { snapshot }))?;
        expect_ok(engine.handle(Request::RegisterDatabase {
            registration: DatabaseRegistration {
                database_id: 1,
                connector: "postgres".into(),
                details: postgres_details,
                pool: PoolConfig::default(),
            },
        }))?;
        let context =
            serde_json::from_value(serde_json::json!({"database_id": 1, "report_timezone": "UTC"}))
                .map_err(|e| HarnessError(e.to_string()))?;
        let session = engine.create_session(context)?;
        Ok(EngineDb {
            engine: Some(engine),
            session,
        })
    }

    /// The Postgres SQL the engine would send for `sql`, for diagnosing a difference.
    pub fn unparse(&self, sql: &str) -> Result<String, HarnessError> {
        let response = self.engine()?.handle(Request::PlanAndUnparse {
            session_id: self.session,
            sql: sql.to_string(),
            params: vec![],
        });
        match response {
            Response::Ok { result } => {
                Ok(result["warehouse_sql"].as_str().unwrap_or("").to_string())
            }
            Response::Err { error } => Err(error.into()),
        }
    }
}

impl EngineDb {
    fn engine(&self) -> Result<&Arc<Engine>, HarnessError> {
        self.engine
            .as_ref()
            .ok_or_else(|| HarnessError("engine already shut down".into()))
    }
}

fn expect_ok(response: Response) -> Result<(), HarnessError> {
    match response {
        Response::Ok { .. } => Ok(()),
        Response::Err { error } => Err(error.into()),
    }
}

#[async_trait]
impl AsyncDB for EngineDb {
    type Error = HarnessError;
    type ColumnType = DefaultColumnType;

    async fn run(&mut self, sql: &str) -> Result<DBOutput<DefaultColumnType>, HarnessError> {
        let engine = Arc::clone(self.engine()?);
        let session = self.session;
        let sql = sql.to_string();
        // the engine's own runtime does the work; this thread only waits, as the JVM does
        tokio::task::spawn_blocking(move || {
            let stream_id = engine.execute_blocking(session, sql, vec![])?;
            let reader = engine.blocking_reader(stream_id)?;
            let schema = reader.schema();
            let mut batches = Vec::new();
            for batch in reader {
                batches.push(batch.map_err(|e| HarnessError(e.to_string()))?);
            }
            engine.close_stream(stream_id);
            Ok(format_batches(&schema, &batches))
        })
        .await
        .map_err(|e| HarnessError(e.to_string()))?
    }

    async fn shutdown(&mut self) {
        if let Some(engine) = self.engine.take() {
            let _ = tokio::task::spawn_blocking(move || drop(engine)).await;
        }
    }

    fn engine_name(&self) -> &str {
        "engine+postgres"
    }
}

// ------------------------------------------------------------------ warehouse loading

/// Create the dataset's tables in the `slt` schema of the Postgres database and load the CSVs.
pub async fn load_postgres(config: &tokio_postgres::Config) -> Result<(), HarnessError> {
    let (client, connection) = config
        .connect(tokio_postgres::NoTls)
        .await
        .map_err(|e| HarnessError(e.to_string()))?;
    tokio::spawn(async move {
        let _ = connection.await;
    });
    client
        .batch_execute(&format!("CREATE SCHEMA IF NOT EXISTS {SCHEMA};"))
        .await
        .map_err(|e| HarnessError(e.to_string()))?;
    for table in dataset() {
        let columns: Vec<String> = table
            .columns
            .iter()
            .map(|c| format!("\"{}\" {}", c.name, c.postgres))
            .collect();
        client
            .batch_execute(&format!(
                "DROP TABLE IF EXISTS {SCHEMA}.\"{0}\"; CREATE TABLE {SCHEMA}.\"{0}\" ({1});",
                table.name,
                columns.join(", ")
            ))
            .await
            .map_err(|e| HarnessError(e.to_string()))?;
        let path = slt_dir().join("data").join(format!("{}.csv", table.name));
        let mut reader = csv::Reader::from_path(&path).map_err(|e| HarnessError(e.to_string()))?;
        let mut rows = Vec::new();
        for record in reader.records() {
            let record = record.map_err(|e| HarnessError(e.to_string()))?;
            let values: Vec<String> = record
                .iter()
                .map(|v| {
                    if v.is_empty() {
                        "NULL".to_string()
                    } else {
                        format!("'{}'", v.replace('\'', "''"))
                    }
                })
                .collect();
            rows.push(format!("({})", values.join(", ")));
        }
        if !rows.is_empty() {
            client
                .batch_execute(&format!(
                    "INSERT INTO {SCHEMA}.\"{}\" VALUES {};",
                    table.name,
                    rows.join(", ")
                ))
                .await
                .map_err(|e| HarnessError(e.to_string()))?;
        }
    }
    Ok(())
}

/// Whether a missing Postgres is a failure rather than a skip (`MB_ENGINE_TESTS_REQUIRED`, as the
/// Clojure engine tests use).
pub fn postgres_required() -> bool {
    std::env::var("MB_ENGINE_TESTS_REQUIRED").is_ok_and(|v| v == "true")
}

/// The Postgres to run against: `ENGINE_TEST_POSTGRES_DETAILS` (the connector's JSON details, as
/// the live connector tests take), else `MB_ENGINE_TEST_PG_*`, else the local development
/// defaults.
pub fn postgres_details() -> serde_json::Value {
    if let Ok(json) = std::env::var("ENGINE_TEST_POSTGRES_DETAILS") {
        return serde_json::from_str(&json).expect("ENGINE_TEST_POSTGRES_DETAILS is JSON");
    }
    let env = |k: &str, d: &str| std::env::var(k).unwrap_or_else(|_| d.to_string());
    serde_json::json!({
        "host": env("MB_ENGINE_TEST_PG_HOST", "localhost"),
        "port": env("MB_ENGINE_TEST_PG_PORT", "5432").parse::<u16>().unwrap_or(5432),
        "dbname": env("MB_ENGINE_TEST_PG_DBNAME", "metabase_1"),
        "user": env("MB_ENGINE_TEST_PG_USER", "metabase"),
        "password": env("MB_ENGINE_TEST_PG_PASSWORD", "password"),
        "ssl": false
    })
}

pub fn postgres_config(details: &serde_json::Value) -> tokio_postgres::Config {
    let mut config = tokio_postgres::Config::new();
    config
        .host(details["host"].as_str().unwrap_or("localhost"))
        .port(details["port"].as_u64().unwrap_or(5432) as u16)
        .dbname(details["dbname"].as_str().unwrap_or("metabase_1"))
        .user(details["user"].as_str().unwrap_or("metabase"))
        .password(details["password"].as_str().unwrap_or(""));
    config
}

/// Every `.slt` file of the suite.
pub fn slt_files() -> Vec<PathBuf> {
    let mut files: Vec<PathBuf> = std::fs::read_dir(slt_dir())
        .expect("slt directory")
        .filter_map(|e| e.ok().map(|e| e.path()))
        .filter(|p| p.extension().is_some_and(|x| x == "slt"))
        .collect();
    files.sort();
    files
}

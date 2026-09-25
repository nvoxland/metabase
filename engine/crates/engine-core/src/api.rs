//! The message-shaped API. Both front-ends (the C ABI and the Flight sidecar) carry these
//! exact JSON shapes, so a request looks the same in-process and over the network.

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::error::EngineError;

/// Everything a request needs beyond the SQL itself.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct SessionContext {
    pub database_id: i64,
    #[serde(default)]
    pub user_id: Option<i64>,
    #[serde(default)]
    pub user_attributes: serde_json::Map<String, Value>,
    #[serde(default)]
    pub workspace_id: Option<String>,
    #[serde(default)]
    pub report_timezone: Option<String>,
    /// Metabase's row cap for the query (what the JDBC path passes to `Statement.setMaxRows`): the
    /// connector stops the statement after this many rows. Not a `LIMIT`, which would change how the
    /// warehouse plans the query.
    #[serde(default)]
    pub row_limit: Option<u64>,
    #[serde(default)]
    pub query_origin: Option<QueryOrigin>,
    /// Audit comment Metabase wants at the top of the warehouse SQL (single line).
    #[serde(default)]
    pub remark: Option<String>,
    /// Impersonation role to set for the statement, when the warehouse supports it.
    #[serde(default)]
    pub role: Option<String>,
    #[serde(default)]
    pub statement_timeout_ms: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryOrigin {
    #[serde(rename = "type")]
    pub kind: String,
    #[serde(default)]
    pub id: Option<i64>,
}

/// A bound parameter value. `type` is an Arrow type in its `Display` form (e.g. `Int64`,
/// `Utf8`, `Timestamp(Microsecond, Some("UTC"))`).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ParamValue {
    #[serde(rename = "type")]
    pub arrow_type: String,
    pub value: Value,
}

/// A database's tables and columns as Metabase knows them.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CatalogSnapshot {
    pub database_id: i64,
    pub catalog_version: String,
    /// Connector kind, e.g. `postgres`.
    pub connector: String,
    #[serde(default)]
    pub default_schema: Option<String>,
    #[serde(default)]
    pub tables: Vec<TableSnapshot>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableSnapshot {
    #[serde(default)]
    pub schema: Option<String>,
    pub name: String,
    #[serde(default)]
    pub columns: Vec<ColumnSnapshot>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnSnapshot {
    pub name: String,
    /// Arrow type in `Display` form.
    #[serde(rename = "type")]
    pub arrow_type: String,
    #[serde(default = "default_true")]
    pub nullable: bool,
    /// The warehouse's own type name, for the decoder.
    #[serde(default)]
    pub db_type: Option<String>,
}

fn default_true() -> bool {
    true
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DatabaseRegistration {
    pub database_id: i64,
    pub connector: String,
    /// Decrypted connection details, as Metabase stores them.
    pub details: Value,
    #[serde(default)]
    pub pool: PoolConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PoolConfig {
    #[serde(default = "default_pool_size")]
    pub max_size: usize,
    #[serde(default = "default_checkout_timeout")]
    pub checkout_timeout_ms: u64,
    #[serde(default = "default_connect_timeout")]
    pub connect_timeout_ms: u64,
}

fn default_pool_size() -> usize {
    15
}
fn default_checkout_timeout() -> u64 {
    10_000
}
fn default_connect_timeout() -> u64 {
    10_000
}

impl Default for PoolConfig {
    fn default() -> Self {
        PoolConfig {
            max_size: default_pool_size(),
            checkout_timeout_ms: default_checkout_timeout(),
            connect_timeout_ms: default_connect_timeout(),
        }
    }
}

/// What `plan_and_unparse` returns.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PlanResult {
    pub warehouse_sql: String,
    pub warehouse: String,
    pub schema: Vec<FieldInfo>,
    #[serde(default)]
    pub warnings: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct FieldInfo {
    pub name: String,
    #[serde(rename = "type")]
    pub arrow_type: String,
    pub nullable: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case")]
pub enum Request {
    Version,
    RegisterCatalog {
        snapshot: CatalogSnapshot,
    },
    InvalidateDatabase {
        database_id: i64,
    },
    RegisterDatabase {
        registration: DatabaseRegistration,
    },
    CreateSession {
        context: SessionContext,
    },
    DestroySession {
        session_id: u64,
    },
    PlanAndUnparse {
        session_id: u64,
        sql: String,
        #[serde(default)]
        params: Vec<ParamValue>,
    },
    Cancel {
        stream_id: u64,
    },
    PoolStats {
        #[serde(default)]
        database_id: Option<i64>,
    },
    /// Test hook: panics inside the engine so callers can verify panic containment.
    /// Only honored when the engine was built with `allow_debug_ops`.
    DebugPanic,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum Response {
    Ok { result: Value },
    Err { error: EngineError },
}

impl Response {
    pub fn ok<T: Serialize>(value: T) -> Response {
        match serde_json::to_value(value) {
            Ok(result) => Response::Ok { result },
            Err(e) => Response::Err {
                error: EngineError::invalid_request(format!("serializing response: {e}")),
            },
        }
    }

    pub fn err(error: EngineError) -> Response {
        Response::Err { error }
    }

    pub fn is_ok(&self) -> bool {
        matches!(self, Response::Ok { .. })
    }

    pub fn to_json(&self) -> String {
        serde_json::to_string(self).unwrap_or_else(|e| {
            Response::err(EngineError::invalid_request(format!(
                "serializing response: {e}"
            )))
            .to_json()
        })
    }
}

impl From<crate::error::Result<Value>> for Response {
    fn from(r: crate::error::Result<Value>) -> Self {
        match r {
            Ok(result) => Response::Ok { result },
            Err(error) => Response::Err { error },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn requests_round_trip_through_json() {
        let json = r#"{"op":"plan_and_unparse","session_id":7,"sql":"SELECT 1","params":[{"type":"Int64","value":3}]}"#;
        let req: Request = serde_json::from_str(json).unwrap();
        match req {
            Request::PlanAndUnparse {
                session_id,
                sql,
                params,
            } => {
                assert_eq!(session_id, 7);
                assert_eq!(sql, "SELECT 1");
                assert_eq!(params[0].arrow_type, "Int64");
            }
            other => panic!("unexpected {other:?}"),
        }
        let version: Request = serde_json::from_str(r#"{"op":"version"}"#).unwrap();
        assert!(matches!(version, Request::Version));
    }

    #[test]
    fn snapshot_columns_default_to_nullable() {
        let json = r#"{"database_id":1,"catalog_version":"v1","connector":"postgres",
            "tables":[{"schema":"public","name":"t","columns":[{"name":"c","type":"Int64"}]}]}"#;
        let snap: CatalogSnapshot = serde_json::from_str(json).unwrap();
        assert!(snap.tables[0].columns[0].nullable);
        assert_eq!(snap.default_schema, None);
    }

    #[test]
    fn response_json_is_tagged_by_status() {
        let ok = Response::ok(serde_json::json!({"a": 1})).to_json();
        assert!(ok.starts_with(r#"{"status":"ok""#));
        let err = Response::err(EngineError::invalid_request("nope")).to_json();
        assert!(err.contains(r#""status":"err""#));
        assert!(err.contains(r#""error_type":"qp""#));
    }
}

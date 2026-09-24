//! Structured errors. `error_type` is the exact keyword name from
//! `metabase.query-processor.error-type`, so the Clojure side rethrows without translation.

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ErrorKind {
    Parse,
    Plan,
    Unsupported,
    CatalogMissing,
    SessionMissing,
    PoolTimeout,
    Connect,
    Warehouse,
    Cancelled,
    Timeout,
    InternalPanic,
    InvalidRequest,
    NotImplemented,
}

impl ErrorKind {
    /// The `metabase.query-processor.error-type` keyword this kind maps onto.
    pub fn error_type(&self) -> &'static str {
        match self {
            ErrorKind::Parse | ErrorKind::Plan => "invalid-query",
            ErrorKind::Unsupported | ErrorKind::NotImplemented => "unsupported-feature",
            ErrorKind::CatalogMissing => "bad-configuration",
            ErrorKind::SessionMissing | ErrorKind::InvalidRequest => "qp",
            ErrorKind::PoolTimeout => "connection-pool-checkout-timeout",
            ErrorKind::Connect => "unable-to-acquire-connection",
            ErrorKind::Warehouse => "db",
            ErrorKind::Cancelled => "qp",
            ErrorKind::Timeout => "timed-out",
            ErrorKind::InternalPanic => "qp",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, thiserror::Error)]
#[error("{message}")]
pub struct EngineError {
    pub kind: ErrorKind,
    pub error_type: String,
    pub message: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub construct: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub warehouse: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sql_state: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub warehouse_message: Option<String>,
    #[serde(default)]
    pub retryable: bool,
}

impl EngineError {
    pub fn new(kind: ErrorKind, message: impl Into<String>) -> EngineError {
        EngineError {
            kind,
            error_type: kind.error_type().to_string(),
            message: message.into(),
            construct: None,
            warehouse: None,
            sql_state: None,
            warehouse_message: None,
            retryable: false,
        }
    }

    pub fn unsupported(construct: impl Into<String>, warehouse: &str) -> EngineError {
        let construct = construct.into();
        let mut e = EngineError::new(
            ErrorKind::Unsupported,
            format!("{construct} is not supported on {warehouse}"),
        );
        e.construct = Some(construct);
        e.warehouse = Some(warehouse.to_string());
        e
    }

    pub fn invalid_request(message: impl Into<String>) -> EngineError {
        EngineError::new(ErrorKind::InvalidRequest, message)
    }

    pub fn not_implemented(what: &str) -> EngineError {
        EngineError::new(
            ErrorKind::NotImplemented,
            format!("{what} is not implemented yet"),
        )
    }

    pub fn to_json(&self) -> String {
        serde_json::to_string(self).unwrap_or_else(|_| {
            format!(
                "{{\"kind\":\"internal_panic\",\"error_type\":\"qp\",\"message\":{:?}}}",
                self.message
            )
        })
    }
}

impl From<datafusion::error::DataFusionError> for EngineError {
    fn from(e: datafusion::error::DataFusionError) -> Self {
        use datafusion::error::DataFusionError as DF;
        let kind = match &e {
            DF::SQL(_, _) => ErrorKind::Parse,
            DF::Plan(_) | DF::SchemaError(_, _) => ErrorKind::Plan,
            DF::NotImplemented(_) => ErrorKind::Unsupported,
            _ => ErrorKind::Plan,
        };
        // `strip_backtrace` keeps the message readable for people.
        EngineError::new(kind, e.strip_backtrace())
    }
}

impl From<serde_json::Error> for EngineError {
    fn from(e: serde_json::Error) -> Self {
        EngineError::invalid_request(format!("invalid JSON: {e}"))
    }
}

pub type Result<T> = std::result::Result<T, EngineError>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn error_json_carries_metabase_error_type() {
        let e = EngineError::unsupported("UNNEST", "postgres");
        let json: serde_json::Value = serde_json::from_str(&e.to_json()).unwrap();
        assert_eq!(json["kind"], "unsupported");
        assert_eq!(json["error_type"], "unsupported-feature");
        assert_eq!(json["construct"], "UNNEST");
        assert_eq!(json["warehouse"], "postgres");
        assert_eq!(json["retryable"], false);
        assert!(json.get("sql_state").is_none());
    }

    #[test]
    fn datafusion_parse_errors_become_invalid_query() {
        let df = datafusion::error::DataFusionError::SQL(
            Box::new(datafusion::sql::sqlparser::parser::ParserError::ParserError("boom".into())),
            None,
        );
        let e: EngineError = df.into();
        assert_eq!(e.kind, ErrorKind::Parse);
        assert_eq!(e.error_type, "invalid-query");
    }
}

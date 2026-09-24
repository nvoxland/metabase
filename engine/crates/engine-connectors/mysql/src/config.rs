//! Metabase connection details -> mysql_async options.

use std::time::Duration;

use engine_core::api::PoolConfig;
use engine_core::error::{EngineError, ErrorKind, Result};
use mysql_async::{Opts, OptsBuilder, PoolConstraints, PoolOpts, SslOpts};
use serde_json::Value;

fn detail_str<'a>(details: &'a Value, keys: &[&str]) -> Option<&'a str> {
    keys.iter()
        .find_map(|k| details.get(k).and_then(Value::as_str))
        .filter(|s| !s.is_empty())
}

fn detail_port(details: &Value) -> Option<u16> {
    match details.get("port") {
        Some(Value::Number(n)) => n.as_u64().map(|p| p as u16),
        Some(Value::String(s)) => s.trim().parse().ok(),
        _ => None,
    }
}

fn detail_bool(details: &Value, key: &str) -> bool {
    match details.get(key) {
        Some(Value::Bool(b)) => *b,
        Some(Value::String(s)) => matches!(s.as_str(), "true" | "yes" | "on" | "1"),
        _ => false,
    }
}

/// How long a connection may sit unused in the pool before mysql_async closes it (the engine's
/// idle reaper does the same for pools it manages itself).
pub const IDLE_TTL: Duration = Duration::from_secs(300);

/// Build the options from Metabase's MySQL driver details (`host`, `port`, `dbname`/`db`, `user`,
/// `password`, `ssl`).
pub fn from_details(details: &Value, pool: &PoolConfig) -> Result<Opts> {
    let bad = |what: &str| {
        EngineError::new(
            ErrorKind::Connect,
            format!("connection details are missing {what}"),
        )
    };
    let host = detail_str(details, &["host"]).ok_or_else(|| bad("host"))?;
    let dbname = detail_str(details, &["dbname", "db"]).ok_or_else(|| bad("dbname"))?;
    let constraints = PoolConstraints::new(0, pool.max_size.max(1))
        .ok_or_else(|| EngineError::new(ErrorKind::InvalidRequest, "invalid pool size"))?;
    let mut builder = OptsBuilder::default()
        .ip_or_hostname(host)
        .tcp_port(detail_port(details).unwrap_or(3306))
        .db_name(Some(dbname))
        .user(detail_str(details, &["user"]).map(str::to_string))
        .pass(detail_str(details, &["password"]).map(str::to_string))
        // the connector sets every session variable it relies on at checkout, so a returned
        // connection needs no server round trip to be reset
        .pool_opts(
            PoolOpts::default()
                .with_constraints(constraints)
                .with_reset_connection(false)
                .with_inactive_connection_ttl(IDLE_TTL)
                .with_ttl_check_interval(Duration::from_secs(60)),
        )
        .stmt_cache_size(64)
        .tcp_keepalive(Some(Duration::from_secs(60)))
        .prefer_socket(false);
    if detail_bool(details, "ssl") {
        // encrypt without authenticating the server, which is what the JDBC path does unless a
        // certificate is supplied (a database with one stays on JDBC)
        builder = builder.ssl_opts(Some(
            SslOpts::default()
                .with_danger_accept_invalid_certs(true)
                .with_danger_skip_domain_validation(true),
        ));
    }
    Ok(Opts::from(builder))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn details_map_onto_the_options() {
        let details = serde_json::json!({"host": "db.example", "port": "3307", "dbname": "warehouse",
                                         "user": "mb", "password": "s3cret", "ssl": false});
        let opts = from_details(&details, &PoolConfig::default()).unwrap();
        assert_eq!(opts.ip_or_hostname(), "db.example");
        assert_eq!(opts.tcp_port(), 3307);
        assert_eq!(opts.db_name(), Some("warehouse"));
        assert_eq!(opts.user(), Some("mb"));
        assert!(opts.ssl_opts().is_none());
        let secure = from_details(
            &serde_json::json!({"host": "h", "db": "d", "user": "mb", "ssl": true}),
            &PoolConfig::default(),
        )
        .unwrap();
        assert_eq!(secure.tcp_port(), 3306);
        assert!(secure.ssl_opts().is_some_and(|s| s.accept_invalid_certs()));
    }

    #[test]
    fn missing_host_or_database_is_a_connect_error() {
        for details in [
            serde_json::json!({"dbname": "x"}),
            serde_json::json!({"host": "h"}),
        ] {
            let err = from_details(&details, &PoolConfig::default()).unwrap_err();
            assert_eq!(err.kind, ErrorKind::Connect);
        }
    }
}

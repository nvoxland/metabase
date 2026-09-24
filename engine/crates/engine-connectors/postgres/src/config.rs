//! Metabase connection details -> tokio-postgres config and TLS mode.

use std::sync::Arc;
use std::time::Duration;

use engine_core::error::{EngineError, ErrorKind, Result};
use serde_json::Value;
use tokio_postgres_rustls::MakeRustlsConnect;

/// How to reach the server, derived from the `ssl` / `ssl-mode` details.
#[derive(Clone)]
pub enum Tls {
    None,
    /// TLS with certificate verification against the system roots.
    Verified(MakeRustlsConnect),
    /// TLS without verification: what Postgres' own `sslmode=require` does.
    Unverified(MakeRustlsConnect),
}

impl Tls {
    pub fn connector(&self) -> Option<MakeRustlsConnect> {
        match self {
            Tls::None => None,
            Tls::Verified(c) | Tls::Unverified(c) => Some(c.clone()),
        }
    }
}

impl std::fmt::Debug for Tls {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Tls::None => "Tls::None",
            Tls::Verified(_) => "Tls::Verified",
            Tls::Unverified(_) => "Tls::Unverified",
        })
    }
}

#[derive(Debug)]
pub struct PgConfig {
    pub config: tokio_postgres::Config,
    pub tls: Tls,
}

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

/// Build the connection config from Metabase's Postgres driver details (`host`, `port`, `dbname`/`db`,
/// `user`, `password`, `ssl`, `ssl-mode`).
pub fn from_details(details: &Value, connect_timeout: Duration) -> Result<PgConfig> {
    let bad = |what: &str| {
        EngineError::new(
            ErrorKind::Connect,
            format!("connection details are missing {what}"),
        )
    };
    let mut config = tokio_postgres::Config::new();
    config.host(detail_str(details, &["host"]).ok_or_else(|| bad("host"))?);
    config.port(detail_port(details).unwrap_or(5432));
    config.dbname(detail_str(details, &["dbname", "db"]).ok_or_else(|| bad("dbname"))?);
    if let Some(user) = detail_str(details, &["user"]) {
        config.user(user);
    }
    if let Some(password) = detail_str(details, &["password"]) {
        config.password(password);
    }
    config.application_name("metabase-engine");
    config.connect_timeout(connect_timeout);

    let tls = if detail_bool(details, "ssl") {
        // Never fall back to plaintext when the details ask for TLS (tokio-postgres defaults to `Prefer`).
        config.ssl_mode(tokio_postgres::config::SslMode::Require);
        let mode = detail_str(details, &["ssl-mode"]).unwrap_or("require");
        match mode {
            "verify-ca" | "verify-full" => {
                Tls::Verified(MakeRustlsConnect::new(verified_client_config()?))
            }
            _ => Tls::Unverified(MakeRustlsConnect::new(unverified_client_config())),
        }
    } else {
        Tls::None
    };
    Ok(PgConfig { config, tls })
}

fn verified_client_config() -> Result<rustls::ClientConfig> {
    let mut roots = rustls::RootCertStore::empty();
    let native = rustls_native_certs::load_native_certs();
    for cert in native.certs {
        roots.add(cert).map_err(|e| {
            EngineError::new(
                ErrorKind::Connect,
                format!("loading system certificates: {e}"),
            )
        })?;
    }
    Ok(rustls::ClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth())
}

fn unverified_client_config() -> rustls::ClientConfig {
    let mut config = rustls::ClientConfig::builder()
        .with_root_certificates(rustls::RootCertStore::empty())
        .with_no_client_auth();
    config
        .dangerous()
        .set_certificate_verifier(Arc::new(NoVerification));
    config
}

/// Accepts any server certificate. Only used for `sslmode=require`, which is Postgres' own definition
/// of "encrypt, don't authenticate".
#[derive(Debug)]
struct NoVerification;

impl rustls::client::danger::ServerCertVerifier for NoVerification {
    fn verify_server_cert(
        &self,
        _end_entity: &rustls::pki_types::CertificateDer<'_>,
        _intermediates: &[rustls::pki_types::CertificateDer<'_>],
        _server_name: &rustls::pki_types::ServerName<'_>,
        _ocsp_response: &[u8],
        _now: rustls::pki_types::UnixTime,
    ) -> std::result::Result<rustls::client::danger::ServerCertVerified, rustls::Error> {
        Ok(rustls::client::danger::ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        _message: &[u8],
        _cert: &rustls::pki_types::CertificateDer<'_>,
        _dss: &rustls::DigitallySignedStruct,
    ) -> std::result::Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
    }

    fn verify_tls13_signature(
        &self,
        _message: &[u8],
        _cert: &rustls::pki_types::CertificateDer<'_>,
        _dss: &rustls::DigitallySignedStruct,
    ) -> std::result::Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
    }

    fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
        rustls::crypto::ring::default_provider()
            .signature_verification_algorithms
            .supported_schemes()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn details_map_onto_the_config() {
        let details = serde_json::json!({"host": "db.example", "port": "6543", "dbname": "warehouse",
                                         "user": "mb", "password": "s3cret", "ssl": false});
        let cfg = from_details(&details, Duration::from_secs(5)).unwrap();
        assert_eq!(cfg.config.get_ports(), &[6543]);
        assert_eq!(cfg.config.get_dbname(), Some("warehouse"));
        assert_eq!(cfg.config.get_user(), Some("mb"));
        assert!(matches!(cfg.tls, Tls::None));
        assert!(matches!(
            cfg.config.get_ssl_mode(),
            tokio_postgres::config::SslMode::Prefer
        ));
        let secure = from_details(
            &serde_json::json!({"host": "h", "port": 5432, "dbname": "d", "user": "mb", "password": "s3cret", "ssl": true}),
            Duration::from_secs(5),
        )
        .unwrap();
        assert!(matches!(secure.tls, Tls::Unverified(_)));
        assert!(matches!(
            secure.config.get_ssl_mode(),
            tokio_postgres::config::SslMode::Require
        ));
    }

    #[test]
    fn db_is_an_alias_for_dbname_and_ssl_picks_a_tls_mode() {
        let details = serde_json::json!({"host": "h", "db": "x", "ssl": true});
        assert!(matches!(
            from_details(&details, Duration::from_secs(1)).unwrap().tls,
            Tls::Unverified(_)
        ));
        let details =
            serde_json::json!({"host": "h", "db": "x", "ssl": true, "ssl-mode": "verify-full"});
        assert!(matches!(
            from_details(&details, Duration::from_secs(1)).unwrap().tls,
            Tls::Verified(_)
        ));
    }

    #[test]
    fn missing_host_is_a_connect_error() {
        let err =
            from_details(&serde_json::json!({"dbname": "x"}), Duration::from_secs(1)).unwrap_err();
        assert_eq!(err.kind, ErrorKind::Connect);
    }
}

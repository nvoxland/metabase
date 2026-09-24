//! Bind parameter values to the types Postgres inferred for the prepared statement.

use bytes::BytesMut;
use chrono::{DateTime, NaiveDate, NaiveDateTime, NaiveTime, TimeZone, Utc};
use engine_core::api::ParamValue;
use engine_core::error::{EngineError, ErrorKind, Result};
use serde_json::Value;
use tokio_postgres::types::{IsNull, Kind, ToSql, Type};

pub type BoundParam = Box<dyn ToSql + Sync + Send>;

fn invalid(index: usize, ty: &Type, value: &Value) -> EngineError {
    EngineError::new(
        ErrorKind::InvalidRequest,
        format!("parameter ${} cannot be bound as {ty}: {value}", index + 1),
    )
}

fn as_string(value: &Value) -> Option<String> {
    match value {
        Value::String(s) => Some(s.clone()),
        Value::Number(n) => Some(n.to_string()),
        Value::Bool(b) => Some(b.to_string()),
        _ => None,
    }
}

fn parse_timestamp(s: &str) -> Option<NaiveDateTime> {
    let s = s.trim();
    if let Ok(dt) = DateTime::parse_from_rfc3339(s) {
        return Some(dt.naive_utc());
    }
    for fmt in [
        "%Y-%m-%dT%H:%M:%S%.f",
        "%Y-%m-%d %H:%M:%S%.f",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%dT%H:%M",
        "%Y-%m-%d %H:%M",
    ] {
        if let Ok(dt) = NaiveDateTime::parse_from_str(s, fmt) {
            return Some(dt);
        }
    }
    NaiveDate::parse_from_str(s, "%Y-%m-%d")
        .ok()
        .map(|d| d.and_hms_opt(0, 0, 0).unwrap())
}

/// Convert one JSON value into the Postgres type the statement expects.
/// A zone-less wall-clock time read in the session's zone (the report time zone), as Postgres reads
/// a `timestamp` literal it coerces to `timestamptz`; UTC when the session has none.
fn zoned(naive: NaiveDateTime, session_tz: Option<&str>) -> Option<DateTime<Utc>> {
    match session_tz {
        None => Some(DateTime::<Utc>::from_naive_utc_and_offset(naive, Utc)),
        Some(tz) => {
            let tz: arrow::array::timezone::Tz = tz.parse().ok()?;
            Some(tz.from_local_datetime(&naive).single()?.with_timezone(&Utc))
        }
    }
}

pub fn bind(
    index: usize,
    param: &ParamValue,
    ty: &Type,
    session_tz: Option<&str>,
) -> Result<BoundParam> {
    let v = &param.value;
    if v.is_null() {
        // A NULL must still carry the type Postgres inferred for the placeholder.
        let null: BoundParam = match *ty {
            Type::BOOL => Box::new(None::<bool>),
            Type::INT2 => Box::new(None::<i16>),
            Type::INT4 => Box::new(None::<i32>),
            Type::INT8 => Box::new(None::<i64>),
            Type::FLOAT4 => Box::new(None::<f32>),
            Type::FLOAT8 => Box::new(None::<f64>),
            Type::NUMERIC => Box::new(None::<rust_decimal::Decimal>),
            Type::DATE => Box::new(None::<NaiveDate>),
            Type::TIMESTAMP => Box::new(None::<NaiveDateTime>),
            Type::TIMESTAMPTZ => Box::new(None::<DateTime<Utc>>),
            Type::TIME => Box::new(None::<NaiveTime>),
            Type::UUID => Box::new(None::<uuid::Uuid>),
            Type::JSON | Type::JSONB => Box::new(None::<Value>),
            _ => Box::new(None::<String>),
        };
        return Ok(null);
    }
    let bound: BoundParam = match *ty {
        // A cast of a literal (`CAST($1 AS BIGINT)`) makes Postgres type the placeholder by the cast while
        // Metabase sends the literal as text, so numbers and booleans are also accepted as strings.
        Type::BOOL => Box::new(as_bool(v).ok_or_else(|| invalid(index, ty, v))?),
        Type::INT2 => Box::new(as_i64(v).ok_or_else(|| invalid(index, ty, v))? as i16),
        Type::INT4 => Box::new(as_i64(v).ok_or_else(|| invalid(index, ty, v))? as i32),
        Type::INT8 => Box::new(as_i64(v).ok_or_else(|| invalid(index, ty, v))?),
        Type::FLOAT4 => Box::new(as_f64(v).ok_or_else(|| invalid(index, ty, v))? as f32),
        Type::FLOAT8 => Box::new(as_f64(v).ok_or_else(|| invalid(index, ty, v))?),
        Type::NUMERIC => {
            let s = as_string(v).ok_or_else(|| invalid(index, ty, v))?;
            Box::new(
                s.parse::<rust_decimal::Decimal>()
                    .map_err(|_| invalid(index, ty, v))?,
            )
        }
        Type::DATE => {
            let s = v.as_str().ok_or_else(|| invalid(index, ty, v))?;
            // Metabase sends dates as `YYYY-MM-DD`, but a date filter value can also arrive as a full timestamp.
            let date = NaiveDate::parse_from_str(s.trim(), "%Y-%m-%d")
                .ok()
                .or_else(|| parse_timestamp(s).map(|t| t.date()))
                .ok_or_else(|| invalid(index, ty, v))?;
            Box::new(date)
        }
        Type::TIMESTAMP => {
            let s = v.as_str().ok_or_else(|| invalid(index, ty, v))?;
            Box::new(parse_timestamp(s).ok_or_else(|| invalid(index, ty, v))?)
        }
        Type::TIMESTAMPTZ => {
            let s = v.as_str().ok_or_else(|| invalid(index, ty, v))?;
            let naive = parse_timestamp(s).ok_or_else(|| invalid(index, ty, v))?;
            Box::new(zoned(naive, session_tz).ok_or_else(|| invalid(index, ty, v))?)
        }
        Type::TIME => {
            let s = v.as_str().ok_or_else(|| invalid(index, ty, v))?;
            let t = ["%H:%M:%S%.f", "%H:%M:%S", "%H:%M"]
                .iter()
                .find_map(|fmt| NaiveTime::parse_from_str(s.trim(), fmt).ok())
                .ok_or_else(|| invalid(index, ty, v))?;
            Box::new(t)
        }
        Type::BIT | Type::VARBIT => {
            let s = v.as_str().ok_or_else(|| invalid(index, ty, v))?;
            Box::new(PgBits::parse(s.trim()).ok_or_else(|| invalid(index, ty, v))?)
        }
        Type::INET | Type::CIDR => {
            let s = v.as_str().ok_or_else(|| invalid(index, ty, v))?;
            Box::new(
                PgInet::parse(s.trim(), *ty == Type::CIDR).ok_or_else(|| invalid(index, ty, v))?,
            )
        }
        Type::UUID => {
            let s = v.as_str().ok_or_else(|| invalid(index, ty, v))?;
            Box::new(s.parse::<uuid::Uuid>().map_err(|_| invalid(index, ty, v))?)
        }
        Type::JSON | Type::JSONB => Box::new(v.clone()),
        Type::TEXT | Type::VARCHAR | Type::BPCHAR | Type::NAME | Type::UNKNOWN => {
            Box::new(as_string(v).ok_or_else(|| invalid(index, ty, v))?)
        }
        // text types, enums, domains, and anything else this binder doesn't know: send the text form
        _ => match ty.kind() {
            Kind::Simple
                if matches!(
                    *ty,
                    Type::TEXT | Type::VARCHAR | Type::BPCHAR | Type::NAME | Type::UNKNOWN
                ) =>
            {
                Box::new(as_string(v).ok_or_else(|| invalid(index, ty, v))?)
            }
            _ => Box::new(AsText(as_string(v).ok_or_else(|| invalid(index, ty, v))?)),
        },
    };
    Ok(bound)
}

/// A `bit`/`varbit` value (`'1011'`) in Postgres' binary form: the bit count, then the bits packed
/// most significant first.
#[derive(Debug)]
struct PgBits {
    len: i32,
    bytes: Vec<u8>,
}

impl PgBits {
    fn parse(s: &str) -> Option<PgBits> {
        let s = s
            .strip_prefix("B'")
            .and_then(|r| r.strip_suffix('\''))
            .unwrap_or(s);
        if s.is_empty() || !s.bytes().all(|b| b == b'0' || b == b'1') {
            return None;
        }
        let mut bytes = vec![0u8; s.len().div_ceil(8)];
        for (i, b) in s.bytes().enumerate() {
            if b == b'1' {
                bytes[i / 8] |= 0x80 >> (i % 8);
            }
        }
        Some(PgBits {
            len: s.len() as i32,
            bytes,
        })
    }
}

impl ToSql for PgBits {
    fn to_sql(
        &self,
        _ty: &Type,
        out: &mut BytesMut,
    ) -> std::result::Result<IsNull, Box<dyn std::error::Error + Sync + Send>> {
        out.extend_from_slice(&self.len.to_be_bytes());
        out.extend_from_slice(&self.bytes);
        Ok(IsNull::No)
    }

    fn accepts(ty: &Type) -> bool {
        *ty == Type::BIT || *ty == Type::VARBIT
    }

    tokio_postgres::types::to_sql_checked!();
}

/// An `inet`/`cidr` value in Postgres' binary form: family, prefix bits, is-cidr, length, address.
#[derive(Debug)]
struct PgInet {
    addr: std::net::IpAddr,
    bits: u8,
    cidr: bool,
}

impl PgInet {
    fn parse(s: &str, cidr: bool) -> Option<PgInet> {
        let (addr, bits) = match s.split_once('/') {
            Some((a, b)) => (a.parse::<std::net::IpAddr>().ok()?, b.parse::<u8>().ok()?),
            None => {
                let addr = s.parse::<std::net::IpAddr>().ok()?;
                (addr, if addr.is_ipv4() { 32 } else { 128 })
            }
        };
        Some(PgInet { addr, bits, cidr })
    }
}

impl ToSql for PgInet {
    fn to_sql(
        &self,
        _ty: &Type,
        out: &mut BytesMut,
    ) -> std::result::Result<IsNull, Box<dyn std::error::Error + Sync + Send>> {
        const PGSQL_AF_INET: u8 = 2;
        const PGSQL_AF_INET6: u8 = 3;
        match self.addr {
            std::net::IpAddr::V4(a) => {
                out.extend_from_slice(&[PGSQL_AF_INET, self.bits, self.cidr as u8, 4]);
                out.extend_from_slice(&a.octets());
            }
            std::net::IpAddr::V6(a) => {
                out.extend_from_slice(&[PGSQL_AF_INET6, self.bits, self.cidr as u8, 16]);
                out.extend_from_slice(&a.octets());
            }
        }
        Ok(IsNull::No)
    }

    fn accepts(ty: &Type) -> bool {
        *ty == Type::INET || *ty == Type::CIDR
    }

    tokio_postgres::types::to_sql_checked!();
}

/// A value sent as its text form for a type this binder has no native encoding for (enums,
/// domains, extension types): Postgres accepts the text representation of any type as a binary
/// parameter for those.
#[derive(Debug)]
struct AsText(String);

impl ToSql for AsText {
    fn to_sql(
        &self,
        _ty: &Type,
        out: &mut BytesMut,
    ) -> std::result::Result<IsNull, Box<dyn std::error::Error + Sync + Send>> {
        out.extend_from_slice(self.0.as_bytes());
        Ok(IsNull::No)
    }

    fn accepts(_ty: &Type) -> bool {
        true
    }

    tokio_postgres::types::to_sql_checked!();
}

fn as_i64(v: &Value) -> Option<i64> {
    v.as_i64()
        .or_else(|| v.as_f64().filter(|f| f.fract() == 0.0).map(|f| f as i64))
        .or_else(|| v.as_str().and_then(|s| s.trim().parse::<i64>().ok()))
}

fn as_f64(v: &Value) -> Option<f64> {
    v.as_f64()
        .or_else(|| v.as_str().and_then(|s| s.trim().parse::<f64>().ok()))
}

fn as_bool(v: &Value) -> Option<bool> {
    v.as_bool().or_else(
        || match v.as_str().map(|s| s.trim().to_ascii_lowercase()).as_deref() {
            Some("true") | Some("t") | Some("1") => Some(true),
            Some("false") | Some("f") | Some("0") => Some(false),
            _ => None,
        },
    )
}

/// The Postgres type to declare for a placeholder when the server can't infer it, from the Arrow
/// type Metabase attached to the value.
pub fn declared_type(arrow_type: &str) -> Type {
    match arrow_type {
        "Boolean" => Type::BOOL,
        "Int8" | "Int16" => Type::INT2,
        "Int32" => Type::INT4,
        "Int64" => Type::INT8,
        "Float32" => Type::FLOAT4,
        "Float64" => Type::FLOAT8,
        "Date32" => Type::DATE,
        t if t.starts_with("Decimal") => Type::NUMERIC,
        t if t.starts_with("Timestamp") && t.contains("Some(") => Type::TIMESTAMPTZ,
        t if t.starts_with("Timestamp") => Type::TIMESTAMP,
        t if t.starts_with("Time64") || t.starts_with("Time32") => Type::TIME,
        _ => Type::TEXT,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn param(value: Value) -> ParamValue {
        ParamValue {
            arrow_type: "Utf8".into(),
            value,
        }
    }

    #[test]
    fn binds_by_the_statements_type_not_the_arrow_hint() {
        assert!(bind(0, &param(Value::from(3)), &Type::FLOAT8, None).is_ok());
        assert!(bind(0, &param(Value::from("2024-02-03")), &Type::DATE, None).is_ok());
        assert!(bind(
            0,
            &param(Value::from("2024-02-03T04:05:06Z")),
            &Type::TIMESTAMPTZ,
            None
        )
        .is_ok());
        assert!(bind(
            0,
            &param(Value::from("2024-02-03 04:05:06.5")),
            &Type::TIMESTAMP,
            None
        )
        .is_ok());
        assert!(bind(0, &param(Value::Null), &Type::INT8, None).is_ok());
        assert!(bind(0, &param(Value::Null), &Type::TIMESTAMPTZ, None).is_ok());
    }

    #[test]
    fn a_zone_less_timestamp_bound_as_timestamptz_is_read_in_the_session_zone() {
        use chrono::NaiveDate;
        // 2024-01-01 00:00 wall clock in Los Angeles is 08:00 UTC; with no session zone it is UTC itself
        let naive = NaiveDate::from_ymd_opt(2024, 1, 1)
            .unwrap()
            .and_hms_opt(0, 0, 0)
            .unwrap();
        let la = zoned(naive, Some("America/Los_Angeles")).unwrap();
        assert_eq!(la.to_rfc3339(), "2024-01-01T08:00:00+00:00");
        let utc = zoned(naive, None).unwrap();
        assert_eq!(utc.to_rfc3339(), "2024-01-01T00:00:00+00:00");
        assert!(zoned(naive, Some("Nowhere/Land")).is_none());
    }

    #[test]
    fn mismatches_are_invalid_requests_naming_the_parameter() {
        let err = bind(1, &param(Value::from("nope")), &Type::INT8, None).unwrap_err();
        assert_eq!(err.kind, ErrorKind::InvalidRequest);
        assert!(err.message.contains("$2"), "{}", err.message);
    }
}

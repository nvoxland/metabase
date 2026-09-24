//! Bind parameter values to the types Postgres inferred for the prepared statement.

use chrono::{DateTime, NaiveDate, NaiveDateTime, NaiveTime, Utc};
use engine_core::api::ParamValue;
use engine_core::error::{EngineError, ErrorKind, Result};
use serde_json::Value;
use tokio_postgres::types::{ToSql, Type};

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
pub fn bind(index: usize, param: &ParamValue, ty: &Type) -> Result<BoundParam> {
    let v = &param.value;
    if v.is_null() {
        return Ok(Box::new(None::<String>) as BoundParam);
    }
    let bound: BoundParam = match *ty {
        Type::BOOL => Box::new(v.as_bool().ok_or_else(|| invalid(index, ty, v))?),
        Type::INT2 => Box::new(v.as_i64().ok_or_else(|| invalid(index, ty, v))? as i16),
        Type::INT4 => Box::new(v.as_i64().ok_or_else(|| invalid(index, ty, v))? as i32),
        Type::INT8 => Box::new(v.as_i64().ok_or_else(|| invalid(index, ty, v))?),
        Type::FLOAT4 => Box::new(v.as_f64().ok_or_else(|| invalid(index, ty, v))? as f32),
        Type::FLOAT8 => Box::new(v.as_f64().ok_or_else(|| invalid(index, ty, v))?),
        Type::NUMERIC => {
            let s = as_string(v).ok_or_else(|| invalid(index, ty, v))?;
            Box::new(
                s.parse::<rust_decimal::Decimal>()
                    .map_err(|_| invalid(index, ty, v))?,
            )
        }
        Type::DATE => {
            let s = v.as_str().ok_or_else(|| invalid(index, ty, v))?;
            Box::new(
                NaiveDate::parse_from_str(s.trim(), "%Y-%m-%d")
                    .map_err(|_| invalid(index, ty, v))?,
            )
        }
        Type::TIMESTAMP => {
            let s = v.as_str().ok_or_else(|| invalid(index, ty, v))?;
            Box::new(parse_timestamp(s).ok_or_else(|| invalid(index, ty, v))?)
        }
        Type::TIMESTAMPTZ => {
            let s = v.as_str().ok_or_else(|| invalid(index, ty, v))?;
            let naive = parse_timestamp(s).ok_or_else(|| invalid(index, ty, v))?;
            Box::new(DateTime::<Utc>::from_naive_utc_and_offset(naive, Utc))
        }
        Type::TIME => {
            let s = v.as_str().ok_or_else(|| invalid(index, ty, v))?;
            let t = NaiveTime::parse_from_str(s.trim(), "%H:%M:%S%.f")
                .or_else(|_| NaiveTime::parse_from_str(s.trim(), "%H:%M:%S"))
                .map_err(|_| invalid(index, ty, v))?;
            Box::new(t)
        }
        Type::UUID => {
            let s = v.as_str().ok_or_else(|| invalid(index, ty, v))?;
            Box::new(s.parse::<uuid::Uuid>().map_err(|_| invalid(index, ty, v))?)
        }
        Type::JSON | Type::JSONB => Box::new(v.clone()),
        Type::TEXT | Type::VARCHAR | Type::BPCHAR | Type::NAME | Type::UNKNOWN => {
            Box::new(as_string(v).ok_or_else(|| invalid(index, ty, v))?)
        }
        _ => {
            // Anything else goes through text; Postgres coerces `unknown` literals by context.
            Box::new(as_string(v).ok_or_else(|| invalid(index, ty, v))?)
        }
    };
    Ok(bound)
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
        assert!(bind(0, &param(Value::from(3)), &Type::FLOAT8).is_ok());
        assert!(bind(0, &param(Value::from("2024-02-03")), &Type::DATE).is_ok());
        assert!(bind(
            0,
            &param(Value::from("2024-02-03T04:05:06Z")),
            &Type::TIMESTAMPTZ
        )
        .is_ok());
        assert!(bind(
            0,
            &param(Value::from("2024-02-03 04:05:06.5")),
            &Type::TIMESTAMP
        )
        .is_ok());
        assert!(bind(0, &param(Value::Null), &Type::INT8).is_ok());
    }

    #[test]
    fn mismatches_are_invalid_requests_naming_the_parameter() {
        let err = bind(1, &param(Value::from("nope")), &Type::INT8).unwrap_err();
        assert_eq!(err.kind, ErrorKind::InvalidRequest);
        assert!(err.message.contains("$2"), "{}", err.message);
    }
}

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
pub fn bind(index: usize, param: &ParamValue, ty: &Type) -> Result<BoundParam> {
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
            Box::new(DateTime::<Utc>::from_naive_utc_and_offset(naive, Utc))
        }
        Type::TIME => {
            let s = v.as_str().ok_or_else(|| invalid(index, ty, v))?;
            let t = ["%H:%M:%S%.f", "%H:%M:%S", "%H:%M"]
                .iter()
                .find_map(|fmt| NaiveTime::parse_from_str(s.trim(), fmt).ok())
                .ok_or_else(|| invalid(index, ty, v))?;
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
        assert!(bind(0, &param(Value::Null), &Type::TIMESTAMPTZ).is_ok());
    }

    #[test]
    fn mismatches_are_invalid_requests_naming_the_parameter() {
        let err = bind(1, &param(Value::from("nope")), &Type::INT8).unwrap_err();
        assert_eq!(err.kind, ErrorKind::InvalidRequest);
        assert!(err.message.contains("$2"), "{}", err.message);
    }
}

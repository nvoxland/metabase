//! Bound parameters. MySQL infers nothing about a placeholder's type, so the Arrow type Metabase
//! attaches to each value decides how it is sent.

use arrow::array::timezone::Tz;
use chrono::{DateTime, NaiveDate, NaiveDateTime, NaiveTime, TimeZone, Utc};
use engine_core::api::ParamValue;
use engine_core::error::{EngineError, ErrorKind, Result};
use serde_json::Value as Json;

fn invalid(index: usize, param: &ParamValue) -> EngineError {
    EngineError::new(
        ErrorKind::InvalidRequest,
        format!(
            "parameter ${} ({}) can't be bound from {}",
            index + 1,
            param.arrow_type,
            param.value
        ),
    )
}

fn as_i64(v: &Json) -> Option<i64> {
    match v {
        Json::Number(n) => n.as_i64().or_else(|| n.as_f64().map(|f| f as i64)),
        Json::String(s) => s.trim().parse().ok(),
        Json::Bool(b) => Some(*b as i64),
        _ => None,
    }
}

fn as_f64(v: &Json) -> Option<f64> {
    match v {
        Json::Number(n) => n.as_f64(),
        Json::String(s) => s.trim().parse().ok(),
        _ => None,
    }
}

fn as_bool(v: &Json) -> Option<bool> {
    match v {
        Json::Bool(b) => Some(*b),
        Json::Number(n) => n.as_i64().map(|i| i != 0),
        Json::String(s) => match s.trim().to_ascii_lowercase().as_str() {
            "true" | "t" | "1" | "yes" => Some(true),
            "false" | "f" | "0" | "no" => Some(false),
            _ => None,
        },
        _ => None,
    }
}

fn as_text(v: &Json) -> Option<String> {
    match v {
        Json::String(s) => Some(s.clone()),
        Json::Number(n) => Some(n.to_string()),
        Json::Bool(b) => Some(b.to_string()),
        _ => None,
    }
}

/// A timestamp as Metabase writes them: `YYYY-MM-DD HH:MM[:SS[.fff]]` or with a `T`, and an optional
/// `Z` or `+HH:MM` offset. Returns the wall-clock time and, when one was given, the offset in
/// seconds east of UTC.
pub fn parse_timestamp(s: &str) -> Option<(NaiveDateTime, Option<i32>)> {
    let s = s.trim();
    if let Ok(dt) = DateTime::parse_from_rfc3339(s) {
        return Some((dt.naive_local(), Some(dt.offset().local_minus_utc())));
    }
    for fmt in [
        "%Y-%m-%dT%H:%M:%S%.f%:z",
        "%Y-%m-%d %H:%M:%S%.f%:z",
        "%Y-%m-%dT%H:%M:%S%:z",
        "%Y-%m-%d %H:%M:%S%:z",
    ] {
        if let Ok(dt) = DateTime::parse_from_str(s, fmt) {
            return Some((dt.naive_local(), Some(dt.offset().local_minus_utc())));
        }
    }
    let naive = s.strip_suffix('Z').map(|n| (n, true)).unwrap_or((s, false));
    for fmt in [
        "%Y-%m-%dT%H:%M:%S%.f",
        "%Y-%m-%d %H:%M:%S%.f",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%dT%H:%M",
        "%Y-%m-%d %H:%M",
    ] {
        if let Ok(t) = NaiveDateTime::parse_from_str(naive.0, fmt) {
            return Some((t, if naive.1 { Some(0) } else { None }));
        }
    }
    NaiveDate::parse_from_str(naive.0, "%Y-%m-%d")
        .ok()
        .map(|d| (d.and_hms_opt(0, 0, 0).unwrap(), None))
}

fn mysql_datetime(t: NaiveDateTime) -> mysql_async::Value {
    use chrono::{Datelike, Timelike};
    mysql_async::Value::Date(
        t.year() as u16,
        t.month() as u8,
        t.day() as u8,
        t.hour() as u8,
        t.minute() as u8,
        t.second() as u8,
        t.and_utc().timestamp_subsec_micros(),
    )
}

/// The wall-clock time in `session_tz` of a zoned value: an explicit offset is honored, a bare
/// wall-clock time is read in the session zone (the report time zone) as the warehouse would.
fn in_session_zone(
    naive: NaiveDateTime,
    offset: Option<i32>,
    session_tz: &Tz,
) -> Option<NaiveDateTime> {
    let instant: DateTime<Utc> = match offset {
        Some(seconds) => {
            let fixed = chrono::FixedOffset::east_opt(seconds)?;
            fixed
                .from_local_datetime(&naive)
                .single()?
                .with_timezone(&Utc)
        }
        None => session_tz
            .from_local_datetime(&naive)
            .earliest()?
            .with_timezone(&Utc),
    };
    Some(instant.with_timezone(session_tz).naive_local())
}

/// Convert one parameter for MySQL. `session_tz` is the zone the connection runs in, which is how
/// a zoned timestamp has to be spelled for it.
pub fn bind(index: usize, param: &ParamValue, session_tz: &Tz) -> Result<mysql_async::Value> {
    use mysql_async::Value;
    let v = &param.value;
    if v.is_null() {
        return Ok(Value::NULL);
    }
    let t = param.arrow_type.as_str();
    let bad = || invalid(index, param);
    Ok(if t.starts_with("Int") || t.starts_with("UInt") {
        Value::Int(as_i64(v).ok_or_else(bad)?)
    } else if t.starts_with("Float") {
        Value::Double(as_f64(v).ok_or_else(bad)?)
    } else if t == "Boolean" {
        Value::Int(as_bool(v).ok_or_else(bad)? as i64)
    } else if t.starts_with("Decimal") {
        Value::Bytes(as_text(v).ok_or_else(bad)?.into_bytes())
    } else if t.starts_with("Date") {
        let s = v.as_str().ok_or_else(bad)?;
        let (t, _) = parse_timestamp(s).ok_or_else(bad)?;
        use chrono::Datelike;
        Value::Date(t.year() as u16, t.month() as u8, t.day() as u8, 0, 0, 0, 0)
    } else if t.starts_with("Timestamp") {
        let s = v.as_str().ok_or_else(bad)?;
        let (naive, offset) = parse_timestamp(s).ok_or_else(bad)?;
        let zoned = t.contains("Some(");
        if zoned || offset.is_some() {
            mysql_datetime(in_session_zone(naive, offset, session_tz).ok_or_else(bad)?)
        } else {
            mysql_datetime(naive)
        }
    } else if t.starts_with("Time") {
        let s = v.as_str().ok_or_else(bad)?;
        let time = ["%H:%M:%S%.f", "%H:%M:%S", "%H:%M"]
            .iter()
            .find_map(|fmt| NaiveTime::parse_from_str(s.trim(), fmt).ok())
            .ok_or_else(bad)?;
        use chrono::Timelike;
        Value::Time(
            false,
            0,
            time.hour() as u8,
            time.minute() as u8,
            time.second() as u8,
            time.nanosecond() / 1000,
        )
    } else {
        Value::Bytes(as_text(v).ok_or_else(bad)?.into_bytes())
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn param(arrow_type: &str, value: Json) -> ParamValue {
        ParamValue {
            arrow_type: arrow_type.into(),
            value,
        }
    }

    fn utc() -> Tz {
        "UTC".parse().unwrap()
    }

    #[test]
    fn binds_by_the_arrow_type() {
        use mysql_async::Value;
        assert_eq!(
            bind(0, &param("Int64", Json::from(3)), &utc()).unwrap(),
            Value::Int(3)
        );
        assert_eq!(
            bind(0, &param("Int64", Json::from("42")), &utc()).unwrap(),
            Value::Int(42)
        );
        assert_eq!(
            bind(0, &param("Boolean", Json::from(true)), &utc()).unwrap(),
            Value::Int(1)
        );
        assert_eq!(
            bind(0, &param("Utf8", Json::from("x")), &utc()).unwrap(),
            Value::Bytes(b"x".to_vec())
        );
        assert_eq!(
            bind(0, &param("Date32", Json::from("2024-02-03")), &utc()).unwrap(),
            Value::Date(2024, 2, 3, 0, 0, 0, 0)
        );
        assert_eq!(
            bind(0, &param("Utf8", Json::Null), &utc()).unwrap(),
            Value::NULL
        );
        assert_eq!(
            bind(0, &param("Int64", Json::from("nope")), &utc())
                .unwrap_err()
                .kind,
            ErrorKind::InvalidRequest
        );
    }

    #[test]
    fn zoned_timestamps_are_spelled_in_the_session_zone() {
        use mysql_async::Value;
        let la: Tz = "America/Los_Angeles".parse().unwrap();
        // an instant: 08:00Z is midnight in Los Angeles
        assert_eq!(
            bind(
                0,
                &param(
                    "Timestamp(Microsecond, Some(\"UTC\"))",
                    Json::from("2024-01-01T08:00:00Z")
                ),
                &la
            )
            .unwrap(),
            Value::Date(2024, 1, 1, 0, 0, 0, 0)
        );
        // a bare wall-clock time typed as zoned is read in the session zone: it stays as written
        assert_eq!(
            bind(
                0,
                &param(
                    "Timestamp(Microsecond, Some(\"UTC\"))",
                    Json::from("2024-01-01 00:00:00")
                ),
                &la
            )
            .unwrap(),
            Value::Date(2024, 1, 1, 0, 0, 0, 0)
        );
        // a zone-less timestamp is a wall-clock time whatever the session zone
        assert_eq!(
            bind(
                0,
                &param(
                    "Timestamp(Microsecond, None)",
                    Json::from("2024-01-01T08:00:00")
                ),
                &la
            )
            .unwrap(),
            Value::Date(2024, 1, 1, 8, 0, 0, 0)
        );
    }
}

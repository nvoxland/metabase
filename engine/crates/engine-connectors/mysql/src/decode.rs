//! MySQL result columns and rows -> Arrow. Rows arrive through the binary protocol (every
//! statement is prepared), so values come typed; text is decoded as UTF-8, which the connection
//! negotiates.

use std::sync::Arc;

use arrow::array::timezone::Tz;
use arrow::array::{
    ArrayRef, BinaryBuilder, BooleanBuilder, Date32Builder, Decimal128Builder, Float32Builder,
    Float64Builder, Int16Builder, Int32Builder, Int64Builder, StringBuilder,
    Time64MicrosecondBuilder, TimestampMicrosecondBuilder, UInt64Builder,
};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use arrow::record_batch::RecordBatch;
use chrono::{NaiveDate, NaiveDateTime, TimeZone};
use engine_core::error::{EngineError, ErrorKind, Result};
use mysql_async::consts::{ColumnFlags, ColumnType};
use mysql_async::{Column, Row, Value};

const DECIMAL_PRECISION: u8 = 38;
const BINARY_CHARSET: u16 = 63;

fn epoch_date() -> NaiveDate {
    NaiveDate::from_ymd_opt(1970, 1, 1).unwrap()
}

fn decode_error(column: &str, what: impl std::fmt::Display) -> EngineError {
    EngineError::new(
        ErrorKind::Warehouse,
        format!("decoding column {column:?}: {what}"),
    )
}

/// The Arrow type a result column decodes to.
pub fn arrow_type_for(column: &Column) -> DataType {
    let flags = column.flags();
    let unsigned = flags.contains(ColumnFlags::UNSIGNED_FLAG);
    match column.column_type() {
        // `tinyint(1)` is how MySQL spells boolean, and how Metabase syncs it
        ColumnType::MYSQL_TYPE_TINY if column.column_length() == 1 => DataType::Boolean,
        ColumnType::MYSQL_TYPE_TINY | ColumnType::MYSQL_TYPE_SHORT if !unsigned => DataType::Int16,
        ColumnType::MYSQL_TYPE_TINY
        | ColumnType::MYSQL_TYPE_SHORT
        | ColumnType::MYSQL_TYPE_INT24
        | ColumnType::MYSQL_TYPE_YEAR => DataType::Int32,
        ColumnType::MYSQL_TYPE_LONG if !unsigned => DataType::Int32,
        ColumnType::MYSQL_TYPE_LONG => DataType::Int64,
        ColumnType::MYSQL_TYPE_LONGLONG if !unsigned => DataType::Int64,
        ColumnType::MYSQL_TYPE_LONGLONG => DataType::UInt64,
        ColumnType::MYSQL_TYPE_FLOAT => DataType::Float32,
        ColumnType::MYSQL_TYPE_DOUBLE => DataType::Float64,
        ColumnType::MYSQL_TYPE_DECIMAL | ColumnType::MYSQL_TYPE_NEWDECIMAL => DataType::Decimal128(
            DECIMAL_PRECISION,
            column.decimals().min(DECIMAL_PRECISION) as i8,
        ),
        ColumnType::MYSQL_TYPE_DATE | ColumnType::MYSQL_TYPE_NEWDATE => DataType::Date32,
        // shown in the session's zone; decoded back to instants
        ColumnType::MYSQL_TYPE_TIMESTAMP | ColumnType::MYSQL_TYPE_TIMESTAMP2 => {
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()))
        }
        ColumnType::MYSQL_TYPE_DATETIME | ColumnType::MYSQL_TYPE_DATETIME2 => {
            DataType::Timestamp(TimeUnit::Microsecond, None)
        }
        ColumnType::MYSQL_TYPE_TIME | ColumnType::MYSQL_TYPE_TIME2 => {
            DataType::Time64(TimeUnit::Microsecond)
        }
        // `bit(1)` is the other boolean
        ColumnType::MYSQL_TYPE_BIT if column.column_length() == 1 => DataType::Boolean,
        ColumnType::MYSQL_TYPE_BIT
        | ColumnType::MYSQL_TYPE_GEOMETRY
        | ColumnType::MYSQL_TYPE_VECTOR => DataType::Binary,
        ColumnType::MYSQL_TYPE_NULL => DataType::Null,
        ColumnType::MYSQL_TYPE_JSON => DataType::Utf8,
        // strings and blobs share types; the character set tells them apart
        _ if column.character_set() == BINARY_CHARSET
            && flags.contains(ColumnFlags::BINARY_FLAG) =>
        {
            DataType::Binary
        }
        _ => DataType::Utf8,
    }
}

enum ColumnBuilder {
    Bool(BooleanBuilder),
    Int16(Int16Builder),
    Int32(Int32Builder),
    Int64(Int64Builder),
    UInt64(UInt64Builder),
    Float32(Float32Builder),
    Float64(Float64Builder),
    Decimal(Decimal128Builder, i8),
    Utf8(StringBuilder),
    Binary(BinaryBuilder),
    Date(Date32Builder),
    DateTime(TimestampMicrosecondBuilder),
    Timestamp(TimestampMicrosecondBuilder),
    Time(Time64MicrosecondBuilder),
    Null(usize),
}

fn as_i64(value: &Value) -> Option<i64> {
    match value {
        Value::Int(i) => Some(*i),
        Value::UInt(u) => i64::try_from(*u).ok(),
        Value::Bytes(b) => std::str::from_utf8(b).ok()?.trim().parse().ok(),
        Value::Double(d) => Some(*d as i64),
        Value::Float(f) => Some(*f as i64),
        _ => None,
    }
}

fn as_f64(value: &Value) -> Option<f64> {
    match value {
        Value::Int(i) => Some(*i as f64),
        Value::UInt(u) => Some(*u as f64),
        Value::Double(d) => Some(*d),
        Value::Float(f) => Some(*f as f64),
        Value::Bytes(b) => std::str::from_utf8(b).ok()?.trim().parse().ok(),
        _ => None,
    }
}

fn as_datetime(value: &Value) -> Option<Option<NaiveDateTime>> {
    match value {
        // MySQL's zero date has no Java or Arrow meaning; the JDBC path reads it as NULL too
        Value::Date(0, 0, 0, ..) => Some(None),
        Value::Date(y, m, d, h, i, s, us) => Some(
            NaiveDate::from_ymd_opt(*y as i32, *m as u32, *d as u32)?
                .and_hms_micro_opt(*h as u32, *i as u32, *s as u32, *us)
                .map(Some)?,
        ),
        Value::Bytes(b) => {
            let s = std::str::from_utf8(b).ok()?;
            if s.starts_with("0000-00-00") {
                return Some(None);
            }
            crate::params::parse_timestamp(s).map(|(t, _)| Some(t))
        }
        _ => None,
    }
}

/// `text` as a scaled decimal integer with `scale` fractional digits.
fn parse_decimal(text: &str, scale: i8) -> Option<i128> {
    let text = text.trim();
    let (negative, digits) = match text.strip_prefix('-') {
        Some(rest) => (true, rest),
        None => (false, text.strip_prefix('+').unwrap_or(text)),
    };
    let (whole, frac) = digits.split_once('.').unwrap_or((digits, ""));
    let scale = scale as usize;
    let frac: String = if frac.len() >= scale {
        frac[..scale].to_string()
    } else {
        format!("{frac:0<scale$}")
    };
    let mut n: i128 = if whole.is_empty() {
        0
    } else {
        whole.parse().ok()?
    };
    n = n.checked_mul(10i128.checked_pow(scale as u32)?)?;
    if !frac.is_empty() {
        n = n.checked_add(frac.parse::<i128>().ok()?)?;
    }
    Some(if negative { -n } else { n })
}

impl ColumnBuilder {
    fn new(data_type: &DataType, capacity: usize) -> ColumnBuilder {
        match data_type {
            DataType::Boolean => ColumnBuilder::Bool(BooleanBuilder::with_capacity(capacity)),
            DataType::Int16 => ColumnBuilder::Int16(Int16Builder::with_capacity(capacity)),
            DataType::Int32 => ColumnBuilder::Int32(Int32Builder::with_capacity(capacity)),
            DataType::Int64 => ColumnBuilder::Int64(Int64Builder::with_capacity(capacity)),
            DataType::UInt64 => ColumnBuilder::UInt64(UInt64Builder::with_capacity(capacity)),
            DataType::Float32 => ColumnBuilder::Float32(Float32Builder::with_capacity(capacity)),
            DataType::Float64 => ColumnBuilder::Float64(Float64Builder::with_capacity(capacity)),
            DataType::Decimal128(p, s) => ColumnBuilder::Decimal(
                Decimal128Builder::with_capacity(capacity)
                    .with_data_type(DataType::Decimal128(*p, *s)),
                *s,
            ),
            DataType::Binary => {
                ColumnBuilder::Binary(BinaryBuilder::with_capacity(capacity, capacity * 16))
            }
            DataType::Date32 => ColumnBuilder::Date(Date32Builder::with_capacity(capacity)),
            DataType::Timestamp(_, None) => {
                ColumnBuilder::DateTime(TimestampMicrosecondBuilder::with_capacity(capacity))
            }
            DataType::Timestamp(_, Some(_)) => ColumnBuilder::Timestamp(
                TimestampMicrosecondBuilder::with_capacity(capacity).with_timezone("UTC"),
            ),
            DataType::Time64(_) => {
                ColumnBuilder::Time(Time64MicrosecondBuilder::with_capacity(capacity))
            }
            DataType::Null => ColumnBuilder::Null(0),
            _ => ColumnBuilder::Utf8(StringBuilder::with_capacity(capacity, capacity * 16)),
        }
    }

    fn append(&mut self, value: &Value, column: &str, session_tz: &Tz) -> Result<()> {
        if matches!(value, Value::NULL) {
            match self {
                ColumnBuilder::Bool(b) => b.append_null(),
                ColumnBuilder::Int16(b) => b.append_null(),
                ColumnBuilder::Int32(b) => b.append_null(),
                ColumnBuilder::Int64(b) => b.append_null(),
                ColumnBuilder::UInt64(b) => b.append_null(),
                ColumnBuilder::Float32(b) => b.append_null(),
                ColumnBuilder::Float64(b) => b.append_null(),
                ColumnBuilder::Decimal(b, _) => b.append_null(),
                ColumnBuilder::Utf8(b) => b.append_null(),
                ColumnBuilder::Binary(b) => b.append_null(),
                ColumnBuilder::Date(b) => b.append_null(),
                ColumnBuilder::DateTime(b) => b.append_null(),
                ColumnBuilder::Timestamp(b) => b.append_null(),
                ColumnBuilder::Time(b) => b.append_null(),
                ColumnBuilder::Null(n) => *n += 1,
            }
            return Ok(());
        }
        let bad = |what: &str| decode_error(column, format!("{what} from {value:?}"));
        match self {
            ColumnBuilder::Bool(b) => {
                let v = match value {
                    Value::Bytes(bytes) => bytes.iter().any(|x| *x != 0),
                    other => as_i64(other).ok_or_else(|| bad("boolean"))? != 0,
                };
                b.append_value(v)
            }
            ColumnBuilder::Int16(b) => b.append_value(
                i16::try_from(as_i64(value).ok_or_else(|| bad("integer"))?)
                    .map_err(|_| bad("16-bit integer"))?,
            ),
            ColumnBuilder::Int32(b) => b.append_value(
                i32::try_from(as_i64(value).ok_or_else(|| bad("integer"))?)
                    .map_err(|_| bad("32-bit integer"))?,
            ),
            ColumnBuilder::Int64(b) => b.append_value(as_i64(value).ok_or_else(|| bad("integer"))?),
            ColumnBuilder::UInt64(b) => b.append_value(match value {
                Value::UInt(u) => *u,
                other => u64::try_from(as_i64(other).ok_or_else(|| bad("integer"))?)
                    .map_err(|_| bad("unsigned integer"))?,
            }),
            ColumnBuilder::Float32(b) => {
                b.append_value(as_f64(value).ok_or_else(|| bad("number"))? as f32)
            }
            ColumnBuilder::Float64(b) => {
                b.append_value(as_f64(value).ok_or_else(|| bad("number"))?)
            }
            ColumnBuilder::Decimal(b, scale) => {
                let text = match value {
                    Value::Bytes(bytes) => String::from_utf8_lossy(bytes).into_owned(),
                    Value::Int(i) => i.to_string(),
                    Value::UInt(u) => u.to_string(),
                    Value::Double(d) => d.to_string(),
                    Value::Float(f) => f.to_string(),
                    _ => return Err(bad("decimal")),
                };
                b.append_value(parse_decimal(&text, *scale).ok_or_else(|| bad("decimal"))?)
            }
            ColumnBuilder::Utf8(b) => match value {
                Value::Bytes(bytes) => {
                    b.append_value(std::str::from_utf8(bytes).map_err(|_| bad("UTF-8 text"))?)
                }
                Value::Int(i) => b.append_value(i.to_string()),
                Value::UInt(u) => b.append_value(u.to_string()),
                Value::Double(d) => b.append_value(d.to_string()),
                Value::Float(f) => b.append_value(f.to_string()),
                Value::Date(..) => b.append_value(
                    as_datetime(value)
                        .flatten()
                        .map(|t| t.to_string())
                        .unwrap_or_default(),
                ),
                other => b.append_value(format!("{other:?}")),
            },
            ColumnBuilder::Binary(b) => match value {
                Value::Bytes(bytes) => b.append_value(bytes),
                _ => return Err(bad("bytes")),
            },
            ColumnBuilder::Date(b) => match as_datetime(value).ok_or_else(|| bad("date"))? {
                Some(t) => b.append_value((t.date() - epoch_date()).num_days() as i32),
                None => b.append_null(),
            },
            ColumnBuilder::DateTime(b) => {
                match as_datetime(value).ok_or_else(|| bad("datetime"))? {
                    Some(t) => b.append_value(t.and_utc().timestamp_micros()),
                    None => b.append_null(),
                }
            }
            ColumnBuilder::Timestamp(b) => {
                match as_datetime(value).ok_or_else(|| bad("timestamp"))? {
                    Some(local) => {
                        let instant = session_tz
                            .from_local_datetime(&local)
                            .earliest()
                            .ok_or_else(|| bad("timestamp in the session time zone"))?;
                        b.append_value(instant.timestamp_micros())
                    }
                    None => b.append_null(),
                }
            }
            ColumnBuilder::Time(b) => match value {
                Value::Time(negative, days, h, m, s, us) => {
                    let micros = (((*days as i64 * 24 + *h as i64) * 60 + *m as i64) * 60
                        + *s as i64)
                        * 1_000_000
                        + *us as i64;
                    if *negative || micros >= 86_400_000_000 {
                        return Err(bad("time of day (MySQL times can exceed a day)"));
                    }
                    b.append_value(micros)
                }
                _ => return Err(bad("time")),
            },
            ColumnBuilder::Null(n) => *n += 1,
        }
        Ok(())
    }

    fn finish(&mut self) -> ArrayRef {
        match self {
            ColumnBuilder::Bool(b) => Arc::new(b.finish()),
            ColumnBuilder::Int16(b) => Arc::new(b.finish()),
            ColumnBuilder::Int32(b) => Arc::new(b.finish()),
            ColumnBuilder::Int64(b) => Arc::new(b.finish()),
            ColumnBuilder::UInt64(b) => Arc::new(b.finish()),
            ColumnBuilder::Float32(b) => Arc::new(b.finish()),
            ColumnBuilder::Float64(b) => Arc::new(b.finish()),
            ColumnBuilder::Decimal(b, _) => Arc::new(b.finish()),
            ColumnBuilder::Utf8(b) => Arc::new(b.finish()),
            ColumnBuilder::Binary(b) => Arc::new(b.finish()),
            ColumnBuilder::Date(b) => Arc::new(b.finish()),
            ColumnBuilder::DateTime(b) => Arc::new(b.finish()),
            ColumnBuilder::Timestamp(b) => Arc::new(b.finish()),
            ColumnBuilder::Time(b) => Arc::new(b.finish()),
            ColumnBuilder::Null(n) => {
                let len = *n;
                *n = 0;
                Arc::new(arrow::array::NullArray::new(len))
            }
        }
    }
}

/// Accumulates rows into Arrow batches.
pub struct RowDecoder {
    schema: SchemaRef,
    names: Vec<String>,
    builders: Vec<ColumnBuilder>,
    rows: usize,
    batch_size: usize,
    session_tz: Tz,
}

impl RowDecoder {
    pub fn new(columns: &[Column], batch_size: usize, session_tz: Tz) -> RowDecoder {
        let mut fields = Vec::with_capacity(columns.len());
        let mut builders = Vec::with_capacity(columns.len());
        let mut names = Vec::with_capacity(columns.len());
        for column in columns {
            let data_type = arrow_type_for(column);
            let name = column.name_str().into_owned();
            fields.push(Field::new(&name, data_type.clone(), true));
            builders.push(ColumnBuilder::new(&data_type, batch_size));
            names.push(name);
        }
        RowDecoder {
            schema: Arc::new(Schema::new(fields)),
            names,
            builders,
            rows: 0,
            batch_size,
            session_tz,
        }
    }

    pub fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    pub fn push(&mut self, row: &Row) -> Result<()> {
        for (i, builder) in self.builders.iter_mut().enumerate() {
            let value = row.as_ref(i).unwrap_or(&Value::NULL);
            builder.append(value, &self.names[i], &self.session_tz)?;
        }
        self.rows += 1;
        Ok(())
    }

    pub fn is_full(&self) -> bool {
        self.rows >= self.batch_size
    }

    /// The rows so far as a batch, or None when there are none.
    pub fn flush(&mut self) -> Result<Option<RecordBatch>> {
        if self.rows == 0 {
            return Ok(None);
        }
        let columns: Vec<ArrayRef> = self.builders.iter_mut().map(|b| b.finish()).collect();
        self.rows = 0;
        let options = arrow::record_batch::RecordBatchOptions::new()
            .with_row_count(Some(columns.first().map(|c| c.len()).unwrap_or(0)));
        RecordBatch::try_new_with_options(Arc::clone(&self.schema), columns, &options)
            .map(Some)
            .map_err(|e| EngineError::new(ErrorKind::Warehouse, format!("building batch: {e}")))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Array;

    #[test]
    fn decimals_scale_by_the_column_scale() {
        assert_eq!(parse_decimal("123.45", 2), Some(12345));
        assert_eq!(parse_decimal("-0.5", 2), Some(-50));
        assert_eq!(parse_decimal("7", 3), Some(7000));
        assert_eq!(parse_decimal("1.23456", 2), Some(123));
        assert_eq!(parse_decimal("x", 2), None);
    }

    #[test]
    fn zero_dates_are_null_and_timestamps_are_read_in_the_session_zone() {
        let la: Tz = "America/Los_Angeles".parse().unwrap();
        let mut ts = ColumnBuilder::new(
            &DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            4,
        );
        ts.append(&Value::Date(2024, 1, 1, 0, 0, 0, 0), "t", &la)
            .unwrap();
        ts.append(&Value::Date(0, 0, 0, 0, 0, 0, 0), "t", &la)
            .unwrap();
        let array = ts.finish();
        let array = array
            .as_any()
            .downcast_ref::<arrow::array::TimestampMicrosecondArray>()
            .unwrap();
        assert_eq!(array.value(0), 1_704_096_000_000_000); // 2024-01-01T08:00:00Z
        assert!(array.is_null(1));

        let mut naive = ColumnBuilder::new(&DataType::Timestamp(TimeUnit::Microsecond, None), 4);
        naive
            .append(&Value::Date(2024, 1, 1, 0, 0, 0, 0), "t", &la)
            .unwrap();
        let array = naive.finish();
        let array = array
            .as_any()
            .downcast_ref::<arrow::array::TimestampMicrosecondArray>()
            .unwrap();
        assert_eq!(array.value(0), 1_704_067_200_000_000); // the wall clock itself
    }

    #[test]
    fn a_time_past_midnight_is_an_error_not_a_wrap() {
        let utc: Tz = "UTC".parse().unwrap();
        let mut time = ColumnBuilder::new(&DataType::Time64(TimeUnit::Microsecond), 2);
        time.append(&Value::Time(false, 0, 13, 30, 0, 0), "t", &utc)
            .unwrap();
        assert!(time
            .append(&Value::Time(false, 1, 1, 0, 0, 0), "t", &utc)
            .is_err());
        assert!(time
            .append(&Value::Time(true, 0, 1, 0, 0, 0), "t", &utc)
            .is_err());
    }
}

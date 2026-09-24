//! Postgres rows -> Arrow record batches.
//!
//! Timestamps decode at microsecond precision (Postgres' own). `timestamptz` becomes a UTC
//! instant; `timestamp` stays naive. `numeric` decodes to `Decimal128(38, 10)`. Types this decoder
//! doesn't handle are cast to text on the SQL side before the statement runs (see `lib.rs`), so
//! every column that reaches here has a known shape.

use std::sync::Arc;

use arrow::array::{
    ArrayRef, BinaryBuilder, BooleanBuilder, Date32Builder, Decimal128Builder, Float32Builder,
    Float64Builder, Int16Builder, Int32Builder, Int64Builder, StringBuilder,
    Time64MicrosecondBuilder, TimestampMicrosecondBuilder, UInt32Builder,
};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use arrow::record_batch::RecordBatch;
use chrono::{DateTime, NaiveDate, NaiveDateTime, NaiveTime, Timelike, Utc};
use engine_core::error::{EngineError, ErrorKind, Result};
use tokio_postgres::types::Type;
use tokio_postgres::Row;

pub const DECIMAL_PRECISION: u8 = 38;
pub const DECIMAL_SCALE: i8 = 10;

/// Which Postgres types decode natively. Everything else is cast to text first.
pub fn arrow_type_for(ty: &Type) -> Option<DataType> {
    Some(match *ty {
        Type::BOOL => DataType::Boolean,
        Type::INT2 => DataType::Int16,
        Type::INT4 => DataType::Int32,
        Type::INT8 => DataType::Int64,
        Type::OID => DataType::UInt32,
        Type::FLOAT4 => DataType::Float32,
        Type::FLOAT8 => DataType::Float64,
        Type::NUMERIC => DataType::Decimal128(DECIMAL_PRECISION, DECIMAL_SCALE),
        Type::TEXT | Type::VARCHAR | Type::BPCHAR | Type::NAME | Type::CHAR | Type::UNKNOWN => {
            DataType::Utf8
        }
        Type::JSON | Type::JSONB | Type::UUID => DataType::Utf8,
        Type::BYTEA => DataType::Binary,
        Type::DATE => DataType::Date32,
        Type::TIMESTAMP => DataType::Timestamp(TimeUnit::Microsecond, None),
        Type::TIMESTAMPTZ => DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
        Type::TIME => DataType::Time64(TimeUnit::Microsecond),
        _ => return None,
    })
}

enum ColumnBuilder {
    Bool(BooleanBuilder),
    Int16(Int16Builder),
    Int32(Int32Builder),
    Int64(Int64Builder),
    UInt32(UInt32Builder),
    Float32(Float32Builder),
    Float64(Float64Builder),
    Decimal(Decimal128Builder),
    Utf8(StringBuilder),
    Json(StringBuilder),
    Uuid(StringBuilder),
    Binary(BinaryBuilder),
    Date(Date32Builder),
    Timestamp(TimestampMicrosecondBuilder),
    TimestampTz(TimestampMicrosecondBuilder),
    Time(Time64MicrosecondBuilder),
}

fn epoch_date() -> NaiveDate {
    NaiveDate::from_ymd_opt(1970, 1, 1).unwrap()
}

fn decode_error(column: &str, e: tokio_postgres::Error) -> EngineError {
    EngineError::new(
        ErrorKind::Warehouse,
        format!("decoding column {column:?}: {e}"),
    )
}

impl ColumnBuilder {
    fn new(ty: &Type, capacity: usize) -> Option<ColumnBuilder> {
        Some(match *ty {
            Type::BOOL => ColumnBuilder::Bool(BooleanBuilder::with_capacity(capacity)),
            Type::INT2 => ColumnBuilder::Int16(Int16Builder::with_capacity(capacity)),
            Type::INT4 => ColumnBuilder::Int32(Int32Builder::with_capacity(capacity)),
            Type::INT8 => ColumnBuilder::Int64(Int64Builder::with_capacity(capacity)),
            Type::OID => ColumnBuilder::UInt32(UInt32Builder::with_capacity(capacity)),
            Type::FLOAT4 => ColumnBuilder::Float32(Float32Builder::with_capacity(capacity)),
            Type::FLOAT8 => ColumnBuilder::Float64(Float64Builder::with_capacity(capacity)),
            Type::NUMERIC => ColumnBuilder::Decimal(
                Decimal128Builder::with_capacity(capacity)
                    .with_data_type(DataType::Decimal128(DECIMAL_PRECISION, DECIMAL_SCALE)),
            ),
            Type::TEXT | Type::VARCHAR | Type::BPCHAR | Type::NAME | Type::CHAR | Type::UNKNOWN => {
                ColumnBuilder::Utf8(StringBuilder::with_capacity(capacity, capacity * 16))
            }
            Type::JSON | Type::JSONB => {
                ColumnBuilder::Json(StringBuilder::with_capacity(capacity, capacity * 32))
            }
            Type::UUID => {
                ColumnBuilder::Uuid(StringBuilder::with_capacity(capacity, capacity * 36))
            }
            Type::BYTEA => {
                ColumnBuilder::Binary(BinaryBuilder::with_capacity(capacity, capacity * 16))
            }
            Type::DATE => ColumnBuilder::Date(Date32Builder::with_capacity(capacity)),
            Type::TIMESTAMP => {
                ColumnBuilder::Timestamp(TimestampMicrosecondBuilder::with_capacity(capacity))
            }
            Type::TIMESTAMPTZ => ColumnBuilder::TimestampTz(
                TimestampMicrosecondBuilder::with_capacity(capacity).with_timezone("UTC"),
            ),
            Type::TIME => ColumnBuilder::Time(Time64MicrosecondBuilder::with_capacity(capacity)),
            _ => return None,
        })
    }

    fn append(&mut self, row: &Row, index: usize, column: &str) -> Result<()> {
        let err = |e| decode_error(column, e);
        match self {
            ColumnBuilder::Bool(b) => {
                b.append_option(row.try_get::<_, Option<bool>>(index).map_err(err)?)
            }
            ColumnBuilder::Int16(b) => {
                b.append_option(row.try_get::<_, Option<i16>>(index).map_err(err)?)
            }
            ColumnBuilder::Int32(b) => {
                b.append_option(row.try_get::<_, Option<i32>>(index).map_err(err)?)
            }
            ColumnBuilder::Int64(b) => {
                b.append_option(row.try_get::<_, Option<i64>>(index).map_err(err)?)
            }
            ColumnBuilder::UInt32(b) => {
                b.append_option(row.try_get::<_, Option<u32>>(index).map_err(err)?)
            }
            ColumnBuilder::Float32(b) => {
                b.append_option(row.try_get::<_, Option<f32>>(index).map_err(err)?)
            }
            ColumnBuilder::Float64(b) => {
                b.append_option(row.try_get::<_, Option<f64>>(index).map_err(err)?)
            }
            ColumnBuilder::Decimal(b) => {
                let value = row
                    .try_get::<_, Option<rust_decimal::Decimal>>(index)
                    .map_err(err)?;
                match value {
                    None => b.append_null(),
                    Some(mut d) => {
                        d.rescale(DECIMAL_SCALE as u32);
                        if d.scale() != DECIMAL_SCALE as u32 {
                            return Err(EngineError::new(
                                ErrorKind::Warehouse,
                                format!("column {column:?}: numeric value {d} does not fit Decimal128(38, 10)"),
                            ));
                        }
                        b.append_value(d.mantissa());
                    }
                }
            }
            ColumnBuilder::Utf8(b) => {
                b.append_option(row.try_get::<_, Option<String>>(index).map_err(err)?)
            }
            ColumnBuilder::Json(b) => {
                let value = row
                    .try_get::<_, Option<serde_json::Value>>(index)
                    .map_err(err)?;
                b.append_option(value.map(|v| v.to_string()))
            }
            ColumnBuilder::Uuid(b) => {
                let value = row.try_get::<_, Option<uuid::Uuid>>(index).map_err(err)?;
                b.append_option(value.map(|u| u.to_string()))
            }
            ColumnBuilder::Binary(b) => {
                b.append_option(row.try_get::<_, Option<Vec<u8>>>(index).map_err(err)?)
            }
            ColumnBuilder::Date(b) => {
                let value = row.try_get::<_, Option<NaiveDate>>(index).map_err(err)?;
                b.append_option(value.map(|d| (d - epoch_date()).num_days() as i32))
            }
            ColumnBuilder::Timestamp(b) => {
                let value = row
                    .try_get::<_, Option<NaiveDateTime>>(index)
                    .map_err(err)?;
                b.append_option(value.map(|t| t.and_utc().timestamp_micros()))
            }
            ColumnBuilder::TimestampTz(b) => {
                let value = row
                    .try_get::<_, Option<DateTime<Utc>>>(index)
                    .map_err(err)?;
                b.append_option(value.map(|t| t.timestamp_micros()))
            }
            ColumnBuilder::Time(b) => {
                let value = row.try_get::<_, Option<NaiveTime>>(index).map_err(err)?;
                b.append_option(value.map(|t| {
                    i64::from(t.num_seconds_from_midnight()) * 1_000_000
                        + i64::from(t.nanosecond() / 1000)
                }))
            }
        }
        Ok(())
    }

    fn finish(&mut self) -> ArrayRef {
        match self {
            ColumnBuilder::Bool(b) => Arc::new(b.finish()),
            ColumnBuilder::Int16(b) => Arc::new(b.finish()),
            ColumnBuilder::Int32(b) => Arc::new(b.finish()),
            ColumnBuilder::Int64(b) => Arc::new(b.finish()),
            ColumnBuilder::UInt32(b) => Arc::new(b.finish()),
            ColumnBuilder::Float32(b) => Arc::new(b.finish()),
            ColumnBuilder::Float64(b) => Arc::new(b.finish()),
            ColumnBuilder::Decimal(b) => Arc::new(b.finish()),
            ColumnBuilder::Utf8(b) | ColumnBuilder::Json(b) | ColumnBuilder::Uuid(b) => {
                Arc::new(b.finish())
            }
            ColumnBuilder::Binary(b) => Arc::new(b.finish()),
            ColumnBuilder::Date(b) => Arc::new(b.finish()),
            ColumnBuilder::Timestamp(b) | ColumnBuilder::TimestampTz(b) => Arc::new(b.finish()),
            ColumnBuilder::Time(b) => Arc::new(b.finish()),
        }
    }
}

/// Accumulates rows for one statement's columns and emits batches.
pub struct RowDecoder {
    schema: SchemaRef,
    names: Vec<String>,
    types: Vec<Type>,
    builders: Vec<ColumnBuilder>,
    batch_size: usize,
    pending: usize,
}

impl RowDecoder {
    /// Fails when a column type isn't decodable; callers cast such columns to text first.
    pub fn new(columns: &[tokio_postgres::Column], batch_size: usize) -> Result<RowDecoder> {
        let mut fields = Vec::with_capacity(columns.len());
        let mut builders = Vec::with_capacity(columns.len());
        let mut names = Vec::with_capacity(columns.len());
        let mut types = Vec::with_capacity(columns.len());
        for column in columns {
            let arrow_type = arrow_type_for(column.type_()).ok_or_else(|| {
                EngineError::new(
                    ErrorKind::Warehouse,
                    format!(
                        "column {:?} has undecodable type {}",
                        column.name(),
                        column.type_()
                    ),
                )
            })?;
            fields.push(Field::new(column.name(), arrow_type, true));
            builders.push(ColumnBuilder::new(column.type_(), batch_size).expect("checked above"));
            names.push(column.name().to_string());
            types.push(column.type_().clone());
        }
        Ok(RowDecoder {
            schema: Arc::new(Schema::new(fields)),
            names,
            types,
            builders,
            batch_size,
            pending: 0,
        })
    }

    pub fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    pub fn push(&mut self, row: &Row) -> Result<()> {
        for (i, builder) in self.builders.iter_mut().enumerate() {
            builder.append(row, i, &self.names[i])?;
        }
        self.pending += 1;
        Ok(())
    }

    pub fn is_full(&self) -> bool {
        self.pending >= self.batch_size
    }

    /// Emit the accumulated rows as a batch and reset. `None` when nothing is pending.
    pub fn flush(&mut self) -> Result<Option<RecordBatch>> {
        if self.pending == 0 {
            return Ok(None);
        }
        let columns: Vec<ArrayRef> = self
            .builders
            .iter_mut()
            .map(ColumnBuilder::finish)
            .collect();
        for (builder, ty) in self.builders.iter_mut().zip(&self.types) {
            *builder = ColumnBuilder::new(ty, self.batch_size).expect("known type");
        }
        self.pending = 0;
        RecordBatch::try_new(Arc::clone(&self.schema), columns)
            .map(Some)
            .map_err(|e| EngineError::new(ErrorKind::Warehouse, format!("building batch: {e}")))
    }
}

//! Cast warehouse batches to the schema the plan promised. The plan's schema is what Metabase was
//! told to expect; the warehouse's decoder produces its own best guess (int4 vs Int64, decimal
//! scale, timestamp unit), and this reconciles them column by column.

use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use arrow::array::ArrayRef;
use arrow::compute::{cast_with_options, CastOptions};
use arrow::datatypes::{Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use datafusion::error::DataFusionError;
use datafusion::physical_plan::{RecordBatchStream, SendableRecordBatchStream};
use futures::Stream;

pub struct SchemaCastStream {
    inner: SendableRecordBatchStream,
    target: SchemaRef,
}

/// A plan can leave a column typed `Null` when nothing pins it down (a bare `$1` in the select
/// list), and it types every `time` as nanoseconds where the warehouse sends microseconds. The
/// warehouse knows better: take its type for those columns.
pub fn resolve_null_types(plan: &Schema, warehouse: &Schema) -> Schema {
    let fields: Vec<Field> = plan
        .fields()
        .iter()
        .enumerate()
        .map(|(i, field)| {
            use arrow::datatypes::DataType;
            let actual = warehouse.fields().get(i).map(|f| f.data_type());
            let take_warehouse_type = match (field.data_type(), actual) {
                (DataType::Null, Some(_)) => true,
                // Arrow's cast kernel has no Time64 unit conversions; the unit means nothing to Metabase
                (
                    DataType::Time64(_) | DataType::Time32(_),
                    Some(DataType::Time64(_) | DataType::Time32(_)),
                ) => true,
                _ => false,
            };
            match (take_warehouse_type, actual) {
                (true, Some(actual)) => Field::new(field.name(), actual.clone(), true),
                _ => field.as_ref().clone(),
            }
        })
        .collect();
    Schema::new_with_metadata(fields, plan.metadata().clone())
}

/// The target schema with every field nullable: the warehouse decides what comes back, and a
/// non-nullable plan field must never make a real result fail.
pub fn nullable_schema(schema: &Schema) -> SchemaRef {
    Arc::new(Schema::new_with_metadata(
        schema
            .fields()
            .iter()
            .map(|f| {
                Field::new(f.name(), f.data_type().clone(), true)
                    .with_metadata(f.metadata().clone())
            })
            .collect::<Vec<_>>(),
        schema.metadata().clone(),
    ))
}

impl SchemaCastStream {
    pub fn new(inner: SendableRecordBatchStream, target: &Schema) -> SchemaCastStream {
        let target = resolve_null_types(target, &inner.schema());
        SchemaCastStream {
            inner,
            target: nullable_schema(&target),
        }
    }

    pub fn cast_batch(
        batch: &RecordBatch,
        target: &SchemaRef,
    ) -> Result<RecordBatch, DataFusionError> {
        if batch.num_columns() != target.fields().len() {
            return Err(DataFusionError::Execution(format!(
                "warehouse returned {} columns but the plan has {}",
                batch.num_columns(),
                target.fields().len()
            )));
        }
        let columns = batch
            .columns()
            .iter()
            .zip(target.fields())
            .map(|(column, field)| -> Result<ArrayRef, DataFusionError> {
                if column.data_type() == field.data_type() {
                    Ok(Arc::clone(column))
                } else {
                    // `safe: false` so a value that doesn't fit the plan's type is an error, never a silent NULL.
                    let options = CastOptions {
                        safe: false,
                        ..Default::default()
                    };
                    cast_with_options(column, field.data_type(), &options).map_err(|e| {
                        DataFusionError::Execution(format!(
                            "casting column {:?} from {} to {}: {e}",
                            field.name(),
                            column.data_type(),
                            field.data_type()
                        ))
                    })
                }
            })
            .collect::<Result<Vec<_>, _>>()?;
        RecordBatch::try_new(Arc::clone(target), columns)
            .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))
    }
}

impl Stream for SchemaCastStream {
    type Item = Result<RecordBatch, DataFusionError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match self.inner.as_mut().poll_next(cx) {
            Poll::Ready(Some(Ok(batch))) => {
                Poll::Ready(Some(Self::cast_batch(&batch, &self.target)))
            }
            other => other,
        }
    }
}

impl RecordBatchStream for SchemaCastStream {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.target)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int32Array, StringArray};
    use arrow::datatypes::DataType;

    #[test]
    fn casts_columns_to_the_plan_types_and_relaxes_nullability() {
        let source = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("n", DataType::Int32, true),
                Field::new("s", DataType::Utf8, true),
            ])),
            vec![
                Arc::new(Int32Array::from(vec![1, 2])),
                Arc::new(StringArray::from(vec!["a", "b"])),
            ],
        )
        .unwrap();
        let plan = Schema::new(vec![
            Field::new("n", DataType::Int64, false),
            Field::new("s", DataType::Utf8, true),
        ]);
        let target = nullable_schema(&plan);
        let out = SchemaCastStream::cast_batch(&source, &target).unwrap();
        assert_eq!(out.schema().field(0).data_type(), &DataType::Int64);
        assert!(out.schema().field(0).is_nullable());
        assert_eq!(out.num_rows(), 2);
    }

    #[test]
    fn values_that_do_not_fit_the_plan_type_fail_instead_of_becoming_null() {
        let source = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("s", DataType::Utf8, true)])),
            vec![Arc::new(StringArray::from(vec!["$1,234.00"]))],
        )
        .unwrap();
        let plan = Schema::new(vec![Field::new("s", DataType::Float64, true)]);
        assert!(SchemaCastStream::cast_batch(&source, &nullable_schema(&plan)).is_err());
    }

    #[test]
    fn null_plan_types_take_the_warehouse_type() {
        let plan = Schema::new(vec![
            Field::new("s", DataType::Null, true),
            Field::new("n", DataType::Int64, true),
        ]);
        let warehouse = Schema::new(vec![
            Field::new("s", DataType::Utf8, true),
            Field::new("n", DataType::Int32, true),
        ]);
        let resolved = resolve_null_types(&plan, &warehouse);
        assert_eq!(resolved.field(0).data_type(), &DataType::Utf8);
        assert_eq!(resolved.field(1).data_type(), &DataType::Int64);
    }

    #[test]
    fn time_columns_keep_the_warehouse_unit() {
        use arrow::datatypes::TimeUnit;
        let plan = Schema::new(vec![Field::new(
            "t",
            DataType::Time64(TimeUnit::Nanosecond),
            true,
        )]);
        let warehouse = Schema::new(vec![Field::new(
            "t",
            DataType::Time64(TimeUnit::Microsecond),
            true,
        )]);
        let resolved = resolve_null_types(&plan, &warehouse);
        assert_eq!(
            resolved.field(0).data_type(),
            &DataType::Time64(TimeUnit::Microsecond)
        );
    }

    #[test]
    fn column_count_mismatch_is_an_execution_error() {
        let source = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("n", DataType::Int32, true)])),
            vec![Arc::new(Int32Array::from(vec![1]))],
        )
        .unwrap();
        let plan = Schema::new(vec![
            Field::new("n", DataType::Int64, true),
            Field::new("m", DataType::Int64, true),
        ]);
        assert!(SchemaCastStream::cast_batch(&source, &nullable_schema(&plan)).is_err());
    }
}

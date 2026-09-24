//! Cast warehouse batches to the schema the plan promised. The plan's schema is what Metabase was
//! told to expect; the warehouse's decoder produces its own best guess (int4 vs Int64, decimal
//! scale, timestamp unit), and this reconciles them column by column.

use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use arrow::array::ArrayRef;
use arrow::compute::cast;
use arrow::datatypes::{Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use datafusion::error::DataFusionError;
use datafusion::physical_plan::{RecordBatchStream, SendableRecordBatchStream};
use futures::Stream;

pub struct SchemaCastStream {
    inner: SendableRecordBatchStream,
    target: SchemaRef,
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
        SchemaCastStream {
            inner,
            target: nullable_schema(target),
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
                    cast(column, field.data_type()).map_err(|e| {
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

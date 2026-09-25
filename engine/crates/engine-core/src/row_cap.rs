//! The generic row cap: a stream that ends after a number of rows, truncating the batch that
//! crosses it. Connectors enforce the cap themselves (stopping the statement and keeping the
//! connection); this is the guarantee for any that don't.

use std::pin::Pin;
use std::task::{Context, Poll};

use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use datafusion::error::DataFusionError;
use datafusion::physical_plan::{RecordBatchStream, SendableRecordBatchStream};
use futures::Stream;

pub struct RowCapStream {
    inner: SendableRecordBatchStream,
    remaining: u64,
}

impl RowCapStream {
    pub fn new(inner: SendableRecordBatchStream, cap: u64) -> RowCapStream {
        RowCapStream {
            inner,
            remaining: cap,
        }
    }
}

impl Stream for RowCapStream {
    type Item = Result<RecordBatch, DataFusionError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if self.remaining == 0 {
            return Poll::Ready(None);
        }
        match self.inner.as_mut().poll_next(cx) {
            Poll::Ready(Some(Ok(batch))) => {
                let rows = batch.num_rows() as u64;
                if rows <= self.remaining {
                    self.remaining -= rows;
                    Poll::Ready(Some(Ok(batch)))
                } else {
                    let keep = self.remaining as usize;
                    self.remaining = 0;
                    Poll::Ready(Some(Ok(batch.slice(0, keep))))
                }
            }
            other => other,
        }
    }
}

impl RecordBatchStream for RowCapStream {
    fn schema(&self) -> SchemaRef {
        self.inner.schema()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{Int64Array, RecordBatch};
    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
    use futures::StreamExt;

    use super::*;

    fn batches(sizes: &[i64]) -> SendableRecordBatchStream {
        let schema = Arc::new(Schema::new(vec![Field::new("n", DataType::Int64, false)]));
        let items: Vec<Result<RecordBatch, DataFusionError>> = sizes
            .iter()
            .map(|n| {
                Ok(RecordBatch::try_new(
                    Arc::clone(&schema),
                    vec![Arc::new(Int64Array::from((0..*n).collect::<Vec<_>>()))],
                )
                .unwrap())
            })
            .collect();
        Box::pin(RecordBatchStreamAdapter::new(
            schema,
            futures::stream::iter(items),
        ))
    }

    async fn row_counts(stream: RowCapStream) -> Vec<usize> {
        stream.map(|b| b.unwrap().num_rows()).collect().await
    }

    #[tokio::test]
    async fn stops_after_the_cap_truncating_the_batch_that_crosses_it() {
        assert_eq!(
            row_counts(RowCapStream::new(batches(&[3, 3, 3]), 5)).await,
            vec![3, 2]
        );
        assert_eq!(
            row_counts(RowCapStream::new(batches(&[3, 3]), 6)).await,
            vec![3, 3]
        );
        assert_eq!(
            row_counts(RowCapStream::new(batches(&[3, 3]), 100)).await,
            vec![3, 3]
        );
        assert!(row_counts(RowCapStream::new(batches(&[3]), 0))
            .await
            .is_empty());
    }
}

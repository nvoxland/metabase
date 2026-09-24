//! Running streams and the blocking reader the C ABI exports.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;

use arrow::array::RecordBatchReader;
use arrow::datatypes::SchemaRef;
use arrow::error::ArrowError;
use arrow::record_batch::RecordBatch;
use datafusion::physical_plan::SendableRecordBatchStream;
use futures::StreamExt;
use parking_lot::Mutex;
use serde::Serialize;
use tokio::runtime::Handle;
use tokio_util::sync::CancellationToken;

#[derive(Debug, Default)]
pub struct StreamStats {
    pub rows: AtomicU64,
    pub batches: AtomicU64,
    pub bytes: AtomicU64,
}

#[derive(Debug, Clone, Serialize)]
pub struct StreamStatsSnapshot {
    pub rows: u64,
    pub batches: u64,
    pub bytes: u64,
    pub elapsed_ms: u128,
    pub exported: bool,
    pub cancelled: bool,
}

/// A statement the engine started: its batches (until exported), its cancel token, and counters.
pub struct StreamEntry {
    pub session_id: u64,
    pub schema: SchemaRef,
    stream: Mutex<Option<SendableRecordBatchStream>>,
    pub cancel: CancellationToken,
    pub stats: Arc<StreamStats>,
    pub started: Instant,
}

impl StreamEntry {
    pub fn new(
        session_id: u64,
        stream: SendableRecordBatchStream,
        cancel: CancellationToken,
    ) -> StreamEntry {
        StreamEntry {
            session_id,
            schema: stream.schema(),
            stream: Mutex::new(Some(stream)),
            cancel,
            stats: Arc::new(StreamStats::default()),
            started: Instant::now(),
        }
    }

    /// Take the batches out for export. Only the first caller gets them.
    pub fn take(&self) -> Option<SendableRecordBatchStream> {
        self.stream.lock().take()
    }

    pub fn exported(&self) -> bool {
        self.stream.lock().is_none()
    }

    pub fn snapshot(&self) -> StreamStatsSnapshot {
        StreamStatsSnapshot {
            rows: self.stats.rows.load(Ordering::Relaxed),
            batches: self.stats.batches.load(Ordering::Relaxed),
            bytes: self.stats.bytes.load(Ordering::Relaxed),
            elapsed_ms: self.started.elapsed().as_millis(),
            exported: self.exported(),
            cancelled: self.cancel.is_cancelled(),
        }
    }
}

/// Pulls batches synchronously from a thread that is not a runtime worker (a JVM thread), so it
/// can back an Arrow C Data Interface stream. Dropping it cancels the statement.
pub struct BlockingReader {
    handle: Handle,
    stream: SendableRecordBatchStream,
    schema: SchemaRef,
    cancel: CancellationToken,
    stats: Arc<StreamStats>,
    finished: bool,
}

impl BlockingReader {
    pub fn new(
        handle: Handle,
        stream: SendableRecordBatchStream,
        cancel: CancellationToken,
        stats: Arc<StreamStats>,
    ) -> BlockingReader {
        BlockingReader {
            handle,
            schema: stream.schema(),
            stream,
            cancel,
            stats,
            finished: false,
        }
    }
}

impl Iterator for BlockingReader {
    type Item = Result<RecordBatch, ArrowError>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.finished {
            return None;
        }
        let next = self.handle.block_on(self.stream.next());
        match next {
            None => {
                self.finished = true;
                None
            }
            Some(Ok(batch)) => {
                self.stats
                    .rows
                    .fetch_add(batch.num_rows() as u64, Ordering::Relaxed);
                self.stats.batches.fetch_add(1, Ordering::Relaxed);
                self.stats
                    .bytes
                    .fetch_add(batch.get_array_memory_size() as u64, Ordering::Relaxed);
                Some(Ok(batch))
            }
            Some(Err(e)) => {
                self.finished = true;
                Some(Err(ArrowError::ExternalError(Box::new(e))))
            }
        }
    }
}

impl RecordBatchReader for BlockingReader {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
}

impl Drop for BlockingReader {
    fn drop(&mut self) {
        if !self.finished {
            self.cancel.cancel();
        }
    }
}

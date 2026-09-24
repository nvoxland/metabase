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
/// can back an Arrow C Data Interface stream. The batches are produced by a task on the engine's
/// worker threads and handed over a small channel, so no engine code runs on the caller's stack.
/// Dropping the reader stops the producer and cancels the statement.
pub struct BlockingReader {
    handle: Handle,
    receiver: Option<tokio::sync::mpsc::Receiver<Result<RecordBatch, ArrowError>>>,
    producer: tokio::task::JoinHandle<()>,
    schema: SchemaRef,
    cancel: CancellationToken,
    stats: Arc<StreamStats>,
    finished: bool,
}

/// How many batches may be decoded ahead of the consumer.
const PREFETCH_BATCHES: usize = 4;

impl BlockingReader {
    pub fn spawn(
        handle: Handle,
        mut stream: SendableRecordBatchStream,
        cancel: CancellationToken,
        stats: Arc<StreamStats>,
    ) -> BlockingReader {
        let schema = stream.schema();
        let (sender, receiver) = tokio::sync::mpsc::channel(PREFETCH_BATCHES);
        let producer_stats = Arc::clone(&stats);
        let producer = handle.spawn(async move {
            while let Some(item) = stream.next().await {
                let item = match item {
                    Ok(batch) => {
                        producer_stats
                            .rows
                            .fetch_add(batch.num_rows() as u64, Ordering::Relaxed);
                        producer_stats.batches.fetch_add(1, Ordering::Relaxed);
                        producer_stats
                            .bytes
                            .fetch_add(batch.get_array_memory_size() as u64, Ordering::Relaxed);
                        Ok(batch)
                    }
                    Err(e) => Err(ArrowError::ExternalError(Box::new(e))),
                };
                let failed = item.is_err();
                if sender.send(item).await.is_err() || failed {
                    break;
                }
            }
            // Dropping the stream here (on a worker) releases the warehouse connection.
        });
        BlockingReader {
            handle,
            receiver: Some(receiver),
            producer,
            schema,
            cancel,
            stats,
            finished: false,
        }
    }

    pub fn stats(&self) -> &StreamStats {
        &self.stats
    }
}

impl Iterator for BlockingReader {
    type Item = Result<RecordBatch, ArrowError>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.finished {
            return None;
        }
        // Runs inside the Arrow C Data Interface `get_next` callback: nothing may unwind out of here.
        let received = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let receiver = self.receiver.as_mut()?;
            self.handle.block_on(receiver.recv())
        }));
        match received {
            Ok(Some(item)) => {
                if item.is_err() {
                    self.finished = true;
                }
                Some(item)
            }
            Ok(None) => {
                self.finished = true;
                None
            }
            Err(_) => {
                self.finished = true;
                Some(Err(ArrowError::ExternalError(Box::new(
                    crate::error::EngineError::new(
                        crate::error::ErrorKind::InternalPanic,
                        "engine panicked while reading results",
                    ),
                ))))
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
        // Also reached from the C stream `release` callback: must never panic and must not run
        // engine code on this thread. Closing the channel makes the producer stop and drop the
        // stream on its own worker; cancelling stops the statement server-side.
        if !self.finished {
            self.cancel.cancel();
        }
        self.receiver.take();
        if self.finished {
            self.producer.abort();
        }
    }
}

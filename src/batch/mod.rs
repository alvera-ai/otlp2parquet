//! In-memory batch accumulation for server mode.
//!
//! Accumulates Arrow batches in memory and merges them into larger Arrow batches.
//! This reduces the number of storage writes and improves compression efficiency.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{anyhow, Result};
use arrow::array::RecordBatch;
use otlp2records::PartitionedBatch;
use parking_lot::Mutex;

mod buffered_batch;

use buffered_batch::BufferedBatch;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct BatchKey {
    service: String,
    minute_bucket: i64,
}

impl BatchKey {
    fn from_metadata(metadata: &SignalMetadata) -> Self {
        let bucket = if metadata.first_timestamp_micros > 0 {
            // Metadata timestamps are stored in microseconds; bucket by minute in micros.
            metadata.first_timestamp_micros / 60_000_000
        } else {
            tracing::warn!(
                service = %metadata.service_name,
                timestamp_micros = metadata.first_timestamp_micros,
                "Batch has non-positive timestamp — bucketed to epoch 0"
            );
            0
        };

        Self {
            service: metadata.service_name.as_ref().to_string(),
            minute_bucket: bucket,
        }
    }
}

#[derive(Debug, Clone)]
pub struct BatchConfig {
    pub max_rows: usize,
    pub max_bytes: usize,
    pub max_age: Duration,
}

/// Metadata extracted during OTLP parsing, shared across all signal types.
#[derive(Debug, Clone)]
pub struct SignalMetadata {
    pub service_name: Arc<str>,
    // Stored in microseconds to align with Parquet expectations.
    pub first_timestamp_micros: i64,
    pub record_count: usize,
}

/// Completed batch ready for storage.
///
/// Contains merged Arrow RecordBatch + metadata.
/// Hashing and serialization happen in the storage layer.
#[derive(Debug)]
pub struct CompletedBatch {
    pub batches: Vec<RecordBatch>,
    pub metadata: SignalMetadata,
}

/// Maximum ratio of pending bytes to configured max_bytes before backpressure kicks in.
const BACKPRESSURE_MULTIPLIER: usize = 8;

/// Thread-safe batch orchestrator shared across handlers.
pub struct BatchManager {
    config: BatchConfig,
    inner: Arc<Mutex<BatchState>>,
}

#[derive(Debug)]
struct BatchState {
    batches: HashMap<BatchKey, BufferedBatch>,
    total_bytes: usize,
}

impl BatchManager {
    pub fn new(config: BatchConfig) -> Self {
        Self {
            config,
            inner: Arc::new(Mutex::new(BatchState {
                batches: HashMap::new(),
                total_bytes: 0,
            })),
        }
    }

    pub fn ingest(
        &self,
        request: &PartitionedBatch,
        approx_bytes: usize,
    ) -> Result<(Vec<CompletedBatch>, SignalMetadata)> {
        let metadata = SignalMetadata {
            service_name: Arc::clone(&request.service_name),
            first_timestamp_micros: request.min_timestamp_micros,
            record_count: request.record_count,
        };

        if metadata.record_count == 0 {
            return Ok((Vec::new(), metadata));
        }

        let key = BatchKey::from_metadata(&metadata);
        let mut guard = self.inner.lock();
        let max_pending_bytes = self.config.max_bytes.saturating_mul(BACKPRESSURE_MULTIPLIER);

        let prospective_total = guard.total_bytes.saturating_add(approx_bytes);
        if prospective_total > max_pending_bytes {
            anyhow::bail!(
                "backpressure: buffered batches exceed limit ({} > {})",
                prospective_total,
                max_pending_bytes
            );
        }

        // Scope the mutable borrow to avoid holding it across flush/remove.
        let flush_now = {
            let buffered = guard
                .batches
                .entry(key.clone())
                .or_insert_with(|| BufferedBatch::new(&metadata));
            buffered.add_batches(vec![request.batch.clone()], &metadata, approx_bytes);
            buffered.should_flush(&self.config)
        };

        guard.total_bytes = prospective_total;

        let mut completed = Vec::new();
        if flush_now {
            let batch = guard
                .batches
                .remove(&key)
                .ok_or_else(|| anyhow!("batch evicted before flush: {:?}", key))?;
            guard.total_bytes = guard.total_bytes.saturating_sub(batch.total_bytes());
            drop(guard);

            match batch.finalize() {
                Ok(c) => completed.push(c),
                Err(e) => {
                    // Batch already removed from map — data lost on this hot path.
                    // Error propagates to HTTP handler → 500 → OTLP client retries.
                    tracing::error!(
                        service = %metadata.service_name,
                        rows = metadata.record_count,
                        error = %e,
                        "Batch finalize failed after removal from buffer"
                    );
                    return Err(e);
                }
            }
        } else {
            drop(guard);
        }

        Ok((completed, metadata))
    }

    /// Drain batches that exceed any threshold (rows, bytes, or age).
    /// Collects expired entries under the lock, then finalizes outside it
    /// to avoid blocking ingest() during finalization.
    pub fn drain_expired(&self) -> Result<Vec<CompletedBatch>> {
        let expired = {
            let mut guard = self.inner.lock();
            let keys: Vec<BatchKey> = guard
                .batches
                .iter()
                .filter(|(_, batch)| batch.should_flush(&self.config))
                .map(|(key, _)| key.clone())
                .collect();

            let mut expired = Vec::with_capacity(keys.len());
            for key in keys {
                if let Some(batch) = guard.batches.remove(&key) {
                    guard.total_bytes = guard.total_bytes.saturating_sub(batch.total_bytes());
                    expired.push(batch);
                }
            }
            expired
        }; // guard dropped here

        expired
            .into_iter()
            .map(|batch| batch.finalize())
            .collect()
    }

    pub fn drain_all(&self) -> Result<Vec<CompletedBatch>> {
        let mut guard = self.inner.lock();
        let drained: Vec<_> = guard.batches.drain().collect();
        guard.total_bytes = 0;
        drop(guard);

        drained
            .into_iter()
            .map(|(_, batch)| batch.finalize())
            .collect()
    }

    /// Re-insert a completed batch that failed to persist.
    /// Uses Arrow memory size for byte tracking (not wire-format estimate).
    pub fn re_insert(&self, completed: CompletedBatch) {
        let CompletedBatch { batches, metadata } = completed;
        let key = BatchKey::from_metadata(&metadata);
        let byte_estimate: usize = batches.iter().map(|b| b.get_array_memory_size()).sum();

        let mut guard = self.inner.lock();
        let buffered = guard
            .batches
            .entry(key)
            .or_insert_with(|| BufferedBatch::new(&metadata));
        buffered.add_batches(batches, &metadata, byte_estimate);
        guard.total_bytes += byte_estimate;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int64Array, StringArray, TimestampMillisecondArray};
    use arrow::datatypes::{DataType, Field, Schema, TimeUnit};

    fn create_test_batch(service_name: &str, record_count: usize) -> PartitionedBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new(
                "timestamp",
                DataType::Timestamp(TimeUnit::Millisecond, None),
                false,
            ),
            Field::new("service_name", DataType::Utf8, true),
            Field::new("severity_number", DataType::Int64, true),
        ]));

        let timestamps: Vec<i64> = (0..record_count)
            .map(|i| 1_700_000_000_000 + i as i64)
            .collect();
        let services: Vec<&str> = vec![service_name; record_count];
        let severities: Vec<i64> = vec![9; record_count];

        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(TimestampMillisecondArray::from(timestamps.clone())),
                Arc::new(StringArray::from(services)),
                Arc::new(Int64Array::from(severities)),
            ],
        )
        .unwrap();

        PartitionedBatch {
            batch,
            service_name: Arc::from(service_name),
            min_timestamp_micros: timestamps[0] * 1000, // Convert ms to us
            record_count,
        }
    }

    fn create_test_batch_with_ts(
        service_name: &str,
        record_count: usize,
        min_ts_micros: i64,
    ) -> PartitionedBatch {
        let mut pb = create_test_batch(service_name, record_count);
        pb.min_timestamp_micros = min_ts_micros;
        pb
    }

    #[test]
    fn test_batch_manager_accumulation() {
        let config = BatchConfig {
            max_rows: 100,
            max_bytes: 1024 * 1024,
            max_age: Duration::from_secs(10),
        };
        let manager = BatchManager::new(config);

        let request1 = create_test_batch("test-service", 10);
        let (completed1, _) = manager.ingest(&request1, 320).unwrap();
        assert_eq!(completed1.len(), 0);

        let request2 = create_test_batch("test-service", 10);
        let (completed2, _) = manager.ingest(&request2, 320).unwrap();
        assert_eq!(completed2.len(), 0);
    }

    #[test]
    fn test_batch_manager_flush_at_row_threshold() {
        let config = BatchConfig {
            max_rows: 20,
            max_bytes: 1024 * 1024,
            max_age: Duration::from_secs(10),
        };
        let manager = BatchManager::new(config);

        let req1 = create_test_batch("test-service", 10);
        let (c1, _) = manager.ingest(&req1, 320).unwrap();
        assert_eq!(c1.len(), 0);

        let req2 = create_test_batch("test-service", 10);
        let (c2, _) = manager.ingest(&req2, 320).unwrap();
        assert_eq!(c2.len(), 1);
        assert_eq!(
            c2[0].batches.iter().map(|b| b.num_rows()).sum::<usize>(),
            20
        );
    }

    #[test]
    fn test_batch_manager_flush_at_byte_threshold() {
        let config = BatchConfig {
            max_rows: 1_000_000,
            max_bytes: 500, // Low byte threshold
            max_age: Duration::from_secs(3600),
        };
        let manager = BatchManager::new(config);

        let req1 = create_test_batch("test-service", 5);
        let (c1, _) = manager.ingest(&req1, 300).unwrap();
        assert_eq!(c1.len(), 0);

        // Second ingest pushes total bytes over 500
        let req2 = create_test_batch("test-service", 5);
        let (c2, _) = manager.ingest(&req2, 300).unwrap();
        assert_eq!(c2.len(), 1, "Should flush when byte threshold exceeded");
    }

    #[test]
    fn test_backpressure_rejects_when_limit_exceeded() {
        // max_bytes=10_000 → flush threshold, but backpressure at 80_000
        // We'll accumulate across many services to avoid per-key flush
        let config = BatchConfig {
            max_rows: 1_000_000,
            max_bytes: 10_000,
            max_age: Duration::from_secs(3600),
        };
        let manager = BatchManager::new(config);

        // Accumulate 8 batches across different services (below per-key byte threshold)
        // Total: 8 * 9_000 = 72_000 < 80_000 backpressure limit
        for i in 0..8 {
            let req = create_test_batch(&format!("svc-{}", i), 5);
            manager.ingest(&req, 9_000).unwrap();
        }

        // This should be rejected (72_000 + 9_000 = 81_000 > 80_000)
        let req_over = create_test_batch("svc-overflow", 5);
        let result = manager.ingest(&req_over, 9_000);
        assert!(result.is_err(), "Should reject when backpressure limit hit");
        assert!(
            result.unwrap_err().to_string().contains("backpressure"),
            "Error should mention backpressure"
        );
    }

    #[test]
    fn test_drain_all_returns_all_buffered_batches() {
        let config = BatchConfig {
            max_rows: 1_000_000,
            max_bytes: 100 * 1024 * 1024,
            max_age: Duration::from_secs(3600),
        };
        let manager = BatchManager::new(config);

        let req1 = create_test_batch("svc-a", 5);
        let req2 = create_test_batch("svc-b", 10);
        manager.ingest(&req1, 100).unwrap();
        manager.ingest(&req2, 200).unwrap();

        let drained = manager.drain_all().unwrap();
        assert_eq!(drained.len(), 2, "Should drain both service batches");

        let total_rows: usize = drained.iter().map(|c| c.metadata.record_count).sum();
        assert_eq!(total_rows, 15);

        // Manager should be empty after drain
        let second_drain = manager.drain_all().unwrap();
        assert!(second_drain.is_empty(), "Second drain should return nothing");
    }

    #[test]
    fn test_drain_expired_returns_aged_batches() {
        let config = BatchConfig {
            max_rows: 1_000_000,
            max_bytes: 100 * 1024 * 1024,
            max_age: Duration::from_millis(1), // Expire almost immediately
        };
        let manager = BatchManager::new(config);

        let req = create_test_batch("test-service", 5);
        manager.ingest(&req, 100).unwrap();

        // Wait for expiry
        std::thread::sleep(Duration::from_millis(5));

        let expired = manager.drain_expired().unwrap();
        assert_eq!(expired.len(), 1, "Should drain the expired batch");
        assert_eq!(expired[0].metadata.record_count, 5);
    }

    #[test]
    fn test_drain_expired_leaves_fresh_batches() {
        let config = BatchConfig {
            max_rows: 1_000_000,
            max_bytes: 100 * 1024 * 1024,
            max_age: Duration::from_secs(3600), // Very long expiry
        };
        let manager = BatchManager::new(config);

        let req = create_test_batch("test-service", 5);
        manager.ingest(&req, 100).unwrap();

        let expired = manager.drain_expired().unwrap();
        assert!(expired.is_empty(), "Fresh batch should not be expired");

        // Batch should still be in the manager
        let all = manager.drain_all().unwrap();
        assert_eq!(all.len(), 1);
    }

    #[test]
    fn test_multi_service_batches_isolated() {
        let config = BatchConfig {
            max_rows: 10,
            max_bytes: 1024 * 1024,
            max_age: Duration::from_secs(3600),
        };
        let manager = BatchManager::new(config);

        // 5 rows for svc-a → not flushed
        let req_a = create_test_batch("svc-a", 5);
        let (c1, _) = manager.ingest(&req_a, 100).unwrap();
        assert_eq!(c1.len(), 0);

        // 5 rows for svc-b → not flushed (different key)
        let req_b = create_test_batch("svc-b", 5);
        let (c2, _) = manager.ingest(&req_b, 100).unwrap();
        assert_eq!(c2.len(), 0);

        // 5 more rows for svc-a → flushes svc-a only (total 10)
        let req_a2 = create_test_batch("svc-a", 5);
        let (c3, _) = manager.ingest(&req_a2, 100).unwrap();
        assert_eq!(c3.len(), 1, "svc-a should flush at 10 rows");
        assert_eq!(*c3[0].metadata.service_name, *"svc-a");

        // svc-b should still be buffered
        let remaining = manager.drain_all().unwrap();
        assert_eq!(remaining.len(), 1);
        assert_eq!(*remaining[0].metadata.service_name, *"svc-b");
    }

    #[test]
    fn test_minute_bucket_partitioning() {
        let config = BatchConfig {
            max_rows: 10,
            max_bytes: 1024 * 1024,
            max_age: Duration::from_secs(3600),
        };
        let manager = BatchManager::new(config);

        // Minute 1: timestamp 60_000_000 micros (1 minute)
        let req1 = create_test_batch_with_ts("svc", 5, 60_000_000);
        manager.ingest(&req1, 100).unwrap();

        // Minute 2: timestamp 120_000_000 micros (2 minutes) → different bucket
        let req2 = create_test_batch_with_ts("svc", 5, 120_000_000);
        manager.ingest(&req2, 100).unwrap();

        // Should have 2 separate batches (different minute buckets)
        let all = manager.drain_all().unwrap();
        assert_eq!(
            all.len(),
            2,
            "Different minute buckets should produce separate batches"
        );
    }

    #[test]
    fn test_re_insert_returns_batch_to_manager() {
        let config = BatchConfig {
            max_rows: 1_000_000,
            max_bytes: 100 * 1024 * 1024,
            max_age: Duration::from_secs(3600),
        };
        let manager = BatchManager::new(config);

        let req = create_test_batch("test-service", 5);
        manager.ingest(&req, 100).unwrap();

        // Drain, then re-insert
        let drained = manager.drain_all().unwrap();
        assert_eq!(drained.len(), 1);

        let completed = drained.into_iter().next().unwrap();
        manager.re_insert(completed);

        // Should be back in the manager
        let re_drained = manager.drain_all().unwrap();
        assert_eq!(re_drained.len(), 1);
        assert_eq!(re_drained[0].metadata.record_count, 5);
    }
}

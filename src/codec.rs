//! Shared codec utilities for OTLP decoding and value extraction.
//!
//! This module provides pure functions for decoding OTLP payloads.

use arrow::array::{Array, RecordBatch, StringArray, UInt32Array};
use arrow::compute;
use crate::types::SeverityPartition;
use std::collections::HashMap;
use otlp2records::{
    group_batch_by_service, transform_logs, transform_metrics, transform_traces, InputFormat,
};

pub use otlp2records::{
    PartitionedBatch, PartitionedMetrics, ServiceGroupedBatches, SkippedMetrics,
};

/// Report skipped metrics via tracing.
/// Uses warn level to ensure visibility in production logs.
pub fn report_skipped_metrics(skipped: &SkippedMetrics) {
    if skipped.has_skipped() {
        tracing::warn!(
            summaries = skipped.summaries,
            nan_values = skipped.nan_values,
            infinity_values = skipped.infinity_values,
            missing_values = skipped.missing_values,
            total = skipped.total(),
            "Skipped unsupported or invalid metric data points"
        );
    }
}

// =============================================================================
// Decode functions - return partitioned Arrow batches
// =============================================================================

/// Decode and transform logs, returning batches grouped by service.
/// Returns String errors for easy wrapping by platform-specific error types.
pub fn decode_logs_partitioned(
    body: &[u8],
    format: InputFormat,
) -> Result<ServiceGroupedBatches, String> {
    let batch = transform_logs(body, format).map_err(|e| e.to_string())?;
    Ok(group_batch_by_service(batch))
}

/// Decode and transform traces, returning batches grouped by service.
/// Returns String errors for easy wrapping by platform-specific error types.
pub fn decode_traces_partitioned(
    body: &[u8],
    format: InputFormat,
) -> Result<ServiceGroupedBatches, String> {
    let batch = transform_traces(body, format).map_err(|e| e.to_string())?;
    Ok(group_batch_by_service(batch))
}

/// Decode and transform metrics, returning partitioned batches by type and service.
/// Returns String errors for easy wrapping by platform-specific error types.
pub fn decode_metrics_partitioned(
    body: &[u8],
    format: InputFormat,
) -> Result<PartitionedMetrics, String> {
    let batches = transform_metrics(body, format).map_err(|e| e.to_string())?;
    Ok(PartitionedMetrics {
        gauge: batches
            .gauge
            .map(group_batch_by_service)
            .unwrap_or_default(),
        sum: batches.sum.map(group_batch_by_service).unwrap_or_default(),
        histogram: batches
            .histogram
            .map(group_batch_by_service)
            .unwrap_or_default(),
        exp_histogram: batches
            .exp_histogram
            .map(group_batch_by_service)
            .unwrap_or_default(),
        skipped: batches.skipped,
    })
}

/// Split a log RecordBatch into sub-batches grouped by normalized severity.
///
/// Returns `(SeverityPartition, sub_batch)` pairs. Single-pass O(N) using
/// index collection + `arrow::compute::take`.
///
/// Errors if:
/// - `severity_text` column is missing (schema mismatch)
/// - `severity_text` column is not a StringArray (type mismatch)
/// - Arrow take/reconstruction fails
pub fn split_batch_by_severity(
    batch: &RecordBatch,
) -> Result<Vec<(SeverityPartition, RecordBatch)>, String> {
    let severity_col = batch
        .column_by_name("severity_text")
        .ok_or("severity_text column not found in log batch; severity partitioning requires this column")?;

    let severity_array = severity_col
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or("severity_text column is not a StringArray; cannot partition by severity")?;

    // Single pass: collect row indices per severity
    let mut groups: HashMap<SeverityPartition, Vec<u32>> = HashMap::with_capacity(7);
    let mut order: Vec<SeverityPartition> = Vec::with_capacity(7);

    for i in 0..severity_array.len() {
        let raw = if severity_array.is_null(i) {
            ""
        } else {
            severity_array.value(i)
        };
        let sev = SeverityPartition::from_severity_text(raw);
        groups
            .entry(sev)
            .or_insert_with(|| {
                order.push(sev);
                Vec::new()
            })
            .push(i as u32);
    }

    // Fast path: all rows have the same severity
    if groups.len() == 1 {
        return Ok(vec![(order[0], batch.clone())]);
    }

    // Build sub-batches via take
    let mut result = Vec::with_capacity(groups.len());
    for sev in order {
        let indices = &groups[&sev];
        let indices_array = UInt32Array::from(indices.clone());
        let columns: Vec<_> = batch
            .columns()
            .iter()
            .map(|col| compute::take(col.as_ref(), &indices_array, None))
            .collect::<std::result::Result<Vec<_>, _>>()
            .map_err(|e| format!("Failed to split batch by severity: {}", e))?;
        let sub_batch = RecordBatch::try_new(batch.schema(), columns)
            .map_err(|e| format!("Failed to reconstruct sub-batch: {}", e))?;
        result.push((sev, sub_batch));
    }

    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::ArrayRef;
    use arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;

    fn make_log_batch(severities: &[&str]) -> RecordBatch {
        let severity_array = StringArray::from(
            severities.iter().map(|s| Some(*s)).collect::<Vec<_>>(),
        );
        let body_array = StringArray::from(
            severities.iter().map(|_| Some("msg")).collect::<Vec<_>>(),
        );
        let schema = Schema::new(vec![
            Field::new("severity_text", DataType::Utf8, false),
            Field::new("body", DataType::Utf8, false),
        ]);
        RecordBatch::try_new(
            Arc::new(schema),
            vec![
                Arc::new(severity_array) as ArrayRef,
                Arc::new(body_array) as ArrayRef,
            ],
        )
        .unwrap()
    }

    #[test]
    fn test_decode_logs_partitioned_empty_jsonl() {
        let result = decode_logs_partitioned(b"", InputFormat::Jsonl);
        assert!(result.is_err());
    }

    #[test]
    fn test_decode_traces_partitioned_empty_jsonl() {
        let result = decode_traces_partitioned(b"", InputFormat::Jsonl);
        assert!(result.is_err());
    }

    #[test]
    fn test_decode_metrics_partitioned_empty_jsonl() {
        let result = decode_metrics_partitioned(b"", InputFormat::Jsonl);
        assert!(result.is_err());
    }

    #[test]
    fn test_split_single_severity() {
        let batch = make_log_batch(&["INFO", "INFO", "INFO"]);
        let result = split_batch_by_severity(&batch).unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].0, SeverityPartition::Info);
        assert_eq!(result[0].1.num_rows(), 3);
    }

    #[test]
    fn test_split_multi_severity() {
        let batch = make_log_batch(&["ERROR", "INFO", "ERROR", "DEBUG"]);
        let result = split_batch_by_severity(&batch).unwrap();
        assert_eq!(result.len(), 3);

        let error_batch = result.iter().find(|(s, _)| *s == SeverityPartition::Error).unwrap();
        assert_eq!(error_batch.1.num_rows(), 2);

        let info_batch = result.iter().find(|(s, _)| *s == SeverityPartition::Info).unwrap();
        assert_eq!(info_batch.1.num_rows(), 1);

        let debug_batch = result.iter().find(|(s, _)| *s == SeverityPartition::Debug).unwrap();
        assert_eq!(debug_batch.1.num_rows(), 1);
    }

    #[test]
    fn test_split_missing_column_errors() {
        let schema = Schema::new(vec![Field::new("body", DataType::Utf8, false)]);
        let body = StringArray::from(vec![Some("msg")]);
        let batch = RecordBatch::try_new(
            Arc::new(schema),
            vec![Arc::new(body) as ArrayRef],
        )
        .unwrap();

        let result = split_batch_by_severity(&batch);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("severity_text column not found"));
    }

    #[test]
    fn test_split_wrong_column_type_errors() {
        let schema = Schema::new(vec![
            Field::new("severity_text", DataType::Int32, false),
            Field::new("body", DataType::Utf8, false),
        ]);
        let int_array = arrow::array::Int32Array::from(vec![1, 2]);
        let body = StringArray::from(vec![Some("a"), Some("b")]);
        let batch = RecordBatch::try_new(
            Arc::new(schema),
            vec![Arc::new(int_array) as ArrayRef, Arc::new(body) as ArrayRef],
        )
        .unwrap();

        let result = split_batch_by_severity(&batch);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("not a StringArray"));
    }

    #[test]
    fn test_split_empty_severity_maps_to_unspecified() {
        let batch = make_log_batch(&["", "", ""]);
        let result = split_batch_by_severity(&batch).unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].0, SeverityPartition::Unspecified);
    }

    #[test]
    fn test_split_preserves_order() {
        let batch = make_log_batch(&["WARN", "ERROR", "WARN"]);
        let result = split_batch_by_severity(&batch).unwrap();
        // First-seen order: WARN, ERROR
        assert_eq!(result[0].0, SeverityPartition::Warn);
        assert_eq!(result[1].0, SeverityPartition::Error);
    }
}

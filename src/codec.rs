//! Shared codec utilities for OTLP decoding and value extraction.
//!
//! This module provides pure functions for decoding OTLP payloads.

use arrow::array::{Array, RecordBatch, StringArray};
use arrow::compute;
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

/// Normalize an OTLP severity text value to a partition key.
///
/// Maps standard OTLP severity names (and their sub-levels like DEBUG2, INFO3)
/// to lowercase base names. Empty or unrecognized values become "unspecified".
fn normalize_severity(severity_text: &str) -> &'static str {
    let upper = severity_text.to_uppercase();
    if upper.starts_with("TRACE") {
        "trace"
    } else if upper.starts_with("DEBUG") {
        "debug"
    } else if upper.starts_with("INFO") {
        "info"
    } else if upper.starts_with("WARN") {
        "warn"
    } else if upper.starts_with("ERROR") {
        "error"
    } else if upper.starts_with("FATAL") {
        "fatal"
    } else {
        "unspecified"
    }
}

/// Split a log RecordBatch into sub-batches grouped by normalized severity.
///
/// Returns `(severity_partition_value, sub_batch)` pairs. Each sub-batch
/// contains only rows matching that severity level.
pub fn split_batch_by_severity(batch: &RecordBatch) -> Vec<(String, RecordBatch)> {
    let severity_col = match batch.column_by_name("severity_text") {
        Some(col) => col,
        None => return vec![("unspecified".to_string(), batch.clone())],
    };
    let severity_array = match severity_col.as_any().downcast_ref::<StringArray>() {
        Some(arr) => arr,
        None => return vec![("unspecified".to_string(), batch.clone())],
    };

    // Collect distinct normalized severity values
    let mut seen: Vec<&'static str> = Vec::with_capacity(7);
    for i in 0..severity_array.len() {
        let raw = if severity_array.is_null(i) {
            ""
        } else {
            severity_array.value(i)
        };
        let norm = normalize_severity(raw);
        if !seen.contains(&norm) {
            seen.push(norm);
        }
    }

    // Fast path: all rows have the same severity
    if seen.len() == 1 {
        return vec![(seen[0].to_string(), batch.clone())];
    }

    let mut result = Vec::with_capacity(seen.len());
    for sev in seen {
        let predicate: arrow::array::BooleanArray = (0..severity_array.len())
            .map(|i| {
                let raw = if severity_array.is_null(i) {
                    ""
                } else {
                    severity_array.value(i)
                };
                Some(normalize_severity(raw) == sev)
            })
            .collect();

        if let Ok(filtered) = compute::filter_record_batch(batch, &predicate) {
            if filtered.num_rows() > 0 {
                result.push((sev.to_string(), filtered));
            }
        }
    }

    result
}

#[cfg(test)]
mod tests {
    use super::*;

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
}

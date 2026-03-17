// HTTP request handlers for server mode
//
// Implements OTLP ingestion and health check endpoints

use crate::{InputFormat, MetricType, SignalType};
use axum::{
    extract::State,
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    Json,
};
use metrics::{counter, histogram};

use crate::batch::CompletedBatch;
use crate::codec::{
    decode_logs_partitioned, decode_metrics_partitioned, decode_traces_partitioned,
    report_skipped_metrics, ServiceGroupedBatches,
};
use serde_json::json;
use std::time::Instant;
use tracing::{debug, info, warn};

use crate::{AppError, AppState};

/// POST /v1/logs - OTLP log ingestion endpoint
pub(crate) async fn handle_logs(
    State(state): State<AppState>,
    headers: HeaderMap,
    body: axum::body::Bytes,
) -> Result<Response, AppError> {
    handle_signal(SignalType::Logs, &state, headers, body).await
}

/// POST /v1/traces - OTLP trace ingestion endpoint
pub(crate) async fn handle_traces(
    State(state): State<AppState>,
    headers: HeaderMap,
    body: axum::body::Bytes,
) -> Result<Response, AppError> {
    handle_signal(SignalType::Traces, &state, headers, body).await
}

/// POST /v1/metrics - OTLP metrics ingestion endpoint
pub(crate) async fn handle_metrics(
    State(state): State<AppState>,
    headers: HeaderMap,
    body: axum::body::Bytes,
) -> Result<Response, AppError> {
    handle_signal(SignalType::Metrics, &state, headers, body).await
}

/// GET /health - Basic health check
pub(crate) async fn health_check() -> impl IntoResponse {
    (StatusCode::OK, Json(json!({"status": "healthy"})))
}

/// GET /ready - Readiness check
pub(crate) async fn ready_check(State(_state): State<AppState>) -> impl IntoResponse {
    (StatusCode::OK, Json(json!({"status": "ready"})))
}

async fn handle_signal(
    signal: SignalType,
    state: &AppState,
    headers: HeaderMap,
    body: axum::body::Bytes,
) -> Result<Response, AppError> {
    let content_type = headers.get("content-type").and_then(|v| v.to_str().ok());
    let format = InputFormat::from_content_type(content_type);

    debug!(
        "Received OTLP {} request ({} bytes, format: {:?}, content-type: {:?})",
        signal.as_str(),
        body.len(),
        format,
        content_type
    );

    let max_payload = state.max_payload_bytes;
    if body.len() > max_payload {
        counter!("otlp.ingest.rejected", "signal" => signal.as_str()).increment(1);
        return Err(AppError::with_status(
            StatusCode::PAYLOAD_TOO_LARGE,
            anyhow::anyhow!("payload {} exceeds limit {}", body.len(), max_payload),
        ));
    }

    match signal {
        SignalType::Logs => process_logs(state, format, body).await,
        SignalType::Traces => process_traces(state, format, body).await,
        SignalType::Metrics => process_metrics(format, body).await,
    }
}

async fn process_logs(
    state: &AppState,
    format: InputFormat,
    body: axum::body::Bytes,
) -> Result<Response, AppError> {
    let start = Instant::now();
    let body_len = body.len();
    counter!("otlp.ingest.requests", "signal" => "logs").increment(1);
    histogram!("otlp.ingest.bytes", "signal" => "logs").record(body_len as f64);

    let parse_start = Instant::now();
    let grouped = decode_logs_partitioned(&body, format).map_err(|e| {
        AppError::bad_request(anyhow::anyhow!("Failed to parse OTLP logs request: {}", e))
    })?;
    debug!(
        elapsed_us = parse_start.elapsed().as_micros() as u64,
        signal = "logs",
        records = grouped.total_records,
        "parse"
    );

    let severity_enabled = state.partition_logs_by_severity;
    if let Some(ref batcher) = state.batcher {
        process_signal_batched(
            batcher,
            grouped,
            body_len,
            start,
            SignalType::Logs,
            severity_enabled,
        )
        .await
    } else {
        process_signal_direct(grouped, start, SignalType::Logs, severity_enabled).await
    }
}

async fn process_traces(
    state: &AppState,
    format: InputFormat,
    body: axum::body::Bytes,
) -> Result<Response, AppError> {
    let start = Instant::now();
    let body_len = body.len();
    counter!("otlp.ingest.requests", "signal" => "traces").increment(1);
    histogram!("otlp.ingest.bytes", "signal" => "traces").record(body_len as f64);

    let parse_start = Instant::now();
    let grouped = decode_traces_partitioned(&body, format).map_err(|e| {
        AppError::bad_request(anyhow::anyhow!(
            "Failed to parse OTLP traces request: {}",
            e
        ))
    })?;
    debug!(
        elapsed_us = parse_start.elapsed().as_micros() as u64,
        signal = "traces",
        spans = grouped.total_records,
        "parse"
    );

    if let Some(ref batcher) = state.trace_batcher {
        process_signal_batched(batcher, grouped, body_len, start, SignalType::Traces, false).await
    } else {
        process_signal_direct(grouped, start, SignalType::Traces, false).await
    }
}

/// Process a signal with batching — accumulate in memory, flush when thresholds hit.
async fn process_signal_batched(
    batcher: &crate::batch::BatchManager,
    grouped: ServiceGroupedBatches,
    body_len: usize,
    start: Instant,
    signal_type: SignalType,
    severity_enabled: bool,
) -> Result<Response, AppError> {
    let mut total_records: usize = 0;
    let mut buffered_records: usize = 0;
    let mut flushed_paths = Vec::new();
    let signal_str = signal_type.as_str();

    let batch_count = grouped.batches.len().max(1);
    let approx_bytes_per_batch = body_len / batch_count;

    let write_start = Instant::now();
    for pb in grouped.batches {
        if pb.batch.num_rows() == 0 {
            continue;
        }

        total_records += pb.record_count;
        counter!("otlp.ingest.records", "signal" => signal_str)
            .increment(pb.record_count as u64);

        let (completed, _metadata) = batcher
            .ingest(&pb, approx_bytes_per_batch)
            .map_err(|e| AppError::internal(anyhow::anyhow!("Batch ingestion failed: {}", e)))?;

        if completed.is_empty() {
            buffered_records += pb.record_count;
            debug!(
                service = %pb.service_name,
                records = pb.record_count,
                signal = signal_str,
                "Buffered"
            );
        } else {
            for batch in completed {
                let paths = persist_batch(&batch, signal_type, severity_enabled)
                    .await
                    .map_err(|e| {
                        AppError::internal(anyhow::anyhow!("Failed to flush batch: {}", e))
                    })?;

                for path in &paths {
                    info!(
                        path = %path,
                        signal = signal_str,
                        service = %batch.metadata.service_name,
                        rows = batch.metadata.record_count,
                        "Flushed batch (threshold)"
                    );
                }
                flushed_paths.extend(paths);
            }
        }
    }

    debug!(
        elapsed_us = write_start.elapsed().as_micros() as u64,
        signal = signal_str,
        "batch_ingest"
    );

    histogram!("otlp.ingest.latency_ms", "signal" => signal_str)
        .record(start.elapsed().as_secs_f64() * 1000.0);

    let response = Json(json!({
        "status": "ok",
        "mode": "batched",
        "records_processed": total_records,
        "records_buffered": buffered_records,
        "flush_count": flushed_paths.len(),
        "partitions": flushed_paths,
    }));

    Ok((StatusCode::OK, response).into_response())
}

/// Process a signal directly — write each batch immediately (no batching).
async fn process_signal_direct(
    grouped: ServiceGroupedBatches,
    start: Instant,
    signal_type: SignalType,
    severity_enabled: bool,
) -> Result<Response, AppError> {
    let signal_str = signal_type.as_str();

    let write_start = Instant::now();
    let (uploaded_paths, total_records) =
        write_grouped_batches(grouped, signal_type, None, severity_enabled).await?;
    debug!(
        elapsed_us = write_start.elapsed().as_micros() as u64,
        signal = signal_str,
        "write"
    );

    histogram!("otlp.ingest.latency_ms", "signal" => signal_str)
        .record(start.elapsed().as_secs_f64() * 1000.0);

    if total_records == 0 {
        return Ok((
            StatusCode::OK,
            Json(json!({
                "status": "ok",
                "message": format!("No {} to process", signal_str),
            })),
        )
            .into_response());
    }

    let response = Json(json!({
        "status": "ok",
        "mode": "direct",
        "records_processed": total_records,
        "partitions": uploaded_paths,
    }));

    Ok((StatusCode::OK, response).into_response())
}

async fn process_metrics(
    format: InputFormat,
    body: axum::body::Bytes,
) -> Result<Response, AppError> {
    let start = Instant::now();
    counter!("otlp.ingest.requests", "signal" => "metrics").increment(1);
    histogram!("otlp.ingest.bytes", "signal" => "metrics").record(body.len() as f64);

    let parse_start = Instant::now();
    let partitioned = decode_metrics_partitioned(&body, format).map_err(|e| {
        AppError::bad_request(anyhow::anyhow!(
            "Failed to parse OTLP metrics request: {}",
            e
        ))
    })?;
    report_skipped_metrics(&partitioned.skipped);
    debug!(
        elapsed_us = parse_start.elapsed().as_micros() as u64,
        signal = "metrics",
        gauge_batches = partitioned.gauge.len(),
        sum_batches = partitioned.sum.len(),
        histogram_batches = partitioned.histogram.len(),
        exp_histogram_batches = partitioned.exp_histogram.len(),
        "parse"
    );

    let gauge_count = partitioned.gauge.total_records;
    let sum_count = partitioned.sum.total_records;
    let histogram_count = partitioned.histogram.total_records;
    let exp_histogram_count = partitioned.exp_histogram.total_records;

    let write_start = Instant::now();
    let mut uploaded_paths = Vec::new();

    uploaded_paths.extend(write_metric_batches(MetricType::Gauge, partitioned.gauge).await?);
    uploaded_paths.extend(write_metric_batches(MetricType::Sum, partitioned.sum).await?);
    uploaded_paths
        .extend(write_metric_batches(MetricType::Histogram, partitioned.histogram).await?);
    uploaded_paths.extend(
        write_metric_batches(MetricType::ExponentialHistogram, partitioned.exp_histogram).await?,
    );

    debug!(
        elapsed_us = write_start.elapsed().as_micros() as u64,
        signal = "metrics",
        "write"
    );

    if uploaded_paths.is_empty() {
        return Ok((
            StatusCode::OK,
            Json(json!({
                "status": "ok",
                "message": "No metrics data points to process",
            })),
        )
            .into_response());
    }

    let total_data_points = gauge_count
        + sum_count
        + histogram_count
        + exp_histogram_count
        + partitioned.skipped.summaries
        + partitioned.skipped.nan_values
        + partitioned.skipped.infinity_values
        + partitioned.skipped.missing_values;

    counter!("otlp.ingest.records", "signal" => "metrics").increment(total_data_points as u64);

    histogram!("otlp.ingest.latency_ms", "signal" => "metrics")
        .record(start.elapsed().as_secs_f64() * 1000.0);

    let response = Json(json!({
        "status": "ok",
        "data_points_processed": gauge_count + sum_count + histogram_count + exp_histogram_count,
        "gauge_count": gauge_count,
        "sum_count": sum_count,
        "histogram_count": histogram_count,
        "exponential_histogram_count": exp_histogram_count,
        "summary_count": partitioned.skipped.summaries,
        "partitions": uploaded_paths,
    }));

    Ok((StatusCode::OK, response).into_response())
}

async fn write_metric_batches(
    metric_type: MetricType,
    grouped: ServiceGroupedBatches,
) -> Result<Vec<String>, AppError> {
    if grouped.is_empty() {
        return Ok(Vec::new());
    }

    match metric_type {
        MetricType::Gauge
        | MetricType::Sum
        | MetricType::Histogram
        | MetricType::ExponentialHistogram => {}
        MetricType::Summary => {
            warn!(
                metric_type = ?metric_type,
                count = grouped.total_records,
                "Unsupported metric type - data not persisted"
            );
            return Ok(Vec::new());
        }
    };

    let (paths, _records) = write_grouped_batches(
        grouped,
        SignalType::Metrics,
        Some(metric_type.as_str()),
        false,
    )
    .await?;

    Ok(paths)
}

/// Persist a completed batch from the BatchManager to storage.
/// Merges all accumulated RecordBatches into one before writing, so a single
/// flush produces K files (one per severity) instead of N×K.
pub(crate) async fn persist_batch(
    completed: &CompletedBatch,
    signal_type: SignalType,
    severity_enabled: bool,
) -> Result<Vec<String>, anyhow::Error> {
    let merged = merge_record_batches(&completed.batches)?;

    let merged = match merged {
        Some(batch) => batch,
        None => {
            if completed.metadata.record_count > 0 {
                return Err(anyhow::anyhow!(
                    "Data loss detected: {} batch for service '{}' reports {} rows but all batches are empty",
                    signal_type.as_str(),
                    completed.metadata.service_name,
                    completed.metadata.record_count
                ));
            }
            return Ok(Vec::new());
        }
    };

    let written = write_batch_with_severity(
        &merged,
        signal_type,
        None,
        &completed.metadata.service_name,
        completed.metadata.first_timestamp_micros,
        severity_enabled,
    )
    .await?;

    counter!("otlp.batch.flushes", "signal" => signal_type.as_str())
        .increment(written.len() as u64);

    Ok(written)
}

/// Merge multiple RecordBatches into one, filtering out empty batches.
/// Returns None if all batches are empty.
pub(crate) fn merge_record_batches(
    batches: &[arrow::array::RecordBatch],
) -> anyhow::Result<Option<arrow::array::RecordBatch>> {
    let non_empty: Vec<&arrow::array::RecordBatch> =
        batches.iter().filter(|b| b.num_rows() > 0).collect();

    if non_empty.is_empty() {
        return Ok(None);
    }

    let schema = non_empty[0].schema();
    let merged = arrow::compute::concat_batches(&schema, non_empty.into_iter())
        .map_err(|e| anyhow::anyhow!("Failed to merge batches: {}", e))?;

    Ok(Some(merged))
}

/// Write a batch (or severity-split sub-batches) to storage.
/// When severity is disabled, writes the batch directly without cloning.
async fn write_batch_with_severity(
    batch: &arrow::array::RecordBatch,
    signal_type: SignalType,
    metric_type: Option<&str>,
    service_name: &str,
    timestamp_micros: i64,
    severity_enabled: bool,
) -> Result<Vec<String>, anyhow::Error> {
    let mut paths = Vec::new();

    if severity_enabled {
        for (sev, sub_batch) in crate::codec::split_batch_by_severity(batch)? {
            let path = crate::writer::write_batch(crate::writer::WriteBatchRequest {
                batch: &sub_batch,
                signal_type,
                metric_type,
                service_name,
                timestamp_micros,
                severity: Some(sev),
            })
            .await?;
            paths.push(path);
        }
    } else {
        let path = crate::writer::write_batch(crate::writer::WriteBatchRequest {
            batch,
            signal_type,
            metric_type,
            service_name,
            timestamp_micros,
            severity: None,
        })
        .await?;
        paths.push(path);
    }

    Ok(paths)
}

async fn write_grouped_batches(
    grouped: ServiceGroupedBatches,
    signal_type: SignalType,
    metric_type: Option<&'static str>,
    severity_enabled: bool,
) -> Result<(Vec<String>, usize), AppError> {
    let mut paths = Vec::new();
    let mut total_records = 0usize;
    let signal_str = signal_type.as_str();

    for pb in grouped.batches {
        if pb.batch.num_rows() == 0 {
            continue;
        }

        total_records += pb.record_count;

        // Record counters for non-metric signals (metrics handles its own counting)
        if metric_type.is_none() {
            counter!("otlp.ingest.records", "signal" => signal_str)
                .increment(pb.record_count as u64);
            histogram!("otlp.batch.rows", "signal" => signal_str)
                .record(pb.record_count as f64);
        }

        let written = write_batch_with_severity(
            &pb.batch,
            signal_type,
            metric_type,
            &pb.service_name,
            pb.min_timestamp_micros,
            severity_enabled,
        )
        .await
        .map_err(|e| {
            AppError::internal(anyhow::anyhow!(
                "Failed to write {} to storage: {}",
                signal_str,
                e
            ))
        })?;

        for path in &written {
            match metric_type {
                Some(mt) => {
                    counter!("otlp.flushes", "signal" => "metrics", "metric_type" => mt)
                        .increment(1);
                }
                None => {
                    counter!("otlp.flushes", "signal" => signal_str).increment(1);
                }
            }
            info!(
                path = %path,
                signal = signal_str,
                service = %pb.service_name,
                "Committed batch"
            );
        }
        paths.extend(written);
    }

    Ok((paths, total_records))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{ArrayRef, RecordBatch, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;

    fn make_batch(rows: &[&str]) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("severity_text", DataType::Utf8, false),
            Field::new("body", DataType::Utf8, false),
        ]));
        let sev = StringArray::from(rows.iter().map(|s| Some(*s)).collect::<Vec<_>>());
        let body = StringArray::from(rows.iter().map(|_| Some("msg")).collect::<Vec<_>>());
        RecordBatch::try_new(
            schema,
            vec![Arc::new(sev) as ArrayRef, Arc::new(body) as ArrayRef],
        )
        .unwrap()
    }

    fn empty_batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("severity_text", DataType::Utf8, false),
            Field::new("body", DataType::Utf8, false),
        ]));
        RecordBatch::new_empty(schema)
    }

    #[test]
    fn test_merge_multiple_batches_preserves_all_rows() {
        let b1 = make_batch(&["INFO", "ERROR"]);
        let b2 = make_batch(&["WARN"]);
        let b3 = make_batch(&["DEBUG", "INFO", "FATAL"]);

        let merged = merge_record_batches(&[b1, b2, b3]).unwrap().unwrap();
        assert_eq!(merged.num_rows(), 6);

        let sev_col = merged
            .column_by_name("severity_text")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(sev_col.value(0), "INFO");
        assert_eq!(sev_col.value(1), "ERROR");
        assert_eq!(sev_col.value(2), "WARN");
        assert_eq!(sev_col.value(3), "DEBUG");
        assert_eq!(sev_col.value(4), "INFO");
        assert_eq!(sev_col.value(5), "FATAL");
    }

    #[test]
    fn test_merge_all_empty_returns_none() {
        let result = merge_record_batches(&[empty_batch(), empty_batch()]).unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn test_merge_no_batches_returns_none() {
        let result = merge_record_batches(&[]).unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn test_merge_mixed_empty_and_nonempty() {
        let b1 = empty_batch();
        let b2 = make_batch(&["ERROR", "WARN"]);
        let b3 = empty_batch();
        let b4 = make_batch(&["INFO"]);

        let merged = merge_record_batches(&[b1, b2, b3, b4]).unwrap().unwrap();
        assert_eq!(merged.num_rows(), 3);
    }

    #[test]
    fn test_merge_single_batch() {
        let b = make_batch(&["INFO", "ERROR"]);
        let merged = merge_record_batches(std::slice::from_ref(&b))
            .unwrap()
            .unwrap();
        assert_eq!(merged.num_rows(), 2);
    }

    #[test]
    fn test_merge_schema_mismatch_errors() {
        let schema_a = Arc::new(Schema::new(vec![Field::new("a", DataType::Utf8, false)]));
        let schema_b = Arc::new(Schema::new(vec![Field::new("b", DataType::Int32, false)]));

        let b1 = RecordBatch::try_new(
            schema_a,
            vec![Arc::new(StringArray::from(vec!["x"])) as ArrayRef],
        )
        .unwrap();
        let b2 = RecordBatch::try_new(
            schema_b,
            vec![Arc::new(arrow::array::Int32Array::from(vec![1])) as ArrayRef],
        )
        .unwrap();

        let result = merge_record_batches(&[b1, b2]);
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn persist_batch_returns_err_when_metadata_reports_rows_but_batches_empty() {
        use crate::batch::{CompletedBatch, SignalMetadata};

        let completed = CompletedBatch {
            batches: vec![empty_batch(), empty_batch()],
            metadata: SignalMetadata {
                service_name: Arc::from("test-service"),
                first_timestamp_micros: 1_700_000_000_000_000,
                record_count: 42,
            },
        };

        let result = persist_batch(&completed, SignalType::Logs, false).await;
        assert!(
            result.is_err(),
            "persist_batch should return Err when metadata reports rows but batches are empty, got Ok({:?})",
            result.unwrap()
        );
    }

    #[tokio::test]
    async fn persist_batch_returns_err_when_metadata_reports_rows_but_no_batches() {
        use crate::batch::{CompletedBatch, SignalMetadata};

        let completed = CompletedBatch {
            batches: vec![],
            metadata: SignalMetadata {
                service_name: Arc::from("trace-service"),
                first_timestamp_micros: 1_700_000_000_000_000,
                record_count: 10,
            },
        };

        let result = persist_batch(&completed, SignalType::Traces, false).await;
        assert!(
            result.is_err(),
            "persist_batch should return Err when metadata reports rows but batch vec is empty, got Ok({:?})",
            result.unwrap()
        );
    }

    #[tokio::test]
    async fn persist_batch_error_message_contains_diagnostic_context() {
        use crate::batch::{CompletedBatch, SignalMetadata};

        let completed = CompletedBatch {
            batches: vec![empty_batch()],
            metadata: SignalMetadata {
                service_name: Arc::from("payment-api"),
                first_timestamp_micros: 1_700_000_000_000_000,
                record_count: 99,
            },
        };

        let err = persist_batch(&completed, SignalType::Logs, false)
            .await
            .unwrap_err();
        let msg = err.to_string();

        assert!(msg.contains("logs"), "got: {}", msg);
        assert!(msg.contains("payment-api"), "got: {}", msg);
        assert!(msg.contains("99"), "got: {}", msg);
    }

    #[tokio::test]
    async fn persist_batch_error_uses_correct_signal_type() {
        use crate::batch::{CompletedBatch, SignalMetadata};

        let completed = CompletedBatch {
            batches: vec![],
            metadata: SignalMetadata {
                service_name: Arc::from("order-service"),
                first_timestamp_micros: 1_700_000_000_000_000,
                record_count: 7,
            },
        };

        let err = persist_batch(&completed, SignalType::Traces, false)
            .await
            .unwrap_err();
        let msg = err.to_string();

        assert!(msg.contains("traces"), "got: {}", msg);
        assert!(msg.contains("order-service"), "got: {}", msg);
    }

    #[test]
    fn test_merge_preserves_schema_fields() {
        let b1 = make_batch(&["INFO"]);
        let b2 = make_batch(&["ERROR"]);

        let merged = merge_record_batches(&[b1.clone(), b2]).unwrap().unwrap();
        assert_eq!(merged.schema(), b1.schema());
    }

    #[test]
    fn test_merge_many_small_batches() {
        let batches: Vec<RecordBatch> = (0..50).map(|_| make_batch(&["INFO"])).collect();

        let merged = merge_record_batches(&batches).unwrap().unwrap();
        assert_eq!(merged.num_rows(), 50);
    }

    #[tokio::test]
    async fn persist_batch_data_loss_message_is_alertable() {
        use crate::batch::{CompletedBatch, SignalMetadata};

        let completed = CompletedBatch {
            batches: vec![empty_batch()],
            metadata: SignalMetadata {
                service_name: Arc::from("svc"),
                first_timestamp_micros: 1_000_000,
                record_count: 1,
            },
        };

        let err = persist_batch(&completed, SignalType::Logs, false)
            .await
            .unwrap_err();
        assert!(
            err.to_string().contains("Data loss detected"),
            "Error message must contain 'Data loss detected' for monitoring alerts, got: {}",
            err
        );
    }
}

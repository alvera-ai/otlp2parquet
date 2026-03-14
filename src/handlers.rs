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
        counter!("otlp.ingest.rejected").increment(1);
        return Err(AppError::with_status(
            StatusCode::PAYLOAD_TOO_LARGE,
            anyhow::anyhow!("payload {} exceeds limit {}", body.len(), max_payload),
        ));
    }

    match signal {
        SignalType::Logs => process_logs(state, format, body).await,
        SignalType::Traces => process_traces(format, body).await,
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
    counter!("otlp.ingest.requests").increment(1);
    histogram!("otlp.ingest.bytes").record(body_len as f64);

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

    // Use batching if enabled, otherwise write directly
    let severity_enabled = state.partition_logs_by_severity;
    if let Some(ref batcher) = state.batcher {
        process_logs_batched(batcher, grouped, body_len, start, severity_enabled).await
    } else {
        process_logs_direct(grouped, start, severity_enabled).await
    }
}

/// Process logs with batching - accumulate in memory, flush when thresholds hit
async fn process_logs_batched(
    batcher: &crate::batch::BatchManager,
    grouped: ServiceGroupedBatches,
    body_len: usize,
    start: Instant,
    severity_enabled: bool,
) -> Result<Response, AppError> {
    let mut total_records: usize = 0;
    let mut buffered_records: usize = 0;
    let mut flushed_paths = Vec::new();

    // Approximate bytes per batch (distribute body size across batches)
    let batch_count = grouped.batches.len().max(1);
    let approx_bytes_per_batch = body_len / batch_count;

    let write_start = Instant::now();
    for pb in grouped.batches {
        if pb.batch.num_rows() == 0 {
            continue;
        }

        total_records += pb.record_count;
        counter!("otlp.ingest.records").increment(pb.record_count as u64);

        // Ingest into batcher - may return completed batches if thresholds hit
        let (completed, _metadata) = batcher
            .ingest(&pb, approx_bytes_per_batch)
            .map_err(|e| AppError::internal(anyhow::anyhow!("Batch ingestion failed: {}", e)))?;

        if completed.is_empty() {
            // Records buffered, not yet flushed
            buffered_records += pb.record_count;
            debug!(
                service = %pb.service_name,
                records = pb.record_count,
                "Buffered logs"
            );
        } else {
            // Thresholds hit - flush completed batches
            for batch in completed {
                let paths = persist_log_batch(&batch, severity_enabled)
                    .await
                    .map_err(|e| {
                        AppError::internal(anyhow::anyhow!("Failed to flush batch: {}", e))
                    })?;

                for path in &paths {
                    info!(
                        path = %path,
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
        signal = "logs",
        "batch_ingest"
    );

    histogram!("otlp.ingest.latency_ms").record(start.elapsed().as_secs_f64() * 1000.0);

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

/// Process logs directly - write each batch immediately (no batching)
async fn process_logs_direct(
    grouped: ServiceGroupedBatches,
    start: Instant,
    severity_enabled: bool,
) -> Result<Response, AppError> {
    let write_start = Instant::now();
    let (uploaded_paths, total_records) = write_grouped_batches(
        grouped,
        SignalType::Logs,
        None,
        "logs to storage",
        BatchWriteMode::Logs,
        severity_enabled,
    )
    .await?;
    debug!(
        elapsed_us = write_start.elapsed().as_micros() as u64,
        signal = "logs",
        "write"
    );

    histogram!("otlp.ingest.latency_ms").record(start.elapsed().as_secs_f64() * 1000.0);

    let response = Json(json!({
        "status": "ok",
        "mode": "direct",
        "records_processed": total_records,
        "flush_count": uploaded_paths.len(),
        "partitions": uploaded_paths,
    }));

    Ok((StatusCode::OK, response).into_response())
}

async fn process_traces(
    format: InputFormat,
    body: axum::body::Bytes,
) -> Result<Response, AppError> {
    let start = Instant::now();
    counter!("otlp.ingest.requests", "signal" => "traces").increment(1);
    histogram!("otlp.ingest.bytes", "signal" => "traces").record(body.len() as f64);

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

    let write_start = Instant::now();
    let (uploaded_paths, spans_processed) = write_grouped_batches(
        grouped,
        SignalType::Traces,
        None,
        "traces to storage",
        BatchWriteMode::Traces,
        false,
    )
    .await?;
    debug!(
        elapsed_us = write_start.elapsed().as_micros() as u64,
        signal = "traces",
        "write"
    );

    if spans_processed == 0 {
        return Ok((
            StatusCode::OK,
            Json(json!({
                "status": "ok",
                "message": "No trace spans to process",
            })),
        )
            .into_response());
    }

    histogram!("otlp.ingest.latency_ms", "signal" => "traces")
        .record(start.elapsed().as_secs_f64() * 1000.0);

    let response = Json(json!({
        "status": "ok",
        "spans_processed": spans_processed,
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

    // Validate supported metric types
    match metric_type {
        MetricType::Gauge
        | MetricType::Sum
        | MetricType::Histogram
        | MetricType::ExponentialHistogram => {}
        _ => {
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
        "metrics to storage",
        BatchWriteMode::Metrics {
            metric_type: metric_type.as_str(),
        },
        false,
    )
    .await?;

    Ok(paths)
}

/// Persist a completed batch from the BatchManager to storage.
/// Used by background flush and shutdown handlers.
pub(crate) async fn persist_log_batch(
    completed: &CompletedBatch,
    severity_enabled: bool,
) -> Result<Vec<String>, anyhow::Error> {
    let mut paths = Vec::new();

    for batch in &completed.batches {
        if batch.num_rows() == 0 {
            continue;
        }

        let written = write_batch_with_severity(
            batch,
            SignalType::Logs,
            None,
            &completed.metadata.service_name,
            completed.metadata.first_timestamp_micros,
            severity_enabled,
        )
        .await?;

        counter!("otlp.batch.flushes").increment(written.len() as u64);
        paths.extend(written);
    }

    Ok(paths)
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

enum BatchWriteMode {
    Logs,
    Traces,
    Metrics { metric_type: &'static str },
}

async fn write_grouped_batches(
    grouped: ServiceGroupedBatches,
    signal_type: SignalType,
    metric_type: Option<&str>,
    error_context: &'static str,
    mode: BatchWriteMode,
    severity_enabled: bool,
) -> Result<(Vec<String>, usize), AppError> {
    let mut paths = Vec::new();
    let mut total_records = 0usize;

    for pb in grouped.batches {
        if pb.batch.num_rows() == 0 {
            continue;
        }

        total_records += pb.record_count;

        // Record ingestion counters + histogram at the original batch level (pre-split)
        match mode {
            BatchWriteMode::Logs => {
                counter!("otlp.ingest.records").increment(pb.record_count as u64);
                histogram!("otlp.batch.rows").record(pb.record_count as f64);
            }
            BatchWriteMode::Traces => {
                counter!("otlp.ingest.records", "signal" => "traces")
                    .increment(pb.record_count as u64);
                histogram!("otlp.batch.rows", "signal" => "traces").record(pb.record_count as f64);
            }
            BatchWriteMode::Metrics { .. } => {}
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
            AppError::internal(anyhow::anyhow!("Failed to write {}: {}", error_context, e))
        })?;

        for path in &written {
            match mode {
                BatchWriteMode::Logs => {
                    counter!("otlp.batch.flushes").increment(1);
                    info!("Committed batch path={} service={}", path, pb.service_name);
                }
                BatchWriteMode::Traces => {
                    counter!("otlp.traces.flushes").increment(1);
                    info!(
                        "Committed traces batch path={} service={}",
                        path, pb.service_name
                    );
                }
                BatchWriteMode::Metrics { metric_type } => {
                    counter!("otlp.metrics.flushes", "metric_type" => metric_type).increment(1);
                    info!(
                        "Committed metrics batch path={} service={}",
                        path, pb.service_name
                    );
                }
            }
        }
        paths.extend(written);
    }

    Ok((paths, total_records))
}

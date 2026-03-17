// Server mode - Full-featured HTTP server with multi-backend storage
//
// This is the default/general-purpose mode that can run anywhere:
// - Docker containers
// - Kubernetes
// - Local development
// - VM instances
//
// Features:
// - Axum HTTP server (HTTP/1.1, HTTP/2)
// - Multi-backend storage (S3-compatible, filesystem)
// - Structured logging with tracing
// - Graceful shutdown
// - Production-ready

use anyhow::{Context, Result};
use axum::{
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::{get, post},
    Json, Router,
};

pub mod config;
pub mod types;

pub use config::{
    BatchConfig, EnvSource, FsConfig, LogFormat, Platform, RequestConfig, RuntimeConfig,
    ServerConfig, StorageBackend, StorageConfig, ENV_PREFIX,
};
pub use otlp2records::InputFormat;
pub use types::{Blake3Hash, MetricType, SignalKey, SignalType};

mod batch;
pub mod codec;

use batch::{BatchConfig as BatcherConfig, BatchManager};
use metrics::counter;
use serde_json::json;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::signal;
use tower_http::decompression::RequestDecompressionLayer;
use tracing::{debug, error, info};

mod handlers;
mod init;
mod writer;

pub mod connect;

use handlers::{handle_logs, handle_metrics, handle_traces, health_check, ready_check};
pub use init::init_tracing;
use init::init_writer;

/// Application state shared across all requests
#[derive(Clone)]
pub(crate) struct AppState {
    pub batcher: Option<Arc<BatchManager>>,
    pub trace_batcher: Option<Arc<BatchManager>>,
    pub max_payload_bytes: usize,
    pub partition_logs_by_severity: bool,
}

/// Error type that implements IntoResponse
pub(crate) struct AppError {
    status: StatusCode,
    error: anyhow::Error,
}

impl IntoResponse for AppError {
    fn into_response(self) -> Response {
        error!("Request error: {:?}", self.error);
        (
            self.status,
            Json(json!({
                "error": self.error.to_string(),
            })),
        )
            .into_response()
    }
}

impl AppError {
    pub fn with_status(status: StatusCode, error: anyhow::Error) -> Self {
        Self { status, error }
    }

    pub fn bad_request<E>(error: E) -> Self
    where
        E: Into<anyhow::Error>,
    {
        Self {
            status: StatusCode::BAD_REQUEST,
            error: error.into(),
        }
    }

    pub fn internal<E>(error: E) -> Self
    where
        E: Into<anyhow::Error>,
    {
        Self {
            status: StatusCode::INTERNAL_SERVER_ERROR,
            error: error.into(),
        }
    }
}

/// Graceful shutdown handler
async fn shutdown_signal() {
    let ctrl_c = async {
        if let Err(e) = signal::ctrl_c().await {
            tracing::error!("Failed to install Ctrl+C handler: {}", e);
        }
    };

    #[cfg(unix)]
    let terminate = async {
        match signal::unix::signal(signal::unix::SignalKind::terminate()) {
            Ok(mut sig) => {
                sig.recv().await;
            }
            Err(e) => {
                tracing::error!("Failed to install SIGTERM handler: {}", e);
            }
        }
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {
            info!("Received Ctrl+C, starting graceful shutdown...");
        },
        _ = terminate => {
            info!("Received SIGTERM, starting graceful shutdown...");
        },
    }
}

/// Entry point for server mode (loads config automatically)
pub async fn run() -> Result<()> {
    let config = RuntimeConfig::load().context("Failed to load configuration")?;
    run_with_config(config).await
}

/// Entry point for server mode with pre-loaded configuration (for CLI usage)
pub async fn run_with_config(config: RuntimeConfig) -> Result<()> {
    // Initialize tracing with config
    init_tracing(&config);

    // Configure Parquet writer properties before first use

    info!("Server mode - full-featured HTTP server with multi-backend storage");

    // Get listen address from config
    let addr = config
        .server
        .as_ref()
        .ok_or_else(|| anyhow::anyhow!("server config required"))?
        .listen_addr
        .clone();

    // Initialize storage
    init_writer(&config)?;

    // Configure batching
    let batch_config = BatcherConfig {
        max_rows: config.batch.max_rows,
        max_bytes: config.batch.max_bytes,
        max_age: Duration::from_secs(config.batch.max_age_secs),
    };

    let (batcher, trace_batcher) = if !config.batch.enabled {
        info!("Batching disabled by configuration");
        (None, None)
    } else {
        info!(
            "Batching enabled (max_rows={} max_bytes={} max_age={}s)",
            batch_config.max_rows,
            batch_config.max_bytes,
            batch_config.max_age.as_secs()
        );
        (
            Some(Arc::new(BatchManager::new(batch_config.clone()))),
            Some(Arc::new(BatchManager::new(batch_config))),
        )
    };

    let max_payload_bytes = config.request.max_payload_bytes;
    info!("Max payload size set to {} bytes", max_payload_bytes);

    // Create app state
    let state = AppState {
        batcher,
        trace_batcher,
        max_payload_bytes,
        partition_logs_by_severity: config.storage.partition_logs_by_severity,
    };

    let router_state = state.clone();

    // Build router with gzip decompression support
    // OTel collectors typically send gzip-compressed payloads by default
    let app = Router::new()
        .route("/v1/logs", post(handle_logs))
        .route("/v1/traces", post(handle_traces))
        .route("/v1/metrics", post(handle_metrics))
        .route("/health", get(health_check))
        .route("/ready", get(ready_check))
        .layer(RequestDecompressionLayer::new().gzip(true))
        .with_state(router_state);

    // Create TCP listener
    let listener = tokio::net::TcpListener::bind(&addr)
        .await
        .with_context(|| format!("Failed to bind to {}", addr))?;

    info!("OTLP HTTP endpoint listening on http://{}", addr);
    info!("Routes:");
    info!("  POST http://{}/v1/logs    - OTLP log ingestion", addr);
    info!("  POST http://{}/v1/metrics - OTLP metrics ingestion", addr);
    info!("  POST http://{}/v1/traces  - OTLP trace ingestion", addr);
    info!("  GET  http://{}/health     - Health check", addr);
    info!("  GET  http://{}/ready      - Readiness check", addr);
    info!("Press Ctrl+C or send SIGTERM to stop");

    // Spawn background flush task if batching is enabled
    let shutdown_flag = Arc::new(AtomicBool::new(false));
    let flush_handle = if state.batcher.is_some() || state.trace_batcher.is_some() {
        let flush_state = state.clone();
        let flush_shutdown = Arc::clone(&shutdown_flag);
        let flush_interval =
            Duration::from_secs(config.batch.max_age_secs.max(1) / 2).max(Duration::from_secs(1));
        Some(tokio::spawn(async move {
            run_background_flush(flush_state, flush_shutdown, flush_interval).await;
        }))
    } else {
        None
    };

    // Start server with graceful shutdown
    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await
        .context("Server error")?;

    // Signal background task to stop and wait for it
    shutdown_flag.store(true, Ordering::SeqCst);
    if let Some(handle) = flush_handle {
        if let Err(e) = handle.await {
            error!(error = ?e, "Background flush task panicked during shutdown");
        }
    }

    flush_pending_batches(&state).await?;

    info!("Server shutdown complete");

    Ok(())
}

async fn flush_pending_batches(state: &AppState) -> Result<()> {
    let logs_result = flush_signal_batches(
        &state.batcher,
        SignalType::Logs,
        state.partition_logs_by_severity,
    )
    .await;
    let traces_result =
        flush_signal_batches(&state.trace_batcher, SignalType::Traces, false).await;
    match (logs_result, traces_result) {
        (Ok(()), Ok(())) => Ok(()),
        (Err(e), Ok(())) => Err(e),
        (Ok(()), Err(e)) => Err(e),
        (Err(logs_err), Err(traces_err)) => {
            error!(error = %traces_err, "Traces flush also failed during shutdown");
            Err(logs_err.context("logs flush failed; traces flush also failed (see logs above)"))
        }
    }
}

async fn flush_signal_batches(
    batcher: &Option<Arc<BatchManager>>,
    signal_type: SignalType,
    severity_enabled: bool,
) -> Result<()> {
    let Some(batcher) = batcher else {
        return Ok(());
    };

    let pending = batcher.drain_all().with_context(|| {
        format!(
            "Failed to drain pending {} batches during shutdown",
            signal_type.as_str()
        )
    })?;

    if pending.is_empty() {
        return Ok(());
    }

    info!(
        signal = signal_type.as_str(),
        batch_count = pending.len(),
        "Flushing buffered batches before shutdown"
    );

    let batch_count = pending.len();
    let mut failures = 0usize;
    for completed in pending {
        let rows = completed.metadata.record_count;
        let service = completed.metadata.service_name.as_ref().to_string();
        match handlers::persist_batch(&completed, signal_type, severity_enabled).await {
            Ok(paths) => {
                for path in paths {
                    info!(
                        path = %path,
                        signal = signal_type.as_str(),
                        service_name = %service,
                        rows,
                        "Flushed pending batch"
                    );
                }
            }
            Err(e) => {
                failures += 1;
                counter!("otlp.batch.data_loss", "signal" => signal_type.as_str())
                    .increment(rows as u64);
                error!(
                    error = %e,
                    signal = signal_type.as_str(),
                    service_name = %service,
                    rows,
                    "Failed to flush pending batch during shutdown — data lost"
                );
            }
        }
    }

    if failures > 0 {
        return Err(anyhow::anyhow!(
            "{} of {} {} batches failed to flush during shutdown",
            failures,
            batch_count,
            signal_type.as_str()
        ));
    }

    Ok(())
}

/// Background task that periodically flushes expired batches
async fn run_background_flush(state: AppState, shutdown: Arc<AtomicBool>, interval: Duration) {
    debug!(
        "Background flush task started (interval={}s)",
        interval.as_secs()
    );

    while !shutdown.load(Ordering::SeqCst) {
        tokio::time::sleep(interval).await;

        if shutdown.load(Ordering::SeqCst) {
            break;
        }

        if let Err(e) = drain_expired_signal(
            &state.batcher,
            SignalType::Logs,
            state.partition_logs_by_severity,
        )
        .await
        {
            error!(error = %e, "Background log flush cycle failed");
        }
        if let Err(e) =
            drain_expired_signal(&state.trace_batcher, SignalType::Traces, false).await
        {
            error!(error = %e, "Background trace flush cycle failed");
        }
    }

    debug!("Background flush task stopped");
}

async fn drain_expired_signal(
    batcher: &Option<Arc<BatchManager>>,
    signal_type: SignalType,
    severity_enabled: bool,
) -> Result<()> {
    let Some(batcher) = batcher else {
        return Ok(());
    };

    let expired = batcher.drain_expired().with_context(|| {
        format!(
            "Failed to drain expired {} batches",
            signal_type.as_str()
        )
    })?;

    let mut failures = 0usize;
    for completed in expired {
        let rows = completed.metadata.record_count;
        let service = completed.metadata.service_name.as_ref().to_string();
        match handlers::persist_batch(&completed, signal_type, severity_enabled).await {
            Ok(paths) => {
                for path in &paths {
                    info!(
                        path = %path,
                        signal = signal_type.as_str(),
                        service_name = %service,
                        rows,
                        "Flushed expired batch"
                    );
                }
            }
            Err(e) => {
                failures += 1;
                counter!("otlp.batch.data_loss", "signal" => signal_type.as_str())
                    .increment(rows as u64);
                error!(
                    error = %e,
                    signal = signal_type.as_str(),
                    service_name = %service,
                    rows,
                    "Failed to flush expired batch — data lost"
                );
            }
        }
    }

    if failures > 0 {
        anyhow::bail!(
            "{} expired {} batches failed to persist — data lost",
            failures,
            signal_type.as_str()
        );
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{ArrayRef, RecordBatch, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;
    use std::time::Duration;

    use crate::batch::{BatchConfig as BatcherConfig, BatchManager};

    fn make_test_batch(rows: usize) -> otlp2records::PartitionedBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("severity_text", DataType::Utf8, false),
            Field::new("body", DataType::Utf8, false),
        ]));
        let sevs: Vec<Option<&str>> = vec![Some("INFO"); rows];
        let bodies: Vec<Option<&str>> = vec![Some("msg"); rows];
        let sev = StringArray::from(sevs);
        let body = StringArray::from(bodies);
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(sev) as ArrayRef, Arc::new(body) as ArrayRef],
        )
        .unwrap();
        otlp2records::PartitionedBatch {
            batch,
            service_name: Arc::from("test-service"),
            min_timestamp_micros: 1_700_000_000_000_000,
            record_count: rows,
        }
    }

    fn make_batcher_with_data(rows: usize) -> BatchManager {
        let config = BatcherConfig {
            max_rows: 1_000_000, // High threshold so nothing auto-flushes
            max_bytes: 100 * 1024 * 1024,
            max_age: Duration::from_secs(3600),
        };
        let manager = BatchManager::new(config);
        let pb = make_test_batch(rows);
        let (_completed, _meta) = manager.ingest(&pb, 100).unwrap();
        manager
    }

    #[tokio::test]
    async fn flush_signal_batches_returns_err_when_persist_fails() {
        let batcher = make_batcher_with_data(5);
        let batcher_opt = Some(Arc::new(batcher));

        let result = flush_signal_batches(&batcher_opt, SignalType::Logs, false).await;
        assert!(
            result.is_err(),
            "flush_signal_batches should return Err when persist_batch fails, but got Ok(())"
        );
    }

    #[tokio::test]
    async fn flush_signal_batches_returns_err_when_trace_persist_fails() {
        let batcher = make_batcher_with_data(10);
        let batcher_opt = Some(Arc::new(batcher));

        let result = flush_signal_batches(&batcher_opt, SignalType::Traces, false).await;
        assert!(
            result.is_err(),
            "flush_signal_batches should return Err when trace persist fails, but got Ok(())"
        );
    }

    #[tokio::test]
    async fn flush_pending_batches_attempts_both_signals_when_first_fails() {
        let log_batcher = make_batcher_with_data(5);
        let trace_batcher = make_batcher_with_data(10);

        let state = AppState {
            batcher: Some(Arc::new(log_batcher)),
            trace_batcher: Some(Arc::new(trace_batcher)),
            max_payload_bytes: 1024 * 1024,
            partition_logs_by_severity: false,
        };

        let result = flush_pending_batches(&state).await;
        assert!(
            result.is_err(),
            "flush_pending_batches should return Err when persists fail"
        );

        // Both batchers must have been drained
        let trace_remaining = state.trace_batcher.as_ref().unwrap().drain_all().unwrap();
        assert!(
            trace_remaining.is_empty(),
            "trace batcher should be empty (drained) even though logs flush failed first, \
             but {} batches remain",
            trace_remaining.len()
        );

        let err_msg = result.unwrap_err().to_string();
        assert!(
            err_msg.contains("logs") && err_msg.contains("also failed"),
            "Dual-failure error should mention both signals, got: {}",
            err_msg
        );
    }

    #[tokio::test]
    async fn flush_signal_batches_returns_ok_when_batcher_is_none() {
        flush_signal_batches(&None, SignalType::Logs, false)
            .await
            .expect("flush_signal_batches with None logs batcher should return Ok");

        flush_signal_batches(&None, SignalType::Traces, false)
            .await
            .expect("flush_signal_batches with None traces batcher should return Ok");
    }

    #[tokio::test]
    async fn flush_signal_batches_empty_batcher_returns_ok() {
        let config = BatcherConfig {
            max_rows: 1_000_000,
            max_bytes: 100 * 1024 * 1024,
            max_age: Duration::from_secs(3600),
        };
        let empty_batcher = BatchManager::new(config);
        let batcher_opt = Some(Arc::new(empty_batcher));

        flush_signal_batches(&batcher_opt, SignalType::Logs, false)
            .await
            .expect("should return Ok when batcher exists but has no pending data");
    }

    #[tokio::test]
    async fn flush_signal_batches_error_contains_count_and_signal() {
        let batcher = make_batcher_with_data(5);
        let batcher_opt = Some(Arc::new(batcher));

        let err = flush_signal_batches(&batcher_opt, SignalType::Logs, false)
            .await
            .unwrap_err();
        let msg = err.to_string();

        assert!(msg.contains("logs"), "got: {}", msg);
        assert!(msg.contains("1 of 1"), "got: {}", msg);
    }

    #[tokio::test]
    async fn flush_pending_batches_only_logs_batcher() {
        let log_batcher = make_batcher_with_data(5);

        let state = AppState {
            batcher: Some(Arc::new(log_batcher)),
            trace_batcher: None,
            max_payload_bytes: 1024 * 1024,
            partition_logs_by_severity: false,
        };

        let result = flush_pending_batches(&state).await;
        assert!(result.is_err(), "Should return Err from logs flush failure");
    }

    #[tokio::test]
    async fn flush_pending_batches_only_trace_batcher() {
        let trace_batcher = make_batcher_with_data(10);

        let state = AppState {
            batcher: None,
            trace_batcher: Some(Arc::new(trace_batcher)),
            max_payload_bytes: 1024 * 1024,
            partition_logs_by_severity: false,
        };

        let result = flush_pending_batches(&state).await;
        assert!(
            result.is_err(),
            "Should return Err from traces flush failure"
        );
    }

    #[tokio::test]
    async fn flush_pending_batches_logs_ok_traces_err() {
        let config = BatcherConfig {
            max_rows: 1_000_000,
            max_bytes: 100 * 1024 * 1024,
            max_age: Duration::from_secs(3600),
        };
        let empty_log_batcher = BatchManager::new(config);
        let trace_batcher = make_batcher_with_data(3);

        let state = AppState {
            batcher: Some(Arc::new(empty_log_batcher)),
            trace_batcher: Some(Arc::new(trace_batcher)),
            max_payload_bytes: 1024 * 1024,
            partition_logs_by_severity: false,
        };

        let result = flush_pending_batches(&state).await;
        assert!(
            result.is_err(),
            "Should return Err when traces flush fails even if logs flush succeeds"
        );
    }

    #[tokio::test]
    async fn flush_pending_batches_single_failure_no_also_failed() {
        let log_batcher = make_batcher_with_data(5);
        let config = BatcherConfig {
            max_rows: 1_000_000,
            max_bytes: 100 * 1024 * 1024,
            max_age: Duration::from_secs(3600),
        };
        let empty_trace_batcher = BatchManager::new(config);

        let state = AppState {
            batcher: Some(Arc::new(log_batcher)),
            trace_batcher: Some(Arc::new(empty_trace_batcher)),
            max_payload_bytes: 1024 * 1024,
            partition_logs_by_severity: false,
        };

        let err = flush_pending_batches(&state).await.unwrap_err();
        let msg = err.to_string();

        assert!(
            !msg.contains("also failed"),
            "Single-signal failure should not say 'also failed', got: {}",
            msg
        );
    }
}

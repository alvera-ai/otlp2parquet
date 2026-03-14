//! Shared types used across storage and ingestion crates
//!
//! These types are defined here to avoid circular dependencies

use std::fmt;
use std::str::FromStr;

/// OpenTelemetry signal types
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum SignalType {
    /// Logs signal
    Logs,
    /// Traces signal
    Traces,
    /// Metrics signal
    Metrics,
}

impl SignalType {
    /// Returns the string representation used in logging and metrics
    pub fn as_str(&self) -> &'static str {
        match self {
            SignalType::Logs => "logs",
            SignalType::Traces => "traces",
            SignalType::Metrics => "metrics",
        }
    }
}

/// Metric data point types (the 5 OTLP metric kinds)
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum MetricType {
    Gauge,
    Sum,
    Histogram,
    ExponentialHistogram,
    Summary,
}

impl MetricType {
    /// Returns the string representation used in file paths and table names
    pub fn as_str(&self) -> &'static str {
        match self {
            MetricType::Gauge => "gauge",
            MetricType::Sum => "sum",
            MetricType::Histogram => "histogram",
            MetricType::ExponentialHistogram => "exponential_histogram",
            MetricType::Summary => "summary",
        }
    }
}

impl fmt::Display for MetricType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for MetricType {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "gauge" => Ok(MetricType::Gauge),
            "sum" => Ok(MetricType::Sum),
            "histogram" => Ok(MetricType::Histogram),
            "exponential_histogram" => Ok(MetricType::ExponentialHistogram),
            "summary" => Ok(MetricType::Summary),
            _ => Err(format!("unknown metric type: {}", s)),
        }
    }
}

/// Unified signal identifier combining signal type and optional metric type.
///
/// This replaces string-based signal identifiers like "metrics:gauge" with
/// a typed enum that can be parsed and formatted consistently.
///
/// # Examples
/// ```
/// use otlp2parquet::types::SignalKey;
/// use std::str::FromStr;
///
/// // Parse from DO ID format
/// let key = SignalKey::from_str("metrics:gauge").unwrap();
/// assert!(matches!(key, SignalKey::Metrics(_)));
///
/// // Format back to string
/// assert_eq!(key.to_string(), "metrics:gauge");
///
/// // Simple signals
/// let logs = SignalKey::from_str("logs").unwrap();
/// assert_eq!(logs, SignalKey::Logs);
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum SignalKey {
    Logs,
    Traces,
    Metrics(MetricType),
}

impl SignalKey {
    /// Returns the base signal type
    pub fn signal_type(&self) -> SignalType {
        match self {
            SignalKey::Logs => SignalType::Logs,
            SignalKey::Traces => SignalType::Traces,
            SignalKey::Metrics(_) => SignalType::Metrics,
        }
    }

    /// Returns the metric type if this is a metrics signal
    pub fn metric_type(&self) -> Option<MetricType> {
        match self {
            SignalKey::Metrics(mt) => Some(*mt),
            _ => None,
        }
    }

    /// Returns the default table-style name for this signal
    pub fn table_name(&self) -> String {
        match self {
            SignalKey::Logs => "otel_logs".to_string(),
            SignalKey::Traces => "otel_traces".to_string(),
            SignalKey::Metrics(mt) => format!("otel_metrics_{}", mt.as_str()),
        }
    }

    /// Returns the analytics/metrics label for this signal
    pub fn analytics_label(&self) -> &'static str {
        match self {
            SignalKey::Logs => "logs",
            SignalKey::Traces => "traces",
            SignalKey::Metrics(MetricType::Gauge) => "metrics_gauge",
            SignalKey::Metrics(MetricType::Sum) => "metrics_sum",
            SignalKey::Metrics(MetricType::Histogram) => "metrics_histogram",
            SignalKey::Metrics(MetricType::ExponentialHistogram) => "metrics_exp_histogram",
            SignalKey::Metrics(MetricType::Summary) => "metrics_summary",
        }
    }
}

impl fmt::Display for SignalKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SignalKey::Logs => f.write_str("logs"),
            SignalKey::Traces => f.write_str("traces"),
            SignalKey::Metrics(mt) => write!(f, "metrics:{}", mt.as_str()),
        }
    }
}

impl FromStr for SignalKey {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if let Some(metric_str) = s.strip_prefix("metrics:") {
            let mt = MetricType::from_str(metric_str)?;
            Ok(SignalKey::Metrics(mt))
        } else {
            match s {
                "logs" => Ok(SignalKey::Logs),
                "traces" => Ok(SignalKey::Traces),
                "metrics" => Err("metrics signal requires type (e.g., metrics:gauge)".to_string()),
                _ => Err(format!("unknown signal: {}", s)),
            }
        }
    }
}

/// Normalized OTLP severity level for Hive-style partition paths.
///
/// Maps the full range of OTLP SeverityText values (including sub-levels like
/// DEBUG2, INFO3) to 7 fixed partition keys. Uses ASCII case-insensitive prefix
/// matching — zero allocations per row.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum SeverityPartition {
    Trace,
    Debug,
    Info,
    Warn,
    Error,
    Fatal,
    Unspecified,
}

impl SeverityPartition {
    /// Number of distinct severity partition variants.
    pub const VARIANT_COUNT: usize = 7;

    /// The lowercased partition key used in file paths.
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Trace => "trace",
            Self::Debug => "debug",
            Self::Info => "info",
            Self::Warn => "warn",
            Self::Error => "error",
            Self::Fatal => "fatal",
            Self::Unspecified => "unspecified",
        }
    }

    /// Map an OTLP severity_text value to a partition key.
    /// ASCII case-insensitive prefix matching, no heap allocations.
    pub fn from_severity_text(text: &str) -> Self {
        if text.is_empty() {
            return Self::Unspecified;
        }
        // str::get avoids panics on multi-byte UTF-8 char boundaries
        if text
            .get(..5)
            .is_some_and(|s| s.eq_ignore_ascii_case("trace"))
        {
            Self::Trace
        } else if text
            .get(..5)
            .is_some_and(|s| s.eq_ignore_ascii_case("debug"))
        {
            Self::Debug
        } else if text
            .get(..4)
            .is_some_and(|s| s.eq_ignore_ascii_case("info"))
        {
            Self::Info
        } else if text
            .get(..4)
            .is_some_and(|s| s.eq_ignore_ascii_case("warn"))
        {
            Self::Warn
        } else if text
            .get(..5)
            .is_some_and(|s| s.eq_ignore_ascii_case("error"))
        {
            Self::Error
        } else if text
            .get(..5)
            .is_some_and(|s| s.eq_ignore_ascii_case("fatal"))
        {
            Self::Fatal
        } else {
            Self::Unspecified
        }
    }
}

impl fmt::Display for SeverityPartition {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Blake3 content hash for deduplication
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Blake3Hash([u8; 32]);

impl Blake3Hash {
    pub fn new(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }

    pub fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }

    pub fn to_hex(&self) -> String {
        hex::encode(self.0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_metric_type_roundtrip() {
        let types = [
            MetricType::Gauge,
            MetricType::Sum,
            MetricType::Histogram,
            MetricType::ExponentialHistogram,
            MetricType::Summary,
        ];
        for mt in types {
            let s = mt.as_str();
            let parsed = MetricType::from_str(s).unwrap();
            assert_eq!(parsed, mt, "Roundtrip failed for {:?}", mt);
        }
    }

    #[test]
    fn test_signal_key_table_names() {
        assert_eq!(SignalKey::Logs.table_name(), "otel_logs");
        assert_eq!(SignalKey::Traces.table_name(), "otel_traces");
        assert_eq!(
            SignalKey::Metrics(MetricType::Gauge).table_name(),
            "otel_metrics_gauge"
        );
        assert_eq!(
            SignalKey::Metrics(MetricType::Sum).table_name(),
            "otel_metrics_sum"
        );
        assert_eq!(
            SignalKey::Metrics(MetricType::Histogram).table_name(),
            "otel_metrics_histogram"
        );
        assert_eq!(
            SignalKey::Metrics(MetricType::ExponentialHistogram).table_name(),
            "otel_metrics_exponential_histogram"
        );
        assert_eq!(
            SignalKey::Metrics(MetricType::Summary).table_name(),
            "otel_metrics_summary"
        );
    }

    #[test]
    fn test_signal_key_analytics_labels() {
        assert_eq!(SignalKey::Logs.analytics_label(), "logs");
        assert_eq!(SignalKey::Traces.analytics_label(), "traces");
        assert_eq!(
            SignalKey::Metrics(MetricType::Gauge).analytics_label(),
            "metrics_gauge"
        );
        assert_eq!(
            SignalKey::Metrics(MetricType::ExponentialHistogram).analytics_label(),
            "metrics_exp_histogram"
        );
    }

    #[test]
    fn test_signal_key_roundtrip() {
        let cases = [
            "logs",
            "traces",
            "metrics:gauge",
            "metrics:exponential_histogram",
        ];
        for input in cases {
            let key = SignalKey::from_str(input).unwrap();
            assert_eq!(key.to_string(), input, "Roundtrip failed for {}", input);
        }
    }

    #[test]
    fn test_signal_key_errors() {
        assert!(SignalKey::from_str("metrics").is_err()); // Missing metric type
        assert!(SignalKey::from_str("unknown").is_err()); // Unknown signal
        assert!(SignalKey::from_str("metrics:unknown").is_err()); // Unknown metric type
    }

    #[test]
    fn test_severity_partition_standard_values() {
        assert_eq!(
            SeverityPartition::from_severity_text("TRACE"),
            SeverityPartition::Trace
        );
        assert_eq!(
            SeverityPartition::from_severity_text("DEBUG"),
            SeverityPartition::Debug
        );
        assert_eq!(
            SeverityPartition::from_severity_text("INFO"),
            SeverityPartition::Info
        );
        assert_eq!(
            SeverityPartition::from_severity_text("WARN"),
            SeverityPartition::Warn
        );
        assert_eq!(
            SeverityPartition::from_severity_text("ERROR"),
            SeverityPartition::Error
        );
        assert_eq!(
            SeverityPartition::from_severity_text("FATAL"),
            SeverityPartition::Fatal
        );
    }

    #[test]
    fn test_severity_partition_sub_levels() {
        assert_eq!(
            SeverityPartition::from_severity_text("DEBUG2"),
            SeverityPartition::Debug
        );
        assert_eq!(
            SeverityPartition::from_severity_text("INFO3"),
            SeverityPartition::Info
        );
        assert_eq!(
            SeverityPartition::from_severity_text("TRACE4"),
            SeverityPartition::Trace
        );
        assert_eq!(
            SeverityPartition::from_severity_text("ERROR2"),
            SeverityPartition::Error
        );
    }

    #[test]
    fn test_severity_partition_case_insensitive() {
        assert_eq!(
            SeverityPartition::from_severity_text("info"),
            SeverityPartition::Info
        );
        assert_eq!(
            SeverityPartition::from_severity_text("Error"),
            SeverityPartition::Error
        );
        assert_eq!(
            SeverityPartition::from_severity_text("dEbUg"),
            SeverityPartition::Debug
        );
    }

    #[test]
    fn test_severity_partition_empty_and_unknown() {
        assert_eq!(
            SeverityPartition::from_severity_text(""),
            SeverityPartition::Unspecified
        );
        // "INFORMATION" starts with "INFO" — prefix match maps to Info
        assert_eq!(
            SeverityPartition::from_severity_text("INFORMATION"),
            SeverityPartition::Info
        );
        assert_eq!(
            SeverityPartition::from_severity_text("ERR"),
            SeverityPartition::Unspecified
        );
        assert_eq!(
            SeverityPartition::from_severity_text("OK"),
            SeverityPartition::Unspecified
        );
        assert_eq!(
            SeverityPartition::from_severity_text("FOO"),
            SeverityPartition::Unspecified
        );
    }

    #[test]
    fn test_severity_partition_non_ascii_no_panic() {
        // "abcdé": byte 5 splits a 2-byte char — must not panic
        assert_eq!(
            SeverityPartition::from_severity_text("abcdé"),
            SeverityPartition::Unspecified
        );
        // 4-byte emoji at position 4
        assert_eq!(
            SeverityPartition::from_severity_text("abc\u{1F525}"),
            SeverityPartition::Unspecified
        );
        // Multi-byte after valid prefix: "INFO" + 2-byte char
        assert_eq!(
            SeverityPartition::from_severity_text("INFOé"),
            SeverityPartition::Info
        );
        // "TRAC" + 2-byte char — byte 5 is not a char boundary
        assert_eq!(
            SeverityPartition::from_severity_text("TRACé"),
            SeverityPartition::Unspecified
        );
    }

    #[test]
    fn test_severity_partition_as_str_roundtrip() {
        let variants = [
            SeverityPartition::Trace,
            SeverityPartition::Debug,
            SeverityPartition::Info,
            SeverityPartition::Warn,
            SeverityPartition::Error,
            SeverityPartition::Fatal,
            SeverityPartition::Unspecified,
        ];
        for v in variants {
            assert_eq!(v.to_string(), v.as_str());
        }
    }
}

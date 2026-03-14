# Severity-Based Log Partitioning

## Problem

All log parquet files are written to a single path hierarchy:

```
logs/{service}/year={Y}/month={M}/day={D}/hour={H}/{ts}-{uuid}.parquet
```

Severity is stored as a column inside each parquet file but is not a partition key. Queries filtering by severity (e.g., `WHERE severity_text = 'ERROR'`) must scan every file in the time range and rely on parquet predicate pushdown — which helps but still requires opening and reading metadata from every file.

For workloads that predominantly query a single severity level (alerting on errors, debugging with debug logs), disk-level partitioning by severity eliminates entire directories from the scan.

## Proposed Path Layout

When enabled, log files are written to:

```
logs/{service}/severity={severity}/year={Y}/month={M}/day={D}/hour={H}/{ts}-{uuid}.parquet
```

Where `{severity}` is the OTLP `severity_text` value, lowercased and sanitized:

| OTLP SeverityText | Partition Value |
|---|---|
| `TRACE` | `trace` |
| `DEBUG` | `debug` |
| `INFO` | `info` |
| `WARN` | `warn` |
| `ERROR` | `error` |
| `FATAL` | `fatal` |
| (empty/missing) | `unspecified` |

When disabled (default), the current flat path layout is preserved — no behavioral change.

## Configuration

### TOML (`config.toml`)

```toml
[storage]
backend = "r2"

# Partition log files by severity level (default: false)
# Adds severity={level} to the Hive-style partition path
partition_logs_by_severity = false
```

### Environment Variable

```
OTLP2PARQUET_PARTITION_LOGS_BY_SEVERITY=true
```

### Precedence

Environment variable overrides TOML, matching the existing config priority chain.

## Implementation

### 1. Config: `src/config/mod.rs`

Add field to `StorageConfig`:

```rust
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StorageConfig {
    pub backend: StorageBackend,
    // ... existing fields ...

    /// Partition log files by severity level in the output path.
    #[serde(default)]
    pub partition_logs_by_severity: bool,
}
```

### 2. Env Override: `src/config/env_overrides.rs`

```rust
if let Some(val) = get_env_bool(env, "PARTITION_LOGS_BY_SEVERITY")? {
    config.storage.partition_logs_by_severity = val;
}
```

### 3. Storage: `src/writer/storage.rs`

Expose the config flag alongside the existing `get_storage_prefix()`:

```rust
pub fn get_partition_logs_by_severity() -> bool { ... }
```

### 4. Path Generation: `src/writer/write.rs`

Extract severity from the Arrow `RecordBatch` and inject it into the path when the flag is set.

The `otlp2records` crate produces a `severity_text` column (Utf8) in the log Arrow schema. Read the first row's value to determine the partition:

```rust
fn generate_parquet_path(
    signal_type: SignalType,
    metric_type: Option<&str>,
    service_name: &str,
    timestamp_micros: i64,
    severity: Option<&str>,       // NEW
) -> Result<String> {
    let (year, month, day, hour) = partition_from_timestamp(timestamp_micros);
    let signal_prefix = match signal_type { /* ... unchanged ... */ };
    let safe_service = sanitize_service_name(service_name);
    let suffix = Uuid::new_v4().simple();
    let storage_prefix = super::storage::get_storage_prefix().unwrap_or("");

    let severity_segment = match severity {
        Some(s) if !s.is_empty() => format!("severity={}/", s.to_lowercase()),
        _ => String::new(),
    };

    Ok(format!(
        "{}{}/{}/{}{}-{}.parquet",
        storage_prefix,
        signal_prefix,
        safe_service,
        severity_segment,                          // NEW — empty string when disabled
        format_args!("year={}/month={:02}/day={:02}/hour={:02}/{}", year, month, day, hour, timestamp_micros),
        suffix
    ))
}
```

### 5. Batch Grouping

`WriteBatchRequest` currently groups by service. When severity partitioning is enabled, a single OTLP request may contain logs at mixed severity levels. The batch must be split by severity before writing, so each parquet file lands in the correct severity partition.

Add a `split_batch_by_severity(batch: &RecordBatch) -> Vec<(String, RecordBatch)>` helper in `codec.rs` that filters the RecordBatch by distinct `severity_text` values and returns `(severity, sub_batch)` pairs.

This split happens at write time (inside `write_grouped_batches` / `persist_log_batch`), not at parse time, so it is gated by the config flag.

### 6. WriteBatchRequest

Add an optional severity field:

```rust
pub struct WriteBatchRequest<'a> {
    pub batch: &'a RecordBatch,
    pub signal_type: SignalType,
    pub metric_type: Option<&'a str>,
    pub service_name: &'a str,
    pub timestamp_micros: i64,
    pub severity: Option<&'a str>,   // NEW
}
```

When `partition_logs_by_severity = false`, callers pass `severity: None` — path generation is unchanged.

## Query Compatibility

Hive-style partitioning is automatically recognized by DuckDB, Athena, Spark, and ClickHouse:

```sql
-- DuckDB: severity is auto-inferred as a virtual column from the path
SELECT * FROM read_parquet('logs/**/severity=error/**/*.parquet');

-- Or scan all severities, engine prunes partitions
SELECT * FROM read_parquet('logs/**/*.parquet')
WHERE severity = 'error';
```

Existing queries without a `severity` filter continue to work via `/**/*.parquet` glob — they just scan all severity directories.

## Migration

No migration needed. When the flag is toggled on:

- New files are written to the new path layout
- Old files remain in the flat layout
- Queries using `/**/*.parquet` glob match both layouts

To fully migrate historical data, a one-time re-partitioning script can read old files and rewrite them to the new layout. This is optional — mixed layouts work correctly.

## Risks

| Risk | Mitigation |
|---|---|
| Mixed-severity batches written to wrong partition | Batch splitting by severity before write (step 5) |
| Increased file count (one file per severity per flush) | Batching already coalesces rows; severity cardinality is bounded (6 values) |
| Breaking change for downstream queries | Default is `false`; opt-in only. Glob patterns match both layouts |

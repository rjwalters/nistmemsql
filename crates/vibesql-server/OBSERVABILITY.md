# OpenTelemetry Observability for VibeSQL Server

This document describes the observability features available in vibesql-server using OpenTelemetry.

## Overview

VibeSQL Server implements comprehensive observability through OpenTelemetry, providing:

- **Metrics**: Connection counts, query latency, error rates, and resource utilization
- **Traces**: Distributed tracing through query execution pipeline
- **Logs**: Structured logs correlated with traces via trace/span IDs

## Configuration

Observability is configured through the `[observability]` section in `vibesql-server.toml`:

```toml
[observability]
enabled = true
level = "INFO"  # TRACE, DEBUG, INFO, WARN, ERROR

[observability.otlp]
endpoint = "http://localhost:4317"  # gRPC OTLP endpoint
protocol = "grpc"  # or "http"
timeout_seconds = 10

[observability.metrics]
enabled = true
export_interval_seconds = 60

[observability.traces]
enabled = true
sampler = "parent_based_always_on"  # or "always_on", "always_off", "trace_id_ratio"
sampling_ratio = 1.0  # 1.0 = 100%, 0.1 = 10% (only for trace_id_ratio sampler)

[observability.logs]
enabled = true
bridge_tracing = true  # Bridge existing tracing logs to OpenTelemetry
```

### Configuration Options

#### Observability Levels

The `level` option controls the verbosity of observability similar to log levels:

- **TRACE**: All metrics, 100% trace sampling, verbose logs (development only)
- **DEBUG**: All metrics, 10% trace sampling, debug logs (staging)
- **INFO**: Core metrics, 1% trace sampling, info logs (production default)
- **WARN**: Error metrics only, error trace sampling only, warn+ logs (production low-overhead)
- **ERROR**: Minimal metrics, no tracing, error logs only (emergency mode)

#### OTLP Configuration

- `endpoint`: The OTLP collector endpoint (default: `http://localhost:4317`)
- `protocol`: Either `grpc` or `http` (default: `grpc`)
- `timeout_seconds`: Timeout for export operations (default: 10)

#### Metrics Configuration

- `enabled`: Enable metrics export (default: `true`)
- `export_interval_seconds`: How often to export metrics (default: 60)

#### Traces Configuration

- `enabled`: Enable trace export (default: `true`)
- `sampler`: Sampling strategy:
  - `always_on`: Sample every trace
  - `always_off`: Sample no traces
  - `parent_based_always_on`: Sample if parent was sampled
  - `trace_id_ratio`: Sample based on ratio
- `sampling_ratio`: Ratio for `trace_id_ratio` sampler (0.0-1.0, default: 1.0)

#### Logs Configuration

- `enabled`: Enable log export (default: `true`)
- `bridge_tracing`: Bridge existing tracing logs to OpenTelemetry (default: `true`)

## Metrics

### Connection Metrics

- `vibesql_server_connections_total`: Total connections accepted
- `vibesql_server_connection_errors_total`: Connection failures by error type
- `vibesql_server_connection_duration_seconds`: Connection lifetime distribution

### Query Metrics

- `vibesql_server_queries_total`: Queries executed by statement type (SELECT, INSERT, UPDATE, DELETE)
- `vibesql_server_query_duration_seconds`: Query execution latency with percentiles (p50, p95, p99)
- `vibesql_server_query_rows_affected`: Rows affected distribution
- `vibesql_server_query_errors_total`: Query errors by error type

### Protocol Metrics

- `vibesql_server_messages_received_total`: PostgreSQL protocol messages received
- `vibesql_server_messages_sent_total`: PostgreSQL protocol messages sent
- `vibesql_server_bytes_received_total`: Total bytes received
- `vibesql_server_bytes_sent_total`: Total bytes sent

## Setting Up an OTLP Collector

### Using Jaeger (for traces)

1. Start Jaeger with OTLP support:

```bash
docker run -d --name jaeger \
  -e COLLECTOR_OTLP_ENABLED=true \
  -p 4317:4317 \
  -p 16686:16686 \
  jaegertracing/all-in-one:latest
```

2. Configure vibesql-server to use Jaeger:

```toml
[observability]
enabled = true

[observability.otlp]
endpoint = "http://localhost:4317"
```

3. Access Jaeger UI at `http://localhost:16686`

### Using Prometheus + Grafana (for metrics)

1. Start the OpenTelemetry Collector:

```bash
docker run -d --name otel-collector \
  -p 4317:4317 \
  -p 8888:8888 \
  -p 8889:8889 \
  -v $(pwd)/otel-collector-config.yaml:/etc/otel/config.yaml \
  otel/opentelemetry-collector:latest \
  --config=/etc/otel/config.yaml
```

Example `otel-collector-config.yaml`:

```yaml
receivers:
  otlp:
    protocols:
      grpc:
        endpoint: 0.0.0.0:4317

exporters:
  prometheus:
    endpoint: "0.0.0.0:8889"
  logging:
    loglevel: debug

service:
  pipelines:
    metrics:
      receivers: [otlp]
      exporters: [prometheus, logging]
```

2. Start Prometheus:

```bash
docker run -d --name prometheus \
  -p 9090:9090 \
  -v $(pwd)/prometheus.yml:/etc/prometheus/prometheus.yml \
  prom/prometheus
```

Example `prometheus.yml`:

```yaml
scrape_configs:
  - job_name: 'otel-collector'
    scrape_interval: 10s
    static_configs:
      - targets: ['localhost:8889']
```

3. Start Grafana:

```bash
docker run -d --name grafana \
  -p 3000:3000 \
  grafana/grafana
```

4. Add Prometheus as a data source in Grafana (http://localhost:3000)

### Using a Full Observability Stack

For a complete solution with metrics, traces, and logs, consider:

- **Grafana Cloud**: Managed Prometheus, Loki, and Tempo with OTLP support
- **Elastic Observability**: APM Server with OTLP support
- **Datadog**: APM with OTLP support
- **New Relic**: OTLP-compatible APM

## Performance Impact

The observability overhead depends on the configuration:

- **Metrics only (recommended for production)**: ~1-2% latency overhead
- **Traces with 1% sampling**: ~1-2% latency overhead
- **Traces with 100% sampling**: ~3-5% latency overhead
- **All features with DEBUG level**: ~5-10% latency overhead

For production deployments, we recommend:

```toml
[observability]
enabled = true
level = "INFO"

[observability.traces]
enabled = true
sampler = "trace_id_ratio"
sampling_ratio = 0.01  # 1% sampling
```

## Troubleshooting

### No Data Appearing in Collector

1. Check that the OTLP endpoint is accessible:
   ```bash
   nc -zv localhost 4317
   ```

2. Enable debug logging in vibesql-server:
   ```toml
   [observability]
   level = "DEBUG"
   ```

3. Check collector logs for errors

### High Memory Usage

Reduce the export interval and sampling ratio:

```toml
[observability.metrics]
export_interval_seconds = 30

[observability.traces]
sampler = "trace_id_ratio"
sampling_ratio = 0.1  # 10% sampling
```

### Missing Traces

Ensure traces are enabled and sampled:

```toml
[observability.traces]
enabled = true
sampler = "always_on"  # For testing
```

## Example Queries

### Prometheus Queries

**Average query latency by statement type:**
```promql
rate(vibesql_server_query_duration_seconds_sum[5m]) /
rate(vibesql_server_query_duration_seconds_count[5m])
```

**Query error rate:**
```promql
rate(vibesql_server_query_errors_total[5m])
```

**Active connections:**
```promql
vibesql_server_connections_total -
rate(vibesql_server_connection_duration_seconds_count[5m])
```

## References

- [OpenTelemetry Documentation](https://opentelemetry.io/docs/)
- [OpenTelemetry Specification](https://opentelemetry.io/docs/specs/otel/)
- [OTLP Specification](https://opentelemetry.io/docs/specs/otlp/)
- [OpenTelemetry Rust SDK](https://github.com/open-telemetry/opentelemetry-rust)

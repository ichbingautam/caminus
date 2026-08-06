use std::net::SocketAddr;
use metrics_exporter_prometheus::PrometheusBuilder;

pub fn init_metrics(port: u16) -> Result<(), Box<dyn std::error::Error>> {
    let addr = SocketAddr::from(([0, 0, 0, 0], port));
    let builder = PrometheusBuilder::new();
    builder
        .with_http_listener(addr)
        .install()?;
    println!("[Metrics Server] Listening on http://{}", addr);
    Ok(())
}

/// Helper to measure and record event processing latency
pub fn record_processing_latency(duration_seconds: f64) {
    metrics::histogram!("caminus_processing_latency_seconds").record(duration_seconds);
}

/// Helper to measure and record replication lag (duration since source commit to engine egress)
pub fn record_replication_lag(lag_seconds: f64) {
    metrics::histogram!("caminus_replication_lag_seconds").record(lag_seconds);
}

/// Helper to track pipeline throughput
pub fn increment_throughput(count: u64) {
    metrics::counter!("caminus_throughput").increment(count);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_observability_helpers() {
        // Record test metrics to ensure the macro expansions check out
        record_processing_latency(0.0125);
        record_replication_lag(0.450);
        increment_throughput(1);
    }
}

use caminus::resiliency::metrics::MetricsRegistry;

#[test]
fn test_prometheus_latency_histogram_rendering() {
    // Record various latencies
    MetricsRegistry::record_latency(800); // 0.8ms -> <= 1ms bucket
    MetricsRegistry::record_latency(4_000); // 4ms   -> <= 5ms bucket
    MetricsRegistry::record_latency(8_000); // 8ms   -> <= 10ms bucket

    let rendered = MetricsRegistry::render();

    assert!(rendered.contains("# TYPE caminus_event_latency_seconds histogram"));
    assert!(rendered.contains("caminus_event_latency_seconds_bucket{le=\"0.001\"}"));
    assert!(rendered.contains("caminus_event_latency_seconds_bucket{le=\"0.005\"}"));
    assert!(rendered.contains("caminus_event_latency_seconds_bucket{le=\"0.01\"}"));
    assert!(rendered.contains("caminus_event_latency_seconds_bucket{le=\"0.05\"}"));
    assert!(rendered.contains("caminus_event_latency_seconds_bucket{le=\"0.1\"}"));
    assert!(rendered.contains("caminus_event_latency_seconds_bucket{le=\"+Inf\"}"));
    assert!(rendered.contains("caminus_event_latency_seconds_sum"));
    assert!(rendered.contains("caminus_event_latency_seconds_count"));
}

use std::sync::atomic::{AtomicU64, Ordering};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

pub static EVENTS_INGESTED: AtomicU64 = AtomicU64::new(0);
pub static EVENTS_PROCESSED: AtomicU64 = AtomicU64::new(0);
pub static DUPLICATES_FILTERED: AtomicU64 = AtomicU64::new(0);
pub static PROCESSING_LATENCY_SUM_US: AtomicU64 = AtomicU64::new(0); // in microseconds
pub static PROCESSING_LATENCY_COUNT: AtomicU64 = AtomicU64::new(0);

pub static LATENCY_BUCKET_1MS: AtomicU64 = AtomicU64::new(0);
pub static LATENCY_BUCKET_5MS: AtomicU64 = AtomicU64::new(0);
pub static LATENCY_BUCKET_10MS: AtomicU64 = AtomicU64::new(0);
pub static LATENCY_BUCKET_50MS: AtomicU64 = AtomicU64::new(0);
pub static LATENCY_BUCKET_100MS: AtomicU64 = AtomicU64::new(0);
pub static LATENCY_BUCKET_INF: AtomicU64 = AtomicU64::new(0);

/// Central thread-safe Prometheus metrics collector and exposition server.
pub struct MetricsRegistry;

impl MetricsRegistry {
    pub fn increment_ingested() {
        EVENTS_INGESTED.fetch_add(1, Ordering::Relaxed);
    }

    pub fn increment_processed() {
        EVENTS_PROCESSED.fetch_add(1, Ordering::Relaxed);
    }

    pub fn increment_duplicates() {
        DUPLICATES_FILTERED.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_latency(duration_us: u64) {
        PROCESSING_LATENCY_SUM_US.fetch_add(duration_us, Ordering::Relaxed);
        PROCESSING_LATENCY_COUNT.fetch_add(1, Ordering::Relaxed);

        if duration_us <= 1_000 {
            LATENCY_BUCKET_1MS.fetch_add(1, Ordering::Relaxed);
        } else if duration_us <= 5_000 {
            LATENCY_BUCKET_5MS.fetch_add(1, Ordering::Relaxed);
        } else if duration_us <= 10_000 {
            LATENCY_BUCKET_10MS.fetch_add(1, Ordering::Relaxed);
        } else if duration_us <= 50_000 {
            LATENCY_BUCKET_50MS.fetch_add(1, Ordering::Relaxed);
        } else if duration_us <= 100_000 {
            LATENCY_BUCKET_100MS.fetch_add(1, Ordering::Relaxed);
        } else {
            LATENCY_BUCKET_INF.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Renders current metrics in the Prometheus Exposition Format with latency histograms.
    pub fn render() -> String {
        let ingested = EVENTS_INGESTED.load(Ordering::Relaxed);
        let processed = EVENTS_PROCESSED.load(Ordering::Relaxed);
        let duplicates = DUPLICATES_FILTERED.load(Ordering::Relaxed);
        let latency_sum_us = PROCESSING_LATENCY_SUM_US.load(Ordering::Relaxed);
        let latency_count = PROCESSING_LATENCY_COUNT.load(Ordering::Relaxed);

        let latency_sum_sec = (latency_sum_us as f64) / 1_000_000.0;

        let b1 = LATENCY_BUCKET_1MS.load(Ordering::Relaxed);
        let b5 = b1 + LATENCY_BUCKET_5MS.load(Ordering::Relaxed);
        let b10 = b5 + LATENCY_BUCKET_10MS.load(Ordering::Relaxed);
        let b50 = b10 + LATENCY_BUCKET_50MS.load(Ordering::Relaxed);
        let b100 = b50 + LATENCY_BUCKET_100MS.load(Ordering::Relaxed);
        let b_inf = b100 + LATENCY_BUCKET_INF.load(Ordering::Relaxed);

        format!(
            "# HELP caminus_events_ingested_total Total events ingested by the sources.\n\
             # TYPE caminus_events_ingested_total counter\n\
             caminus_events_ingested_total {}\n\
             # HELP caminus_events_processed_total Total events successfully processed and dispatched.\n\
             # TYPE caminus_events_processed_total counter\n\
             caminus_events_processed_total {}\n\
             # HELP caminus_duplicates_filtered_total Total duplicate events filtered out by deduplication.\n\
             # TYPE caminus_duplicates_filtered_total counter\n\
             caminus_duplicates_filtered_total {}\n\
             # HELP caminus_event_latency_seconds Event processing latency histogram.\n\
             # TYPE caminus_event_latency_seconds histogram\n\
             caminus_event_latency_seconds_bucket{{le=\"0.001\"}} {}\n\
             caminus_event_latency_seconds_bucket{{le=\"0.005\"}} {}\n\
             caminus_event_latency_seconds_bucket{{le=\"0.01\"}} {}\n\
             caminus_event_latency_seconds_bucket{{le=\"0.05\"}} {}\n\
             caminus_event_latency_seconds_bucket{{le=\"0.1\"}} {}\n\
             caminus_event_latency_seconds_bucket{{le=\"+Inf\"}} {}\n\
             caminus_event_latency_seconds_sum {:.6}\n\
             caminus_event_latency_seconds_count {}\n",
            ingested,
            processed,
            duplicates,
            b1,
            b5,
            b10,
            b50,
            b100,
            b_inf,
            latency_sum_sec,
            latency_count
        )
    }

    /// Starts a lightweight TCP metrics server in the background.
    pub fn start_exporter(addr: &'static str) {
        tokio::spawn(async move {
            let listener = match TcpListener::bind(addr).await {
                Ok(l) => {
                    println!("[Metrics Server] Listening on http://{}", addr);
                    l
                }
                Err(e) => {
                    eprintln!("[Metrics Server] Failed to bind to {}: {:?}", addr, e);
                    return;
                }
            };

            loop {
                match listener.accept().await {
                    Ok((mut socket, _)) => {
                        tokio::spawn(async move {
                            let mut buf = [0; 1024];
                            let _ = socket.read(&mut buf).await;

                            let metrics_data = Self::render();
                            let response = format!(
                                "HTTP/1.1 200 OK\r\n\
                                 Content-Type: text/plain; version=0.0.4; charset=utf-8\r\n\
                                 Content-Length: {}\r\n\
                                 Connection: close\r\n\r\n\
                                 {}",
                                metrics_data.len(),
                                metrics_data
                            );
                            let _ = socket.write_all(response.as_bytes()).await;
                            let _ = socket.flush().await;
                        });
                    }
                    Err(_) => continue,
                }
            }
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    #[tokio::test]
    async fn test_metrics_rendering() {
        // Reset metrics
        EVENTS_INGESTED.store(0, Ordering::Relaxed);
        EVENTS_PROCESSED.store(0, Ordering::Relaxed);
        DUPLICATES_FILTERED.store(0, Ordering::Relaxed);
        PROCESSING_LATENCY_SUM_US.store(0, Ordering::Relaxed);
        PROCESSING_LATENCY_COUNT.store(0, Ordering::Relaxed);

        LATENCY_BUCKET_1MS.store(0, Ordering::Relaxed);
        LATENCY_BUCKET_5MS.store(0, Ordering::Relaxed);
        LATENCY_BUCKET_10MS.store(0, Ordering::Relaxed);
        LATENCY_BUCKET_50MS.store(0, Ordering::Relaxed);
        LATENCY_BUCKET_100MS.store(0, Ordering::Relaxed);
        LATENCY_BUCKET_INF.store(0, Ordering::Relaxed);

        MetricsRegistry::increment_ingested();
        MetricsRegistry::increment_processed();
        MetricsRegistry::increment_duplicates();
        MetricsRegistry::record_latency(500); // 0.5ms -> bucket 1ms

        let rendered = MetricsRegistry::render();
        assert!(rendered.contains("caminus_events_ingested_total 1"));
        assert!(rendered.contains("caminus_events_processed_total 1"));
        assert!(rendered.contains("caminus_duplicates_filtered_total 1"));
        assert!(rendered.contains("caminus_event_latency_seconds_bucket{le=\"0.001\"} 1"));
        assert!(rendered.contains("caminus_event_latency_seconds_count 1"));
    }

    #[tokio::test]
    async fn test_metrics_server() {
        let addr = "127.0.0.1:29101";
        MetricsRegistry::start_exporter(addr);

        // Sleep to let server bind
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;

        // Make HTTP request
        let mut stream = tokio::net::TcpStream::connect(addr).await.unwrap();
        stream
            .write_all(b"GET /metrics HTTP/1.1\r\n\r\n")
            .await
            .unwrap();
        stream.flush().await.unwrap();

        let mut resp = String::new();
        stream.read_to_string(&mut resp).await.unwrap();

        assert!(resp.contains("HTTP/1.1 200 OK"));
        assert!(resp.contains("caminus_events_ingested_total"));
    }
}

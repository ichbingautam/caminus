use super::{CdcSource, ChangeEvent, Operation};
use chrono::Utc;
use futures_util::stream::{self, BoxStream, StreamExt};
use serde_json::json;
use std::time::Duration;

pub struct CassandraSource {
    pub cdc_directory: String,
}

impl CassandraSource {
    pub fn new(cdc_directory: String) -> Self {
        Self { cdc_directory }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum CassandraSourceError {
    #[error("CDC directory access error: {0}")]
    DirectoryAccess(String),
    #[error("CommitLog parsing error: {0}")]
    CommitLogParse(String),
}

impl CdcSource for CassandraSource {
    type Error = CassandraSourceError;

    async fn start_stream(
        &self,
        start_offset: Option<String>,
    ) -> Result<BoxStream<'static, Result<ChangeEvent, Self::Error>>, Self::Error> {
        static ATTEMPTS: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        let make_conn = || async {
            let attempt = ATTEMPTS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            if attempt < 2 {
                Err(CassandraSourceError::DirectoryAccess(
                    "Simulated Cassandra CommitLog folder lock error".to_string(),
                ))
            } else {
                Ok(())
            }
        };

        super::retry_with_backoff(
            make_conn,
            5,
            Duration::from_millis(50),
            2.0,
            Duration::from_millis(500),
        )
        .await?;

        // Bootstrap mock stream generator simulating Cassandra CommitLog parsing
        let start_pos = start_offset
            .and_then(|o| o.parse::<u64>().ok())
            .unwrap_or(0);

        let keyspace = "caminus_keyspace".to_string();

        let stream = stream::unfold(start_pos, move |pos| {
            let ks = keyspace.clone();
            async move {
                // Throttled event simulation representing Cassandra CDC record capture
                tokio::time::sleep(Duration::from_millis(600)).await;

                let next_pos = pos + 1;
                let event = ChangeEvent {
                    id: format!("cas-evt-{}", next_pos),
                    source_database: ks,
                    source_table_or_collection: "sensor_readings".to_string(),
                    operation: Operation::Create,
                    timestamp: Utc::now(),
                    key: json!({ "sensor_id": format!("sensor-{}", next_pos % 10), "timestamp": Utc::now().to_rfc3339() }),
                    before: None,
                    after: Some(json!({
                        "sensor_id": format!("sensor-{}", next_pos % 10),
                        "reading_value": 20.0 + (next_pos as f64) * 0.1,
                        "status": "OK"
                    })),
                    transaction_id: None,
                    offset: next_pos.to_string(),
                };

                Some((Ok(event), next_pos))
            }
        });

        Ok(stream.boxed())
    }
}

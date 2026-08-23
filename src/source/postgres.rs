use super::{CdcSource, ChangeEvent, Operation};
use chrono::Utc;
use futures_util::stream::{self, BoxStream, StreamExt};
use serde_json::json;
use std::time::Duration;

pub struct PostgresSource {
    pub connection_string: String,
    pub slot_name: String,
    pub publication: String,
}

impl PostgresSource {
    pub fn new(connection_string: String, slot_name: String, publication: String) -> Self {
        Self {
            connection_string,
            slot_name,
            publication,
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum PostgresSourceError {
    #[error("Postgres connection failed: {0}")]
    Connection(String),
    #[error("Replication slot error: {0}")]
    Slot(String),
    #[error("Log parsing error: {0}")]
    Parse(String),
}

impl CdcSource for PostgresSource {
    type Error = PostgresSourceError;

    async fn start_stream(
        &self,
        start_offset: Option<String>,
    ) -> Result<BoxStream<'static, Result<ChangeEvent, Self::Error>>, Self::Error> {
        static ATTEMPTS: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        let make_conn = || async {
            let attempt = ATTEMPTS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            if attempt < 2 {
                Err(PostgresSourceError::Connection(
                    "Simulated Postgres database outage".to_string(),
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

        let start_seq = start_offset
            .and_then(|o| o.parse::<u64>().ok())
            .unwrap_or(0);

        let source_db = "caminus_db".to_string();

        let stream = stream::unfold(start_seq, move |seq| {
            let db = source_db.clone();
            async move {
                tokio::time::sleep(Duration::from_millis(500)).await;

                let next_seq = seq + 1;

                // Determine transaction cycle: 3 mutations followed by 1 commit
                let tx_cycle = (next_seq - 1) / 4 + 1000;
                let is_commit = next_seq % 4 == 0;

                let op = if is_commit {
                    Operation::Commit
                } else if next_seq % 4 == 1 {
                    Operation::Create
                } else {
                    Operation::Update
                };

                let event = ChangeEvent {
                    id: format!("pg-evt-{}", next_seq),
                    source_database: db,
                    source_table_or_collection: "users".to_string(),
                    operation: op,
                    timestamp: Utc::now(),
                    key: json!({ "id": next_seq }),
                    before: None,
                    after: if is_commit {
                        None
                    } else {
                        Some(json!({
                            "id": next_seq,
                            "name": format!("User {}", next_seq),
                            "email": format!("user{}@caminus.io", next_seq)
                        }))
                    },
                    transaction_id: Some(format!("tx-{}", tx_cycle)),
                    offset: next_seq.to_string(),
                };

                Some((Ok(event), next_seq))
            }
        });

        Ok(stream.boxed())
    }
}

use super::{CdcSource, ChangeEvent, Operation};
use futures_util::stream::{self, BoxStream, StreamExt};
use std::time::Duration;
use chrono::Utc;
use serde_json::json;

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
        // Bootstrap mock stream generator simulating PostgreSQL logical replication frames (pgoutput)
        let start_seq = start_offset
            .and_then(|o| o.parse::<u64>().ok())
            .unwrap_or(0);
            
        let source_db = "caminus_db".to_string();
        
        let stream = stream::unfold(start_seq, move |seq| {
            let db = source_db.clone();
            async move {
                // Throttled event simulation
                tokio::time::sleep(Duration::from_millis(500)).await;
                
                let next_seq = seq + 1;
                let event = ChangeEvent {
                    id: format!("pg-evt-{}", next_seq),
                    source_database: db,
                    source_table_or_collection: "users".to_string(),
                    operation: Operation::Create,
                    timestamp: Utc::now(),
                    key: json!({ "id": next_seq }),
                    before: None,
                    after: Some(json!({ "id": next_seq, "name": format!("User {}", next_seq), "email": format!("user{}@caminus.io", next_seq) })),
                    transaction_id: Some(format!("tx-{}", 1000 + next_seq / 5)),
                    offset: next_seq.to_string(),
                };
                
                Some((Ok(event), next_seq))
            }
        });
        
        Ok(stream.boxed())
    }
}

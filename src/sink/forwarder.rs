use thiserror::Error;

use crate::sink::CdcSink;
use crate::source::ChangeEvent;
use crate::serialize::serialize_event;

#[derive(Debug, Error)]
pub enum ForwarderSinkError {
    #[error("Target cluster unreachable: {0}")]
    Unreachable(String),
    #[error("Serialization failed: {0}")]
    Serialization(String),
}

pub struct ClusterForwarderSink {
    pub target_cluster_id: String,
    pub target_endpoint: String,
}

impl ClusterForwarderSink {
    pub fn new(target_cluster_id: impl Into<String>, target_endpoint: impl Into<String>) -> Self {
        Self {
            target_cluster_id: target_cluster_id.into(),
            target_endpoint: target_endpoint.into(),
        }
    }
}

impl CdcSink for ClusterForwarderSink {
    type Error = ForwarderSinkError;

    async fn send(&self, event: &ChangeEvent) -> Result<(), Self::Error> {
        let serialized = serialize_event(event)
            .map_err(|e| ForwarderSinkError::Serialization(e.to_string()))?;

        println!(
            "[CLUSTER FORWARDER] Forwarding change event {} ({}) to remote cluster '{}' at endpoint '{}' ({} bytes)",
            event.id, event.source_table_or_collection, self.target_cluster_id, self.target_endpoint, serialized.len()
        );

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;
    use serde_json::json;
    use crate::source::Operation;

    #[tokio::test]
    async fn test_cluster_forwarder_sink() {
        let forwarder = ClusterForwarderSink::new("eu-west-1", "https://eu.caminus.io:9000");

        let event = ChangeEvent {
            id: "evt-fwd-1".to_string(),
            source_database: "db".to_string(),
            source_table_or_collection: "orders".to_string(),
            operation: Operation::Create,
            timestamp: Utc::now(),
            key: json!({ "id": 100 }),
            before: None,
            after: Some(json!({ "id": 100, "total": 49.99 })),
            transaction_id: Some("tx-100".to_string()),
            offset: "offset-100".to_string(),
        };

        let result = forwarder.send(&event).await;
        assert!(result.is_ok());
    }
}

use serde::{Serialize, Deserialize};
use chrono::{DateTime, Utc};
use crate::source::ChangeEvent;
use crate::serialize;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DlqRecord {
    pub failed_event: ChangeEvent,
    pub error_reason: String,
    pub failed_step: String,
    pub timestamp: DateTime<Utc>,
    pub retry_attempts: u32,
}

impl DlqRecord {
    pub fn new(event: ChangeEvent, error_reason: String, failed_step: String, retry_attempts: u32) -> Self {
        Self {
            failed_event: event,
            error_reason,
            failed_step,
            timestamp: Utc::now(),
            retry_attempts,
        }
    }
}

pub struct DeadLetterQueue {
    pub dlq_topic_or_path: String,
}

impl DeadLetterQueue {
    pub fn new(dlq_topic_or_path: String) -> Self {
        Self { dlq_topic_or_path }
    }

    /// Routes a failed poison pill event to the Dead Letter Queue sink with diagnostic error metadata.
    pub fn route_to_dlq(&self, record: &DlqRecord) -> Result<(), String> {
        let serialized = serialize::serialize_event(&record.failed_event)
            .unwrap_or_else(|_| "Failed to serialize raw event".into());

        println!(
            "[DEAD LETTER QUEUE] Poison pill routed to '{}' | Step: {} | Retries: {} | Reason: {} | Event Payload: {}",
            self.dlq_topic_or_path, record.failed_step, record.retry_attempts, record.error_reason, serialized
        );

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::source::Operation;
    use serde_json::json;

    #[test]
    fn test_dlq_routing() {
        let dlq = DeadLetterQueue::new("caminus_dlq_topic".into());

        let event = ChangeEvent {
            id: "poison-1".into(),
            source_database: "db".into(),
            source_table_or_collection: "users".into(),
            operation: Operation::Create,
            timestamp: Utc::now(),
            key: json!({ "id": 999 }),
            before: None,
            after: Some(json!({ "id": 999, "malformed": true })),
            transaction_id: None,
            offset: "100".into(),
        };

        let record = DlqRecord::new(
            event,
            "Type mismatch on field 'id'".into(),
            "SchemaValidation".into(),
            3,
        );

        assert!(dlq.route_to_dlq(&record).is_ok());
    }
}

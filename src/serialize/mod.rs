use crate::source::ChangeEvent;
use thiserror::Error;

pub mod avro;

#[derive(Error, Debug)]
pub enum SerializationError {
    #[error("simd-json serialization error: {0}")]
    Simd(#[from] simd_json::Error),
}

/// Serializes a ChangeEvent utilizing SIMD-accelerated simd-json engine.
pub fn serialize_event(event: &ChangeEvent) -> Result<String, SerializationError> {
    let json_str = simd_json::to_string(event)?;
    Ok(json_str)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::source::Operation;
    use chrono::Utc;

    #[test]
    fn test_simd_serialization() {
        let event = ChangeEvent {
            id: "evt-simd".into(),
            source_database: "db".into(),
            source_table_or_collection: "users".into(),
            operation: Operation::Create,
            timestamp: Utc::now(),
            key: serde_json::json!({ "id": 1 }),
            before: None,
            after: Some(serde_json::json!({ "id": 1, "name": "Simd" })),
            transaction_id: None,
            offset: "100".into(),
        };

        let serialized = serialize_event(&event).expect("Failed to serialize event");
        assert!(serialized.contains("evt-simd"));
        assert!(serialized.contains("users"));
    }
}

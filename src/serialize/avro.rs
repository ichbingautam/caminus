use crate::source::ChangeEvent;
use crate::storage::confluent::ConfluentRegistryClient;

pub struct AvroEncoder {
    registry_client: ConfluentRegistryClient,
}

impl AvroEncoder {
    pub fn new(registry_url: impl Into<String>) -> Self {
        Self {
            registry_client: ConfluentRegistryClient::new(registry_url),
        }
    }

    pub fn encode_event(&self, event: &ChangeEvent) -> Result<Vec<u8>, String> {
        let subject = format!("{}-value", event.source_table_or_collection);
        let schema_json = r#"{"type":"record","name":"ChangeEvent"}"#;

        let schema_id = self.registry_client.register_schema(&subject, schema_json);

        // Serialize event JSON payload to binary bytes
        let raw_payload =
            serde_json::to_vec(event).map_err(|e| format!("Serialization error: {}", e))?;

        // Format with 5-byte Confluent wire header (0x00 + 4-byte schema_id)
        Ok(self
            .registry_client
            .encode_wire_format(schema_id, &raw_payload))
    }

    pub fn decode_event<'a>(&self, buffer: &'a [u8]) -> Result<(u32, ChangeEvent), String> {
        let (schema_id, payload_bytes) = self.registry_client.decode_wire_format(buffer)?;
        let event: ChangeEvent = serde_json::from_slice(payload_bytes)
            .map_err(|e| format!("Deserialization error: {}", e))?;

        Ok((schema_id, event))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::source::Operation;
    use chrono::Utc;
    use serde_json::json;

    #[test]
    fn test_avro_encoder_confluent_header() {
        let encoder = AvroEncoder::new("http://localhost:8081");

        let event = ChangeEvent {
            id: "evt-avro-1".to_string(),
            source_database: "db".to_string(),
            source_table_or_collection: "users".to_string(),
            operation: Operation::Create,
            timestamp: Utc::now(),
            key: json!({ "id": 1 }),
            before: None,
            after: Some(json!({ "id": 1, "name": "Bob" })),
            transaction_id: Some("tx-1".to_string()),
            offset: "offset-1".to_string(),
        };

        let encoded = encoder.encode_event(&event).unwrap();
        assert_eq!(encoded[0], 0x00);

        let (schema_id, decoded) = encoder.decode_event(&encoded).unwrap();
        assert_eq!(schema_id, 100);
        assert_eq!(decoded.id, "evt-avro-1");
    }
}

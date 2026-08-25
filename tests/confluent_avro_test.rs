use chrono::Utc;
use serde_json::json;

use caminus::serialize::avro::AvroEncoder;
use caminus::source::{ChangeEvent, Operation};
use caminus::storage::confluent::ConfluentRegistryClient;

#[test]
fn test_confluent_schema_registry_wire_format_integration() {
    let registry_url = "http://localhost:8081";
    let client = ConfluentRegistryClient::new(registry_url);
    let encoder = AvroEncoder::new(registry_url);

    let event = ChangeEvent {
        id: "evt-confluent-99".to_string(),
        source_database: "production_db".to_string(),
        source_table_or_collection: "orders".to_string(),
        operation: Operation::Create,
        timestamp: Utc::now(),
        key: json!({ "order_id": 999 }),
        before: None,
        after: Some(json!({ "order_id": 999, "amount": 199.95 })),
        transaction_id: Some("tx-999".to_string()),
        offset: "offset-999".to_string(),
    };

    // 1. Encode with Avro/Confluent encoder
    let wire_bytes = encoder.encode_event(&event).unwrap();

    // 2. Validate Confluent 5-byte header specification
    assert!(wire_bytes.len() > 5);
    assert_eq!(wire_bytes[0], 0x00); // Confluent Magic Byte

    // 3. Decode header and verify Schema ID
    let (schema_id, payload) = client.decode_wire_format(&wire_bytes).unwrap();
    assert_eq!(schema_id, 100);
    assert!(!payload.is_empty());

    // 4. Decode event payload
    let (decoded_id, decoded_event) = encoder.decode_event(&wire_bytes).unwrap();
    assert_eq!(decoded_id, 100);
    assert_eq!(decoded_event.id, "evt-confluent-99");
    assert_eq!(decoded_event.source_table_or_collection, "orders");
}

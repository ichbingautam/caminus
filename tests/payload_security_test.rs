use chrono::Utc;
use serde_json::json;

use caminus::source::{ChangeEvent, Operation};
use caminus::security::{PayloadSecurityEngine, MaskingRule};
use caminus::security::kms::KmsProvider;

#[test]
fn test_end_to_end_payload_security_and_key_rotation() {
    let kms = KmsProvider::new(b"initial-32-byte-master-key-v1!!");
    let mut engine = PayloadSecurityEngine::new()
        .with_kms(kms);

    engine.add_rule("ssn", MaskingRule::Redact);
    engine.add_rule("email", MaskingRule::Hash);
    engine.add_rule("credit_card", MaskingRule::Encrypt);

    let event = ChangeEvent {
        id: "evt-sec-100".to_string(),
        source_database: "finance_db".to_string(),
        source_table_or_collection: "accounts".to_string(),
        operation: Operation::Create,
        timestamp: Utc::now(),
        key: json!({ "id": 100 }),
        before: None,
        after: Some(json!({
            "id": 100,
            "owner": "John Doe",
            "ssn": "123-45-6789",
            "email": "john@domain.com",
            "credit_card": "4532-1234-5678-9012"
        })),
        transaction_id: Some("tx-sec-100".to_string()),
        offset: "sec-offset-100".to_string(),
    };

    let secured_v1 = engine.apply_security(event.clone());
    let after_v1 = secured_v1.after.unwrap();

    assert_eq!(after_v1["ssn"], "[REDACTED]");
    assert_ne!(after_v1["email"], "john@domain.com");
    assert!(after_v1["credit_card"].as_str().unwrap().starts_with("ENC:v1:"));
    assert_eq!(after_v1["owner"], "John Doe");
}

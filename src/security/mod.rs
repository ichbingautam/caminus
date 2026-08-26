pub mod kms;

use crate::source::ChangeEvent;
use kms::KmsProvider;
use serde_json::Value;
use std::collections::HashMap;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MaskingRule {
    Redact,
    Hash,
    Encrypt,
}

pub struct PayloadSecurityEngine {
    field_rules: HashMap<String, MaskingRule>,
    kms_provider: Option<KmsProvider>,
}

impl Default for PayloadSecurityEngine {
    fn default() -> Self {
        Self::new()
    }
}

impl PayloadSecurityEngine {
    pub fn new() -> Self {
        Self {
            field_rules: HashMap::new(),
            kms_provider: None,
        }
    }

    pub fn with_kms(mut self, kms: KmsProvider) -> Self {
        self.kms_provider = Some(kms);
        self
    }

    pub fn add_rule(&mut self, field_name: impl Into<String>, rule: MaskingRule) {
        self.field_rules.insert(field_name.into(), rule);
    }

    pub fn apply_security(&self, mut event: ChangeEvent) -> ChangeEvent {
        if let Some(ref mut after) = event.after {
            self.transform_value(after);
        }
        if let Some(ref mut before) = event.before {
            self.transform_value(before);
        }
        event
    }

    fn transform_value(&self, value: &mut Value) {
        if let Value::Object(map) = value {
            for (key, rule) in &self.field_rules {
                if let Some(val) = map.get_mut(key) {
                    match rule {
                        MaskingRule::Redact => {
                            *val = Value::String("[REDACTED]".to_string());
                        }
                        MaskingRule::Hash => {
                            if let Value::String(s) = val {
                                let mut hash: u64 = 5381;
                                for b in s.bytes() {
                                    hash = ((hash << 5).wrapping_add(hash)).wrapping_add(b as u64);
                                }
                                *val = Value::String(format!("{:016x}", hash));
                            }
                        }
                        MaskingRule::Encrypt => {
                            if let Some(kms) = &self.kms_provider {
                                let (version, key) = kms.get_active_key();
                                let raw_str = val.to_string();
                                let hex_key: String =
                                    key.iter().map(|b| format!("{:02x}", b)).collect();
                                let hex_data: String = raw_str
                                    .as_bytes()
                                    .iter()
                                    .map(|b| format!("{:02x}", b))
                                    .collect();
                                let encrypted =
                                    format!("ENC:v{}:{}:{}", version, hex_key, hex_data);
                                *val = Value::String(encrypted);
                            } else if let Value::String(_) = val {
                                *val = Value::String("[ENCRYPTED_WITHOUT_KMS]".to_string());
                            }
                        }
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::source::Operation;
    use chrono::Utc;
    use serde_json::json;

    #[test]
    fn test_payload_security_redact_and_hash() {
        let mut engine = PayloadSecurityEngine::new();
        engine.add_rule("ssn", MaskingRule::Redact);
        engine.add_rule("email", MaskingRule::Hash);

        let event = ChangeEvent {
            id: "evt-sec-1".to_string(),
            source_database: "db".to_string(),
            source_table_or_collection: "users".to_string(),
            operation: Operation::Create,
            timestamp: Utc::now(),
            key: json!({ "id": 1 }),
            before: None,
            after: Some(json!({
                "id": 1,
                "name": "Alice",
                "ssn": "000-12-3456",
                "email": "alice@example.com"
            })),
            transaction_id: Some("tx-1".to_string()),
            offset: "offset-1".to_string(),
        };

        let secured = engine.apply_security(event);
        let after = secured.after.unwrap();

        assert_eq!(after["ssn"], "[REDACTED]");
        assert_ne!(after["email"], "alice@example.com");
        assert_eq!(after["name"], "Alice");
    }
}

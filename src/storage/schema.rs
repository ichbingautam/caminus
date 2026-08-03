use serde::{Serialize, Deserialize};
use serde_json::Value;
use thiserror::Error;
use crate::source::ChangeEvent;
use crate::storage::{StateStore, StorageError};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum SchemaCompatibility {
    None,
    Backward,
    Forward,
    Full,
}

#[derive(Error, Debug)]
pub enum SchemaError {
    #[error("Storage error: {0}")]
    Storage(#[from] StorageError),
    #[error("Incompatible schema change: {0}")]
    Incompatibility(String),
    #[error("Validation failed: {0}")]
    Validation(String),
}

pub struct SchemaRegistry;

impl SchemaRegistry {
    /// Registers a new schema for a source. Validates compatibility with the existing schema if one exists.
    pub fn register_schema(
        store: &StateStore,
        source_id: &str,
        new_schema: Value,
        mode: SchemaCompatibility,
    ) -> Result<(), SchemaError> {
        if let Some(old_schema) = store.get_schema(source_id)? {
            Self::check_compatibility(&old_schema, &new_schema, mode)?;
        }
        store.save_schema(source_id, &new_schema)?;
        Ok(())
    }

    /// Evaluates if new_schema is compatible with old_schema based on the compatibility mode.
    pub fn check_compatibility(
        old_schema: &Value,
        new_schema: &Value,
        mode: SchemaCompatibility,
    ) -> Result<(), SchemaError> {
        if mode == SchemaCompatibility::None {
            return Ok(());
        }

        let old_fields = old_schema.get("fields")
            .and_then(|f| f.as_object())
            .ok_or_else(|| SchemaError::Incompatibility("Old schema missing 'fields' object".to_string()))?;

        let new_fields = new_schema.get("fields")
            .and_then(|f| f.as_object())
            .ok_or_else(|| SchemaError::Incompatibility("New schema missing 'fields' object".to_string()))?;

        // 1. Check for type changes in common fields
        for (field_name, old_type_val) in old_fields {
            if let Some(new_type_val) = new_fields.get(field_name) {
                if old_type_val != new_type_val {
                    return Err(SchemaError::Incompatibility(format!(
                        "Field '{}' type changed from {:?} to {:?}",
                        field_name, old_type_val, new_type_val
                    )));
                }
            }
        }

        match mode {
            SchemaCompatibility::Backward => {
                // Backward: New schema must be able to read old data.
                // We cannot delete fields because old data expects them.
                for field_name in old_fields.keys() {
                    if !new_fields.contains_key(field_name) {
                        return Err(SchemaError::Incompatibility(format!(
                            "Backward compatibility violation: Field '{}' was deleted",
                            field_name
                        )));
                    }
                }
            }
            SchemaCompatibility::Forward => {
                // Forward: Old schema must be able to read new data.
                // We cannot add new fields because old schema doesn't know about them.
                for field_name in new_fields.keys() {
                    if !old_fields.contains_key(field_name) {
                        return Err(SchemaError::Incompatibility(format!(
                            "Forward compatibility violation: New field '{}' added",
                            field_name
                        )));
                    }
                }
            }
            SchemaCompatibility::Full => {
                // Full: Both backward and forward compatible. No fields can be added or deleted.
                for field_name in old_fields.keys() {
                    if !new_fields.contains_key(field_name) {
                        return Err(SchemaError::Incompatibility(format!(
                            "Full compatibility violation: Field '{}' was deleted",
                            field_name
                        )));
                    }
                }
                for field_name in new_fields.keys() {
                    if !old_fields.contains_key(field_name) {
                        return Err(SchemaError::Incompatibility(format!(
                            "Full compatibility violation: Field '{}' was added",
                            field_name
                        )));
                    }
                }
            }
            SchemaCompatibility::None => {}
        }

        Ok(())
    }

    /// Validates a change event payload against its registered schema.
    pub fn validate_event(
        store: &StateStore,
        source_id: &str,
        event: &ChangeEvent,
    ) -> Result<(), SchemaError> {
        let schema = match store.get_schema(source_id)? {
            Some(s) => s,
            None => return Ok(()), // No schema registered, skip validation
        };

        let fields = schema.get("fields")
            .and_then(|f| f.as_object())
            .ok_or_else(|| SchemaError::Validation("Registered schema missing 'fields' object".to_string()))?;

        // Only validate payloads for Create and Update mutations
        if event.operation != crate::source::Operation::Create && event.operation != crate::source::Operation::Update {
            return Ok(());
        }

        if let Some(payload) = &event.after {
            let payload_obj = payload.as_object()
                .ok_or_else(|| SchemaError::Validation("Event payload is not a JSON object".to_string()))?;

            for (field_name, type_val) in fields {
                let expected_type = type_val.as_str()
                    .ok_or_else(|| SchemaError::Validation(format!("Schema type for '{}' is not a string", field_name)))?;

                let val = payload_obj.get(field_name)
                    .ok_or_else(|| SchemaError::Validation(format!("Missing required field '{}'", field_name)))?;

                match expected_type {
                    "integer" => {
                        if !val.is_number() || val.as_f64().unwrap().fract() != 0.0 {
                            return Err(SchemaError::Validation(format!(
                                "Field '{}' expected integer, found {:?}",
                                field_name, val
                            )));
                        }
                    }
                    "string" => {
                        if !val.is_string() {
                            return Err(SchemaError::Validation(format!(
                                "Field '{}' expected string, found {:?}",
                                field_name, val
                            )));
                        }
                    }
                    "boolean" => {
                        if !val.is_boolean() {
                            return Err(SchemaError::Validation(format!(
                                "Field '{}' expected boolean, found {:?}",
                                field_name, val
                            )));
                        }
                    }
                    "float" => {
                        if !val.is_number() {
                            return Err(SchemaError::Validation(format!(
                                "Field '{}' expected float, found {:?}",
                                field_name, val
                            )));
                        }
                    }
                    other => {
                        return Err(SchemaError::Validation(format!(
                            "Unsupported schema type: {}",
                            other
                        )));
                    }
                }
            }
            Ok(())
        } else {
            Err(SchemaError::Validation("Event payload 'after' is missing".to_string()))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use serde_json::json;
    use chrono::Utc;

    #[test]
    fn test_schema_registration_and_validation() {
        let test_path = "./data/test_schema_db";
        let _ = fs::remove_dir_all(test_path);
        let store = StateStore::new(test_path).unwrap();

        let schema = json!({
            "fields": {
                "id": "integer",
                "name": "string",
                "active": "boolean"
            }
        });

        // Register initial schema
        SchemaRegistry::register_schema(&store, "pg_users", schema.clone(), SchemaCompatibility::None).unwrap();

        // Valid event
        let event = ChangeEvent {
            id: "evt-1".into(),
            source_database: "db".into(),
            source_table_or_collection: "users".into(),
            operation: crate::source::Operation::Create,
            timestamp: Utc::now(),
            key: json!({ "id": 1 }),
            before: None,
            after: Some(json!({ "id": 1, "name": "John", "active": true })),
            transaction_id: None,
            offset: "1".into(),
        };
        assert!(SchemaRegistry::validate_event(&store, "pg_users", &event).is_ok());

        // Invalid event - missing active field
        let invalid_event_1 = ChangeEvent {
            after: Some(json!({ "id": 1, "name": "John" })),
            ..event.clone()
        };
        assert!(SchemaRegistry::validate_event(&store, "pg_users", &invalid_event_1).is_err());

        // Invalid event - type mismatch
        let invalid_event_2 = ChangeEvent {
            after: Some(json!({ "id": "not-an-int", "name": "John", "active": true })),
            ..event.clone()
        };
        assert!(SchemaRegistry::validate_event(&store, "pg_users", &invalid_event_2).is_err());

        let _ = fs::remove_dir_all(test_path);
    }

    #[test]
    fn test_schema_compatibility() {
        let old_schema = json!({
            "fields": {
                "id": "integer",
                "name": "string"
            }
        });

        // 1. Backward compatible: Adding a field is allowed
        let new_schema_add = json!({
            "fields": {
                "id": "integer",
                "name": "string",
                "email": "string"
            }
        });
        assert!(SchemaRegistry::check_compatibility(&old_schema, &new_schema_add, SchemaCompatibility::Backward).is_ok());

        // Backward incompatible: Deleting a field is not allowed
        let new_schema_del = json!({
            "fields": {
                "id": "integer"
            }
        });
        assert!(SchemaRegistry::check_compatibility(&old_schema, &new_schema_del, SchemaCompatibility::Backward).is_err());

        // 2. Forward compatible: Deleting a field is allowed
        assert!(SchemaRegistry::check_compatibility(&old_schema, &new_schema_del, SchemaCompatibility::Forward).is_ok());

        // Forward incompatible: Adding a field is not allowed
        assert!(SchemaRegistry::check_compatibility(&old_schema, &new_schema_add, SchemaCompatibility::Forward).is_err());

        // 3. Full compatibility
        assert!(SchemaRegistry::check_compatibility(&old_schema, &old_schema, SchemaCompatibility::Full).is_ok());
        assert!(SchemaRegistry::check_compatibility(&old_schema, &new_schema_add, SchemaCompatibility::Full).is_err());
    }
}

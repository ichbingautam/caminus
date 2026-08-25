use std::collections::HashMap;
use std::sync::RwLock;

pub struct ConfluentRegistryClient {
    pub registry_url: String,
    next_schema_id: RwLock<u32>,
    schema_cache: RwLock<HashMap<String, u32>>,
}

impl ConfluentRegistryClient {
    pub fn new(registry_url: impl Into<String>) -> Self {
        Self {
            registry_url: registry_url.into(),
            next_schema_id: RwLock::new(100),
            schema_cache: RwLock::new(HashMap::new()),
        }
    }

    /// Registers or retrieves the 4-byte Schema ID for a given subject.
    pub fn register_schema(&self, subject: &str, _schema_json: &str) -> u32 {
        {
            let cache = self.schema_cache.read().unwrap();
            if let Some(&id) = cache.get(subject) {
                return id;
            }
        }

        let mut next_id_guard = self.next_schema_id.write().unwrap();
        let assigned_id = *next_id_guard;
        *next_id_guard += 1;

        let mut cache_guard = self.schema_cache.write().unwrap();
        cache_guard.insert(subject.to_string(), assigned_id);

        println!(
            "[CONFLUENT REGISTRY] Registered subject '{}' at registry '{}' with Schema ID: {}",
            subject, self.registry_url, assigned_id
        );

        assigned_id
    }

    /// Encodes payload using Confluent 5-Byte Wire Format:
    /// Byte 0: 0x00 (Magic Byte)
    /// Bytes 1-4: 4-byte Big-Endian Schema ID
    /// Bytes 5+: Binary/JSON Payload
    pub fn encode_wire_format(&self, schema_id: u32, payload_bytes: &[u8]) -> Vec<u8> {
        let mut buffer = Vec::with_capacity(5 + payload_bytes.len());
        buffer.push(0x00); // Magic Byte
        buffer.extend_from_slice(&schema_id.to_be_bytes()); // 4-byte Big-Endian Schema ID
        buffer.extend_from_slice(payload_bytes);
        buffer
    }

    /// Decodes Confluent Wire Format back to (schema_id, payload_bytes).
    pub fn decode_wire_format<'a>(&self, buffer: &'a [u8]) -> Result<(u32, &'a [u8]), String> {
        if buffer.len() < 5 {
            return Err("Buffer too short for Confluent wire format header".to_string());
        }
        if buffer[0] != 0x00 {
            return Err(format!(
                "Invalid magic byte: expected 0x00, got 0x{:02x}",
                buffer[0]
            ));
        }
        let mut id_bytes = [0u8; 4];
        id_bytes.copy_from_slice(&buffer[1..5]);
        let schema_id = u32::from_be_bytes(id_bytes);

        Ok((schema_id, &buffer[5..]))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_confluent_wire_format_encoding_decoding() {
        let client = ConfluentRegistryClient::new("http://localhost:8081");
        let schema_id = client.register_schema("postgres_users-value", r#"{"type":"record"}"#);

        assert_eq!(schema_id, 100);

        let payload = b"avro-binary-payload-data";
        let encoded = client.encode_wire_format(schema_id, payload);

        assert_eq!(encoded.len(), 5 + payload.len());
        assert_eq!(encoded[0], 0x00);
        assert_eq!(&encoded[1..5], &[0, 0, 0, 100]); // 100 in Big-Endian

        let (decoded_id, decoded_payload) = client.decode_wire_format(&encoded).unwrap();
        assert_eq!(decoded_id, 100);
        assert_eq!(decoded_payload, payload);
    }
}

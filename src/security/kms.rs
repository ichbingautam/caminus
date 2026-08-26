use std::collections::HashMap;
use std::sync::RwLock;

pub struct KmsProvider {
    current_version: RwLock<u32>,
    keys: RwLock<HashMap<u32, Vec<u8>>>,
}

impl KmsProvider {
    pub fn new(initial_key: &[u8]) -> Self {
        let mut keys = HashMap::new();
        keys.insert(1, initial_key.to_vec());
        Self {
            current_version: RwLock::new(1),
            keys: RwLock::new(keys),
        }
    }

    pub fn get_active_key(&self) -> (u32, Vec<u8>) {
        let version = *self.current_version.read().unwrap();
        let keys = self.keys.read().unwrap();
        let key = keys.get(&version).cloned().unwrap_or_default();
        (version, key)
    }

    pub fn get_key_by_version(&self, version: u32) -> Option<Vec<u8>> {
        let keys = self.keys.read().unwrap();
        keys.get(&version).cloned()
    }

    pub fn rotate_key(&self, new_key: &[u8]) -> u32 {
        let mut version_guard = self.current_version.write().unwrap();
        let next_version = *version_guard + 1;
        let mut keys_guard = self.keys.write().unwrap();

        keys_guard.insert(next_version, new_key.to_vec());
        *version_guard = next_version;

        println!(
            "[KMS PROVIDER] Key rotation completed. Promoted key version to v{}",
            next_version
        );

        next_version
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_kms_key_rotation() {
        let kms = KmsProvider::new(b"secret-key-v1-32-bytes-long-key!");

        let (ver1, key1) = kms.get_active_key();
        assert_eq!(ver1, 1);
        assert_eq!(key1, b"secret-key-v1-32-bytes-long-key!");

        let ver2 = kms.rotate_key(b"secret-key-v2-32-bytes-long-key!");
        assert_eq!(ver2, 2);

        let (ver_act, key_act) = kms.get_active_key();
        assert_eq!(ver_act, 2);
        assert_eq!(key_act, b"secret-key-v2-32-bytes-long-key!");

        assert_eq!(
            kms.get_key_by_version(1),
            Some(b"secret-key-v1-32-bytes-long-key!".to_vec())
        );
    }
}

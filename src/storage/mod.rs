use std::path::Path;
use rocksdb::{DB, Options};
use thiserror::Error;

#[derive(Error, Debug)]
pub enum StorageError {
    #[error("RocksDB error: {0}")]
    Db(#[from] rocksdb::Error),
    #[error("Serialization error: {0}")]
    Serialization(#[from] serde_json::Error),
    #[error("Utf8 conversion error: {0}")]
    Utf8(#[from] std::string::FromUtf8Error),
}

pub struct StateStore {
    db: DB,
}

impl StateStore {
    pub fn new<P: AsRef<Path>>(path: P) -> Result<Self, StorageError> {
        let mut opts = Options::default();
        opts.create_if_missing(true);
        let db = DB::open(&opts, path)?;
        Ok(Self { db })
    }

    pub fn save_offset(&self, source_id: &str, offset: &str) -> Result<(), StorageError> {
        let key = format!("offset:{}", source_id);
        self.db.put(key.as_bytes(), offset.as_bytes())?;
        Ok(())
    }

    pub fn get_offset(&self, source_id: &str) -> Result<Option<String>, StorageError> {
        let key = format!("offset:{}", source_id);
        match self.db.get(key.as_bytes())? {
            Some(val) => {
                let offset_str = String::from_utf8(val)?;
                Ok(Some(offset_str))
            }
            None => Ok(None),
        }
    }

    pub fn save_schema(&self, source_id: &str, schema: &serde_json::Value) -> Result<(), StorageError> {
        let key = format!("schema:{}", source_id);
        let serialized = serde_json::to_vec(schema)?;
        self.db.put(key.as_bytes(), serialized)?;
        Ok(())
    }

    pub fn get_schema(&self, source_id: &str) -> Result<Option<serde_json::Value>, StorageError> {
        let key = format!("schema:{}", source_id);
        match self.db.get(key.as_bytes())? {
            Some(val) => {
                let schema = serde_json::from_slice(&val)?;
                Ok(Some(schema))
            }
            None => Ok(None),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    #[test]
    fn test_offset_and_schema_storage() {
        let test_path = "./data/test_db_state";
        // Clean up any old test db
        let _ = fs::remove_dir_all(test_path);

        let store = StateStore::new(test_path).expect("Failed to create StateStore");

        // Test offset
        store.save_offset("pg_users", "offset-99").unwrap();
        assert_eq!(store.get_offset("pg_users").unwrap(), Some("offset-99".to_string()));
        assert_eq!(store.get_offset("unknown").unwrap(), None);

        // Test schema
        let schema = serde_json::json!({
            "table": "users",
            "columns": {
                "id": "INT",
                "name": "VARCHAR"
            }
        });
        store.save_schema("pg_users", &schema).unwrap();
        assert_eq!(store.get_schema("pg_users").unwrap(), Some(schema));

        // Clean up
        let _ = fs::remove_dir_all(test_path);
    }
}

use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use serde::{Serialize, Deserialize};
use crate::source::ChangeEvent;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum PartitionStrategy {
    KeyHash,
    TenantPrefix,
    RoundRobin,
}

pub struct PartitionRouter {
    pub num_partitions: u32,
    pub strategy: PartitionStrategy,
}

impl PartitionRouter {
    pub fn new(num_partitions: u32, strategy: PartitionStrategy) -> Self {
        Self {
            num_partitions,
            strategy,
        }
    }

    /// Determines the target partition index (0..num_partitions-1) for a ChangeEvent.
    pub fn resolve_partition(&self, event: &ChangeEvent) -> u32 {
        if self.num_partitions <= 1 {
            return 0;
        }

        match self.strategy {
            PartitionStrategy::KeyHash => {
                let key_str = event.key.to_string();
                let mut hasher = DefaultHasher::new();
                key_str.hash(&mut hasher);
                let hash_val = hasher.finish();
                (hash_val % (self.num_partitions as u64)) as u32
            }
            PartitionStrategy::TenantPrefix => {
                let tenant_id = event.after
                    .as_ref()
                    .and_then(|a| a.get("tenant_id"))
                    .and_then(|t| t.as_str())
                    .unwrap_or(&event.source_database);

                let mut hasher = DefaultHasher::new();
                tenant_id.hash(&mut hasher);
                let hash_val = hasher.finish();
                (hash_val % (self.num_partitions as u64)) as u32
            }
            PartitionStrategy::RoundRobin => {
                let mut hasher = DefaultHasher::new();
                event.id.hash(&mut hasher);
                let hash_val = hasher.finish();
                (hash_val % (self.num_partitions as u64)) as u32
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::source::Operation;
    use serde_json::json;
    use chrono::Utc;

    #[test]
    fn test_key_hash_partitioning() {
        let router = PartitionRouter::new(4, PartitionStrategy::KeyHash);

        let event1 = ChangeEvent {
            id: "e1".into(),
            source_database: "db".into(),
            source_table_or_collection: "users".into(),
            operation: Operation::Create,
            timestamp: Utc::now(),
            key: json!({ "user_id": 42 }),
            before: None,
            after: Some(json!({ "user_id": 42, "tenant_id": "tenant-a" })),
            transaction_id: None,
            offset: "1".into(),
        };

        let p1 = router.resolve_partition(&event1);
        let p2 = router.resolve_partition(&event1);

        assert_eq!(p1, p2);
        assert!(p1 < 4);
    }

    #[test]
    fn test_tenant_partitioning() {
        let router = PartitionRouter::new(8, PartitionStrategy::TenantPrefix);

        let event = ChangeEvent {
            id: "e2".into(),
            source_database: "db".into(),
            source_table_or_collection: "users".into(),
            operation: Operation::Create,
            timestamp: Utc::now(),
            key: json!({ "user_id": 100 }),
            before: None,
            after: Some(json!({ "user_id": 100, "tenant_id": "tenant-alpha" })),
            transaction_id: None,
            offset: "2".into(),
        };

        let p = router.resolve_partition(&event);
        assert!(p < 8);
    }
}

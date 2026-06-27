use std::collections::HashMap;
use crate::source::{ChangeEvent, Operation};

pub struct TransactionBuffer {
    buffer: HashMap<String, Vec<ChangeEvent>>,
}

impl TransactionBuffer {
    pub fn new() -> Self {
        Self {
            buffer: HashMap::new(),
        }
    }

    /// Processes an incoming event.
    /// Returns a list of events to be flushed downstream.
    pub fn process(&mut self, event: ChangeEvent) -> Vec<ChangeEvent> {
        match &event.transaction_id {
            None => {
                // If there's no transaction context, output immediately.
                vec![event]
            }
            Some(tx_id) => {
                match event.operation {
                    Operation::Commit => {
                        // Transaction is committed. Flush all buffered mutations for this tx.
                        if let Some(mutations) = self.buffer.remove(tx_id) {
                            mutations
                        } else {
                            vec![]
                        }
                    }
                    Operation::Rollback => {
                        // Transaction is rolled back. Drop all mutations.
                        self.buffer.remove(tx_id);
                        vec![]
                    }
                    _ => {
                        // Regular mutation (Create, Update, Delete, Snapshot) inside a transaction.
                        // Buffer it and return nothing.
                        self.buffer.entry(tx_id.clone()).or_insert_with(Vec::new).push(event);
                        vec![]
                    }
                }
            }
        }
    }

    /// Clean up expired/orphaned transactions to manage memory growth.
    pub fn prune(&mut self, max_size: usize) {
        if self.buffer.len() > max_size {
            self.buffer.clear();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;

    #[test]
    fn test_transaction_commit() {
        let mut tb = TransactionBuffer::new();

        let e1 = ChangeEvent {
            id: "1".into(),
            source_database: "db".into(),
            source_table_or_collection: "tbl".into(),
            operation: Operation::Create,
            timestamp: Utc::now(),
            key: serde_json::json!({}),
            before: None,
            after: None,
            transaction_id: Some("tx-123".into()),
            offset: "1".into(),
        };

        let e2 = ChangeEvent {
            id: "2".into(),
            source_database: "db".into(),
            source_table_or_collection: "tbl".into(),
            operation: Operation::Update,
            timestamp: Utc::now(),
            key: serde_json::json!({}),
            before: None,
            after: None,
            transaction_id: Some("tx-123".into()),
            offset: "2".into(),
        };

        let commit = ChangeEvent {
            id: "3".into(),
            source_database: "db".into(),
            source_table_or_collection: "tbl".into(),
            operation: Operation::Commit,
            timestamp: Utc::now(),
            key: serde_json::json!({}),
            before: None,
            after: None,
            transaction_id: Some("tx-123".into()),
            offset: "3".into(),
        };

        // Mutation 1 - buffered
        let out = tb.process(e1);
        assert!(out.is_empty());

        // Mutation 2 - buffered
        let out = tb.process(e2);
        assert!(out.is_empty());

        // Commit - flushes mutations
        let out = tb.process(commit);
        assert_eq!(out.len(), 2);
        assert_eq!(out[0].id, "1");
        assert_eq!(out[1].id, "2");
    }

    #[test]
    fn test_transaction_rollback() {
        let mut tb = TransactionBuffer::new();

        let e1 = ChangeEvent {
            id: "1".into(),
            source_database: "db".into(),
            source_table_or_collection: "tbl".into(),
            operation: Operation::Create,
            timestamp: Utc::now(),
            key: serde_json::json!({}),
            before: None,
            after: None,
            transaction_id: Some("tx-123".into()),
            offset: "1".into(),
        };

        let rollback = ChangeEvent {
            id: "2".into(),
            source_database: "db".into(),
            source_table_or_collection: "tbl".into(),
            operation: Operation::Rollback,
            timestamp: Utc::now(),
            key: serde_json::json!({}),
            before: None,
            after: None,
            transaction_id: Some("tx-123".into()),
            offset: "2".into(),
        };

        // Mutation 1 - buffered
        let out = tb.process(e1);
        assert!(out.is_empty());

        // Rollback - drops mutations
        let out = tb.process(rollback);
        assert!(out.is_empty());

        // Check buffer is clean
        assert!(tb.buffer.is_empty());
    }

    #[test]
    fn test_passthrough() {
        let mut tb = TransactionBuffer::new();

        let e1 = ChangeEvent {
            id: "1".into(),
            source_database: "db".into(),
            source_table_or_collection: "tbl".into(),
            operation: Operation::Create,
            timestamp: Utc::now(),
            key: serde_json::json!({}),
            before: None,
            after: None,
            transaction_id: None,
            offset: "1".into(),
        };

        let out = tb.process(e1);
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].id, "1");
    }
}

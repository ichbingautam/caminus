use std::collections::HashMap;
use crate::source::{ChangeEvent, Operation};

pub struct WatermarkSnapshotter {
    active_chunk: HashMap<String, ChangeEvent>,
    in_watermark_window: bool,
    active_tx: Option<String>,
}

impl WatermarkSnapshotter {
    pub fn new() -> Self {
        Self {
            active_chunk: HashMap::new(),
            in_watermark_window: false,
            active_tx: None,
        }
    }

    /// Load a chunk query result into the snapshotter.
    pub fn load_chunk(&mut self, chunk_events: Vec<ChangeEvent>) {
        self.active_chunk.clear();
        for event in chunk_events {
            self.active_chunk.insert(event.key.to_string(), event);
        }
    }

    /// Processes an incoming replication log event.
    /// Reconciles snapshot chunk data dynamically to maintain consistency.
    pub fn process_replication_event(&mut self, event: &ChangeEvent) -> Option<ChangeEvent> {
        // Detect watermark events from metadata table
        if event.source_table_or_collection == "caminus_watermarks" {
            if event.operation == Operation::Create || event.operation == Operation::Commit {
                if !self.in_watermark_window {
                    // Low watermark marker
                    self.in_watermark_window = true;
                    self.active_tx = event.transaction_id.clone();
                    println!(
                        "[Watermark Engine] Low watermark reached for transaction {:?}",
                        self.active_tx
                    );
                } else {
                    // High watermark marker
                    self.in_watermark_window = false;
                    self.active_tx = None;
                    println!("[Watermark Engine] High watermark reached. Reconciled snapshot chunk ready.");
                }
            }
            return None; // Filter watermark rows out from downstream targets
        }

        if self.in_watermark_window {
            let key_str = event.key.to_string();
            if self.active_chunk.contains_key(&key_str) {
                match event.operation {
                    Operation::Delete => {
                        // Reconcile deletion: remove row from snapshot chunk
                        self.active_chunk.remove(&key_str);
                    }
                    Operation::Update | Operation::Create => {
                        // Reconcile mutation: overwrite snapshot values with log event values
                        if let Some(chunk_event) = self.active_chunk.get_mut(&key_str) {
                            chunk_event.after = event.after.clone();
                            chunk_event.timestamp = event.timestamp;
                        }
                    }
                    _ => {}
                }
            }
        }

        Some(event.clone())
    }

    /// Get all remaining reconciled chunk events to flush downstream.
    pub fn flush_chunk(&mut self) -> Vec<ChangeEvent> {
        let flushed = self.active_chunk.values().cloned().collect();
        self.active_chunk.clear();
        flushed
    }

    pub fn is_in_window(&self) -> bool {
        self.in_watermark_window
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;
    use serde_json::json;

    #[test]
    fn test_watermark_reconciliation() {
        let mut snapshotter = WatermarkSnapshotter::new();

        // 1. Prepare a chunk select result
        let chunk_event = ChangeEvent {
            id: "snap-1".into(),
            source_database: "db".into(),
            source_table_or_collection: "users".into(),
            operation: Operation::Snapshot,
            timestamp: Utc::now(),
            key: json!({ "id": 1 }),
            before: None,
            after: Some(json!({ "id": 1, "name": "John" })),
            transaction_id: None,
            offset: "0".into(),
        };
        snapshotter.load_chunk(vec![chunk_event]);

        // 2. Start of chunk select (low watermark event)
        let low_watermark = ChangeEvent {
            id: "wm-low".into(),
            source_database: "db".into(),
            source_table_or_collection: "caminus_watermarks".into(),
            operation: Operation::Create,
            timestamp: Utc::now(),
            key: json!({ "wm_id": "low-123" }),
            before: None,
            after: None,
            transaction_id: Some("tx-watermark".into()),
            offset: "1".into(),
        };
        let out = snapshotter.process_replication_event(&low_watermark);
        assert!(out.is_none());
        assert!(snapshotter.is_in_window());

        // 3. User updates row 1 while chunk query is running
        let update_event = ChangeEvent {
            id: "pg-evt-2".into(),
            source_database: "db".into(),
            source_table_or_collection: "users".into(),
            operation: Operation::Update,
            timestamp: Utc::now(),
            key: json!({ "id": 1 }),
            before: None,
            after: Some(json!({ "id": 1, "name": "John Updated" })),
            transaction_id: Some("tx-user-update".into()),
            offset: "2".into(),
        };
        let out = snapshotter.process_replication_event(&update_event);
        assert!(out.is_some());

        // 4. End of chunk select (high watermark event)
        let high_watermark = ChangeEvent {
            id: "wm-high".into(),
            source_database: "db".into(),
            source_table_or_collection: "caminus_watermarks".into(),
            operation: Operation::Create,
            timestamp: Utc::now(),
            key: json!({ "wm_id": "high-123" }),
            before: None,
            after: None,
            transaction_id: Some("tx-watermark".into()),
            offset: "3".into(),
        };
        let out = snapshotter.process_replication_event(&high_watermark);
        assert!(out.is_none());
        assert!(!snapshotter.is_in_window());

        // 5. Assert the snapshot chunk has reconciled "John" to "John Updated"
        let chunk_events = snapshotter.flush_chunk();
        assert_eq!(chunk_events.len(), 1);
        assert_eq!(
            chunk_events[0].after.as_ref().unwrap().get("name").unwrap(),
            "John Updated"
        );
    }
}

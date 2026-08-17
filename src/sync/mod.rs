use crate::source::ChangeEvent;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClusterNode {
    pub cluster_id: String,
    pub endpoint_url: String,
    pub is_active: bool,
}

pub struct SyncCoordinator {
    pub local_cluster_id: String,
    pub remote_peers: Vec<ClusterNode>,
}

impl SyncCoordinator {
    pub fn new(local_cluster_id: impl Into<String>) -> Self {
        Self {
            local_cluster_id: local_cluster_id.into(),
            remote_peers: Vec::new(),
        }
    }

    pub fn register_peer(&mut self, cluster_id: impl Into<String>, endpoint_url: impl Into<String>) {
        self.remote_peers.push(ClusterNode {
            cluster_id: cluster_id.into(),
            endpoint_url: endpoint_url.into(),
            is_active: true,
        });
    }

    /// Evaluates if an incoming event originated locally or from a remote cluster loop.
    pub fn should_forward(&self, event: &ChangeEvent) -> bool {
        if let Some(tx_id) = &event.transaction_id {
            if tx_id.starts_with(&format!("remote-{}", self.local_cluster_id)) {
                return false;
            }
        }
        true
    }

    /// Resolves concurrent mutation conflicts using Last-Write-Wins (LWW) timestamp ordering.
    pub fn resolve_conflict<'a>(&self, event_a: &'a ChangeEvent, event_b: &'a ChangeEvent) -> &'a ChangeEvent {
        if event_a.timestamp >= event_b.timestamp {
            event_a
        } else {
            event_b
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;
    use serde_json::json;
    use crate::source::Operation;

    #[test]
    fn test_sync_coordinator_loop_suppression() {
        let coordinator = SyncCoordinator::new("us-east-1");

        let local_event = ChangeEvent {
            id: "evt-1".to_string(),
            source_database: "caminus_db".to_string(),
            source_table_or_collection: "users".to_string(),
            operation: Operation::Create,
            timestamp: Utc::now(),
            key: json!({ "id": 1 }),
            before: None,
            after: Some(json!({ "id": 1 })),
            transaction_id: Some("local-tx-1".to_string()),
            offset: "offset-1".to_string(),
        };

        assert!(coordinator.should_forward(&local_event));

        let remote_loop_event = ChangeEvent {
            transaction_id: Some("remote-us-east-1-tx".to_string()),
            ..local_event
        };

        assert!(!coordinator.should_forward(&remote_loop_event));
    }

    #[test]
    fn test_lww_conflict_resolution() {
        let coordinator = SyncCoordinator::new("us-east-1");

        let now = Utc::now();
        let earlier = now - chrono::Duration::seconds(5);

        let event_old = ChangeEvent {
            id: "evt-old".to_string(),
            source_database: "db".to_string(),
            source_table_or_collection: "users".to_string(),
            operation: Operation::Update,
            timestamp: earlier,
            key: json!({ "id": 1 }),
            before: None,
            after: Some(json!({ "name": "Old" })),
            transaction_id: None,
            offset: "off-1".to_string(),
        };

        let event_new = ChangeEvent {
            id: "evt-new".to_string(),
            timestamp: now,
            after: Some(json!({ "name": "New" })),
            ..event_old.clone()
        };

        let winner = coordinator.resolve_conflict(&event_old, &event_new);
        assert_eq!(winner.id, "evt-new");
    }
}

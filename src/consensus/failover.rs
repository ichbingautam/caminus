use crate::storage::StateStore;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::Mutex;

pub struct FailoverController {
    pub node_id: u64,
    pub heartbeat_interval: Duration,
    pub last_leader_heartbeat: Mutex<Instant>,
    pub active_lease: Mutex<bool>,
}

impl FailoverController {
    pub fn new(node_id: u64, heartbeat_millis: u64) -> Self {
        Self {
            node_id,
            heartbeat_interval: Duration::from_millis(heartbeat_millis),
            last_leader_heartbeat: Mutex::new(Instant::now()),
            active_lease: Mutex::new(false),
        }
    }

    /// Records a heartbeat pulse from the active leader.
    pub async fn record_heartbeat(&self) {
        let mut last = self.last_leader_heartbeat.lock().await;
        *last = Instant::now();
    }

    /// Evaluates if the leader heartbeat has lapsed and claims leadership if timed out.
    pub async fn check_and_claim_lease(&self) -> bool {
        let last = self.last_leader_heartbeat.lock().await;
        let elapsed = last.elapsed();
        let mut lease = self.active_lease.lock().await;

        if elapsed > self.heartbeat_interval * 2 {
            println!(
                "[FAILOVER CONTROLLER] Leader heartbeat timed out ({:?} elapsed). Node {} claiming active stream lease!",
                elapsed, self.node_id
            );
            *lease = true;
            true
        } else {
            *lease
        }
    }

    /// Fetches the last checkpoint offset from RocksDB to resume replication without data loss.
    pub fn resume_checkpoint_offset(&self, store: &StateStore, source_id: &str) -> Option<String> {
        match store.get_offset(source_id) {
            Ok(Some(offset)) => {
                println!(
                    "[FAILOVER CONTROLLER] Node {} resuming stream '{}' from checkpoint offset: {}",
                    self.node_id, source_id, offset
                );
                Some(offset)
            }
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    #[tokio::test]
    async fn test_failover_controller_lease_claim() {
        let controller = FailoverController::new(2, 50);

        assert!(!controller.check_and_claim_lease().await);

        tokio::time::sleep(Duration::from_millis(120)).await;

        assert!(controller.check_and_claim_lease().await);
    }

    #[test]
    fn test_checkpoint_resume() {
        let test_path = "./data/test_failover_db";
        let _ = fs::remove_dir_all(test_path);
        let store = StateStore::new(test_path).unwrap();

        store
            .save_offset("pg_users", "offset-checkpoint-555")
            .unwrap();

        let controller = FailoverController::new(1, 1000);
        let resumed = controller.resume_checkpoint_offset(&store, "pg_users");

        assert_eq!(resumed, Some("offset-checkpoint-555".to_string()));

        let _ = fs::remove_dir_all(test_path);
    }
}

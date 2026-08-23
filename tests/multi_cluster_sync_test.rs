use chrono::Utc;
use serde_json::json;

use caminus::sink::CdcSink;
use caminus::sink::forwarder::ClusterForwarderSink;
use caminus::source::{ChangeEvent, Operation};
use caminus::sync::SyncCoordinator;

#[tokio::test]
async fn test_active_active_multi_cluster_sync() {
    let mut coord_us_east = SyncCoordinator::new("us-east-1");
    let mut coord_eu_west = SyncCoordinator::new("eu-west-1");

    coord_us_east.register_peer("eu-west-1", "https://eu.caminus.io:9000");
    coord_eu_west.register_peer("us-east-1", "https://us.caminus.io:9000");

    let forwarder_to_eu = ClusterForwarderSink::new("eu-west-1", "https://eu.caminus.io:9000");

    // 1. Generate local mutation in US-East
    let event_us = ChangeEvent {
        id: "evt-us-100".to_string(),
        source_database: "users_db".to_string(),
        source_table_or_collection: "accounts".to_string(),
        operation: Operation::Update,
        timestamp: Utc::now(),
        key: json!({ "account_id": 42 }),
        before: None,
        after: Some(json!({ "balance": 1500.00 })),
        transaction_id: Some("tx-us-100".to_string()),
        offset: "us-offset-100".to_string(),
    };

    // Step A: US-East forwards event to EU-West
    assert!(coord_us_east.should_forward(&event_us));
    assert!(forwarder_to_eu.send(&event_us).await.is_ok());

    // Step B: Loop suppression check when EU-West receives echo event tagged with origin us-east-1
    let echoed_event = ChangeEvent {
        transaction_id: Some("remote-eu-west-1-tx".to_string()),
        ..event_us.clone()
    };
    assert!(!coord_eu_west.should_forward(&echoed_event));

    // Step C: Concurrent conflict resolution (LWW)
    let earlier_timestamp = Utc::now() - chrono::Duration::seconds(10);
    let event_eu_conflicting = ChangeEvent {
        id: "evt-eu-100".to_string(),
        timestamp: earlier_timestamp,
        after: Some(json!({ "balance": 1200.00 })),
        ..event_us.clone()
    };

    let winner = coord_us_east.resolve_conflict(&event_us, &event_eu_conflicting);
    assert_eq!(winner.id, "evt-us-100");
    assert_eq!(winner.after.as_ref().unwrap()["balance"], 1500.00);
}

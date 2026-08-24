use chrono::Utc;
use serde_json::json;
use std::fs;
use std::sync::Arc;
use std::time::Instant;

use caminus::resiliency::dedup::DeduplicationFilter;
use caminus::resiliency::rate_limiter::TokenBucketLimiter;
use caminus::router::{PartitionRouter, PartitionStrategy};
use caminus::serialize::serialize_event;
use caminus::sink::{CdcSink, stdout::StdoutSink};
use caminus::source::{ChangeEvent, Operation};
use caminus::storage::StateStore;
use caminus::storage::schema::{SchemaCompatibility, SchemaRegistry};

#[tokio::test]
async fn test_high_throughput_e2e_pipeline() {
    let test_path = "./data/test_e2e_bench_db";
    let _ = fs::remove_dir_all(test_path);

    // 1. Initialize StateStore
    let store = Arc::new(StateStore::new(test_path).expect("Failed to initialize RocksDB"));

    // 2. Register Table Schema
    let schema = json!({
        "fields": {
            "id": "integer",
            "name": "string"
        }
    });
    SchemaRegistry::register_schema(&store, "pg_users", schema, SchemaCompatibility::Backward)
        .expect("Failed to register schema");

    // 3. Components Initialization
    let mut dedup_filter = DeduplicationFilter::new(5000);
    let rate_limiter = TokenBucketLimiter::new(10000, 10000.0);
    let router = PartitionRouter::new(4, PartitionStrategy::KeyHash);
    let sink = StdoutSink;

    let total_events = 1000;
    let start_time = Instant::now();

    for i in 0..total_events {
        rate_limiter.acquire(1).await;

        let event = ChangeEvent {
            id: format!("bench-evt-{}", i),
            source_database: "caminus_db".to_string(),
            source_table_or_collection: "users".to_string(),
            operation: Operation::Create,
            timestamp: Utc::now(),
            key: json!({ "id": i }),
            before: None,
            after: Some(json!({ "id": i, "name": format!("User-{}", i) })),
            transaction_id: Some("tx-1".to_string()),
            offset: format!("offset-{}", i),
        };

        // Step A: Deduplication check
        assert!(!dedup_filter.check_and_track(&event.id));

        // Step B: Schema Validation
        assert!(SchemaRegistry::validate_event(&store, "pg_users", &event).is_ok());

        // Step C: Partition Resolution
        let partition = router.resolve_partition(&event);
        assert!(partition < 4);

        // Step D: SIMD Serialization
        let serialized = serialize_event(&event).expect("Failed SIMD serialization");
        assert!(serialized.contains(&format!("User-{}", i)));

        // Step E: Sink Delivery
        let send_res = sink.send(&event).await;
        assert!(send_res.is_ok());

        // Step F: Save Offset
        store.save_offset("pg_users", &event.offset).unwrap();
    }

    let duration = start_time.elapsed();
    let events_per_sec = (total_events as f64) / duration.as_secs_f64();
    println!(
        "[E2E BENCHMARK] Processed {} events in {:?}. Throughput: {:.2} events/sec",
        total_events, duration, events_per_sec
    );

    // Verify last offset stored
    let last_offset = store.get_offset("pg_users").unwrap();
    assert_eq!(last_offset, Some("offset-999".to_string()));

    let _ = fs::remove_dir_all(test_path);
}

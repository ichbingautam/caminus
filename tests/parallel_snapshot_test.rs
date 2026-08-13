use std::fs;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;
use chrono::Utc;
use serde_json::json;

use caminus::source::{ChangeEvent, Operation};
use caminus::storage::StateStore;
use caminus::snapshot::parallel::ParallelSnapshotter;

#[tokio::test]
async fn test_multi_threaded_parallel_snapshot_backfill() {
    let test_path = "./data/test_parallel_snapshot_integration_db";
    let _ = fs::remove_dir_all(test_path);

    let store = Arc::new(StateStore::new(test_path).expect("Failed to initialize RocksDB"));
    let total_rows = 10_000u64;
    let chunk_size = 2_500u64;
    let worker_count = 4;

    let snapshotter = ParallelSnapshotter::new("postgres_users", total_rows, chunk_size);
    let events_generated = Arc::new(AtomicU64::new(0));

    let start_time = Instant::now();

    let counter_clone = Arc::clone(&events_generated);
    snapshotter.process_chunks(&store, worker_count, move |chunk| {
        let counter = Arc::clone(&counter_clone);
        async move {
            let mut events = Vec::new();
            for i in chunk.min_key..=chunk.max_key {
                let event = ChangeEvent {
                    id: format!("snap-{}", i),
                    source_database: "caminus_db".to_string(),
                    source_table_or_collection: "postgres_users".to_string(),
                    operation: Operation::Create,
                    timestamp: Utc::now(),
                    key: json!({ "id": i }),
                    before: None,
                    after: Some(json!({ "id": i, "name": format!("User-{}", i) })),
                    transaction_id: Some("snap-tx".to_string()),
                    offset: format!("snap-offset-{}", i),
                };
                events.push(event);
                counter.fetch_add(1, Ordering::Relaxed);
            }
            events
        }
    }).await;

    let duration = start_time.elapsed();
    let total_processed = events_generated.load(Ordering::Relaxed);

    assert_eq!(total_processed, total_rows);

    let progress = store.get_offset("postgres_users_snapshot_progress").unwrap();
    assert_eq!(progress, Some("4/4".to_string()));

    println!(
        "[PARALLEL SNAPSHOT INTEGRATION] Successfully backfilled {} rows across {} parallel workers in {:?}. Speed: {:.2} rows/sec",
        total_processed, worker_count, duration, (total_processed as f64) / duration.as_secs_f64()
    );

    let _ = fs::remove_dir_all(test_path);
}

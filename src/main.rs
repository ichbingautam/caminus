use std::sync::Arc;
use tokio::signal;
use futures_util::StreamExt;
use std::time::Duration;

mod source;
mod storage;
mod sink;
mod buffer;
mod transform;
mod consensus;
mod snapshot;
mod resiliency;

use source::{CdcSource, postgres::PostgresSource, cassandra::CassandraSource};
use storage::StateStore;
use sink::{CdcSink, stdout::StdoutSink, kafka::KafkaSink};
use buffer::TransactionBuffer;
use transform::{Transformer, WasmTransformer};
use consensus::ClusterCoordinator;
use snapshot::watermark::WatermarkSnapshotter;
use resiliency::dedup::DeduplicationFilter;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("Starting Caminus CDC Engine (Phase 3 - Cluster Coordination & Resiliency)...");

    // Initialize distributed consensus coordinator (Node 1)
    let coordinator = Arc::new(ClusterCoordinator::new(1));
    coordinator.start_election_loop();

    // Initialize state store directory
    let state_store_path = "./data/caminus_state";
    let state_store = Arc::new(StateStore::new(state_store_path)?);
    println!("Initialized local state store at {}", state_store_path);

    // Initialize output sinks
    let stdout_sink = Arc::new(StdoutSink);
    let kafka_sink = Arc::new(KafkaSink::new(
        "localhost:9092".to_string(),
        "caminus_mutations".to_string(),
        "caminus_engine".to_string(),
    ));

    // Bootstrap passthrough WebAssembly transform module
    let wat = r#"
        (module
          (memory (export "memory") 1)
          (func (export "alloc") (param i32) (result i32)
            i32.const 0
          )
          (func (export "transform") (param i32 i32) (result i32)
            local.get 1
          )
        )
    "#;
    let transformer = Arc::new(WasmTransformer::new(wat.as_bytes())?);

    // Bootstrap sources
    let pg_source = PostgresSource::new(
        "postgresql://postgres:password@localhost:5432/caminus_db".to_string(),
        "caminus_slot".to_string(),
        "caminus_pub".to_string(),
    );

    let cass_source = CassandraSource::new("/var/lib/cassandra/cdc_raw".to_string());

    // Spawn PostgreSQL logical replication task (consensus and watermark-snapshot aware)
    let pg_store = Arc::clone(&state_store);
    let pg_transformer = Arc::clone(&transformer);
    let pg_stdout = Arc::clone(&stdout_sink);
    let pg_kafka = Arc::clone(&kafka_sink);
    let pg_coord = Arc::clone(&coordinator);
    
    let pg_handle = tokio::spawn(async move {
        println!("Waiting for PostgreSQL stream node leadership...");
        
        let mut tx_buffer = TransactionBuffer::new();
        let mut watermark_engine = WatermarkSnapshotter::new();
        let mut dedup_filter = DeduplicationFilter::new(1000);

        // Fetch stream starting offset
        let last_offset = pg_store.get_offset("postgres_users").unwrap_or(None);
        
        match pg_source.start_stream(last_offset).await {
            Ok(mut stream) => {
                while let Some(event_result) = stream.next().await {
                    // Check consensus role before handling events
                    if !pg_coord.is_leader() {
                        tokio::time::sleep(Duration::from_millis(100)).await;
                        continue;
                    }

                    match event_result {
                        Ok(event) => {
                            // 1. Check deduplication exactly-once filter
                            if dedup_filter.check_and_track(&event.id) {
                                println!("[PG Source] Filtered out duplicate event ID: {}", event.id);
                                continue;
                            }

                            // 2. Interleave and merge with Netflix DBLog watermark logic
                            let processed_event = match watermark_engine.process_replication_event(&event) {
                                Some(e) => e,
                                None => continue, // Filtered out metadata event
                            };

                            let raw_op = processed_event.operation.clone();
                            let raw_tx = processed_event.transaction_id.clone();
                            
                            // 3. Pipe through transactional buffer
                            let mutations = tx_buffer.process(processed_event);
                            
                            if raw_op == source::Operation::Commit {
                                println!(
                                    "[PG Source] Received COMMIT for transaction {:?}. Flushing {} mutations...",
                                    raw_tx, mutations.len()
                                );
                            }

                            // 4. Transform and dispatch to Sinks
                            for mut_event in mutations {
                                if let Ok(transformed) = pg_transformer.transform(mut_event) {
                                    let _ = pg_kafka.send(&transformed).await;
                                    let _ = pg_stdout.send(&transformed).await;
                                    
                                    let _ = pg_store.save_offset("postgres_users", &transformed.offset);
                                }
                            }
                        }
                        Err(e) => {
                            eprintln!("[PG Source] Ingestion stream error: {:?}", e);
                            break;
                        }
                    }
                }
            }
            Err(e) => {
                eprintln!("[PG Source] Failed to start stream: {:?}", e);
            }
        }
    });

    // Spawn Cassandra CommitLog parsing task
    let cass_store = Arc::clone(&state_store);
    let cass_transformer = Arc::clone(&transformer);
    let cass_stdout = Arc::clone(&stdout_sink);
    let cass_kafka = Arc::clone(&kafka_sink);
    let cass_coord = Arc::clone(&coordinator);
    
    let cass_handle = tokio::spawn(async move {
        println!("Waiting for Cassandra stream node leadership...");
        
        let mut tx_buffer = TransactionBuffer::new();
        let mut dedup_filter = DeduplicationFilter::new(1000);
        let last_offset = cass_store.get_offset("cassandra_sensors").unwrap_or(None);

        match cass_source.start_stream(last_offset).await {
            Ok(mut stream) => {
                while let Some(event_result) = stream.next().await {
                    if !cass_coord.is_leader() {
                        tokio::time::sleep(Duration::from_millis(100)).await;
                        continue;
                    }

                    match event_result {
                        Ok(event) => {
                            if dedup_filter.check_and_track(&event.id) {
                                continue;
                            }

                            let mutations = tx_buffer.process(event);
                            for mut_event in mutations {
                                if let Ok(transformed) = cass_transformer.transform(mut_event) {
                                    let _ = cass_kafka.send(&transformed).await;
                                    let _ = cass_stdout.send(&transformed).await;
                                    let _ = cass_store.save_offset("cassandra_sensors", &transformed.offset);
                                }
                            }
                        }
                        Err(e) => {
                            eprintln!("[Cassandra Source] Ingestion stream error: {:?}", e);
                            break;
                        }
                    }
                }
            }
            Err(e) => {
                eprintln!("[Cassandra Source] Failed to start stream: {:?}", e);
            }
        }
    });

    // Wait for shutdown signal (Ctrl+C)
    signal::ctrl_c().await?;
    println!("Shutdown signal received. Stopping Caminus engine...");

    pg_handle.abort();
    cass_handle.abort();

    Ok(())
}

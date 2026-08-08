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
mod observability;
mod serialize;

use source::{CdcSource, postgres::PostgresSource, cassandra::CassandraSource};
use storage::StateStore;
use storage::schema::{SchemaRegistry, SchemaCompatibility};
use resiliency::dlq::{DeadLetterQueue, DlqRecord};
use sink::{CdcSink, stdout::StdoutSink, kafka::KafkaSink};
use buffer::TransactionBuffer;
use transform::{Transformer, WasmTransformer};
use consensus::ClusterCoordinator;
use snapshot::watermark::WatermarkSnapshotter;
use resiliency::dedup::DeduplicationFilter;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("Starting Caminus CDC Engine (Phase 5 - Schema Evolution & DLQ Engine)...");

    // Initialize Prometheus metrics scraper server on port 9000
    if let Err(e) = observability::init_metrics(9000) {
        eprintln!("[Metrics Server] Failed to initialize metrics exporter: {:?}", e);
    }

    // Initialize distributed consensus coordinator (Node 1)
    let coordinator = Arc::new(ClusterCoordinator::new(1));
    coordinator.start_election_loop();

    // Initialize state store directory
    let state_store_path = "./data/caminus_state";
    let state_store = Arc::new(StateStore::new(state_store_path)?);
    println!("Initialized local state store at {}", state_store_path);

    // Register active table schema in store
    let user_schema = serde_json::json!({
        "fields": {
            "id": "integer",
            "name": "string"
        }
    });
    if let Err(e) = SchemaRegistry::register_schema(&state_store, "postgres_users", user_schema, SchemaCompatibility::Backward) {
        eprintln!("Failed to register initial schema: {:?}", e);
    }

    // Initialize Dead Letter Queue (DLQ)
    let dlq = Arc::new(DeadLetterQueue::new("caminus_dlq_poison_pills".to_string()));

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

    // Spawn PostgreSQL logical replication task
    let pg_store = Arc::clone(&state_store);
    let pg_transformer = Arc::clone(&transformer);
    let pg_stdout = Arc::clone(&stdout_sink);
    let pg_kafka = Arc::clone(&kafka_sink);
    let pg_coord = Arc::clone(&coordinator);
    let pg_dlq = Arc::clone(&dlq);
    
    let pg_handle = tokio::spawn(async move {
        println!("Waiting for PostgreSQL stream node leadership...");
        
        let mut tx_buffer = TransactionBuffer::new();
        let mut watermark_engine = WatermarkSnapshotter::new();
        let mut dedup_filter = DeduplicationFilter::new(1000);

        let last_offset = pg_store.get_offset("postgres_users").unwrap_or(None);
        
        match pg_source.start_stream(last_offset).await {
            Ok(mut stream) => {
                while let Some(event_result) = stream.next().await {
                    if !pg_coord.is_leader() {
                        tokio::time::sleep(Duration::from_millis(100)).await;
                        continue;
                    }

                    let processing_start = std::time::Instant::now();

                    match event_result {
                        Ok(event) => {
                            // 1. Deduplication check
                            if dedup_filter.check_and_track(&event.id) {
                                continue;
                            }

                            // 2. Schema compatibility validation
                            if let Err(e) = SchemaRegistry::validate_event(&pg_store, "postgres_users", &event) {
                                let dlq_record = DlqRecord::new(
                                    event,
                                    e.to_string(),
                                    "SchemaValidation".to_string(),
                                    1,
                                );
                                let _ = pg_dlq.route_to_dlq(&dlq_record);
                                continue;
                            }

                            // 3. Watermark snapshot reconciliation
                            let processed_event = match watermark_engine.process_replication_event(&event) {
                                Some(e) => e,
                                None => continue,
                            };

                            let raw_op = processed_event.operation.clone();
                            let raw_tx = processed_event.transaction_id.clone();
                            
                            let mutations = tx_buffer.process(processed_event);
                            
                            if raw_op == source::Operation::Commit {
                                println!(
                                    "[PG Source] Received COMMIT for transaction {:?}. Flushing {} mutations...",
                                    raw_tx, mutations.len()
                                );
                            }

                            for mut_event in mutations {
                                match pg_transformer.transform(mut_event.clone()) {
                                    Ok(transformed) => {
                                        let _ = pg_kafka.send(&transformed).await;
                                        let _ = pg_stdout.send(&transformed).await;
                                        let _ = pg_store.save_offset("postgres_users", &transformed.offset);

                                        let elapsed = processing_start.elapsed().as_secs_f64();
                                        observability::record_processing_latency(elapsed);
                                        observability::increment_throughput(1);
                                        
                                        let lag = (chrono::Utc::now() - transformed.timestamp).num_milliseconds() as f64 / 1000.0;
                                        observability::record_replication_lag(lag);
                                    }
                                    Err(e) => {
                                        let dlq_record = DlqRecord::new(
                                            mut_event,
                                            e.to_string(),
                                            "WasmTransform".to_string(),
                                            1,
                                        );
                                        let _ = pg_dlq.route_to_dlq(&dlq_record);
                                    }
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

    // Spawn Cassandra CommitLog ingestion task
    let cass_store = Arc::clone(&state_store);
    let cass_transformer = Arc::clone(&transformer);
    let cass_stdout = Arc::clone(&stdout_sink);
    let cass_kafka = Arc::clone(&kafka_sink);
    let cass_coord = Arc::clone(&coordinator);
    let cass_dlq = Arc::clone(&dlq);
    
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

                    let processing_start = std::time::Instant::now();

                    match event_result {
                        Ok(event) => {
                            if dedup_filter.check_and_track(&event.id) {
                                continue;
                            }

                            let mutations = tx_buffer.process(event);
                            for mut_event in mutations {
                                match cass_transformer.transform(mut_event.clone()) {
                                    Ok(transformed) => {
                                        let _ = cass_kafka.send(&transformed).await;
                                        let _ = cass_stdout.send(&transformed).await;
                                        let _ = cass_store.save_offset("cassandra_sensors", &transformed.offset);

                                        let elapsed = processing_start.elapsed().as_secs_f64();
                                        observability::record_processing_latency(elapsed);
                                        observability::increment_throughput(1);
                                        
                                        let lag = (chrono::Utc::now() - transformed.timestamp).num_milliseconds() as f64 / 1000.0;
                                        observability::record_replication_lag(lag);
                                    }
                                    Err(e) => {
                                        let dlq_record = DlqRecord::new(
                                            mut_event,
                                            e.to_string(),
                                            "WasmTransform".to_string(),
                                            1,
                                        );
                                        let _ = cass_dlq.route_to_dlq(&dlq_record);
                                    }
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

use std::sync::Arc;
use tokio::signal;
use futures_util::StreamExt;

mod source;
mod storage;
mod sink;
mod buffer;
mod transform;

use source::{CdcSource, postgres::PostgresSource, cassandra::CassandraSource};
use storage::StateStore;
use sink::{CdcSink, stdout::StdoutSink, kafka::KafkaSink};
use buffer::TransactionBuffer;
use transform::{Transformer, WasmTransformer};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("Starting Caminus CDC Engine (Phase 2 - Distribution & Ingestion)...");

    // Initialize state store directory under workspace
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

    // Bootstrap simple passthrough WebAssembly transform module
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
    println!("Initialized WebAssembly single message transform engine (Wasmtime)");

    // Bootstrap sources
    let pg_source = PostgresSource::new(
        "postgresql://postgres:password@localhost:5432/caminus_db".to_string(),
        "caminus_slot".to_string(),
        "caminus_pub".to_string(),
    );

    let cass_source = CassandraSource::new("/var/lib/cassandra/cdc_raw".to_string());

    // Spawn PostgreSQL logical replication ingestion task
    let pg_store = Arc::clone(&state_store);
    let pg_transformer = Arc::clone(&transformer);
    let pg_stdout = Arc::clone(&stdout_sink);
    let pg_kafka = Arc::clone(&kafka_sink);
    let pg_handle = tokio::spawn(async move {
        println!("Starting PostgreSQL logical replication stream...");
        let last_offset = pg_store.get_offset("postgres_users").unwrap_or(None);
        println!("Postgres last checkpoint offset: {:?}", last_offset);

        let mut tx_buffer = TransactionBuffer::new();

        match pg_source.start_stream(last_offset).await {
            Ok(mut stream) => {
                while let Some(event_result) = stream.next().await {
                    match event_result {
                        Ok(event) => {
                            let raw_op = event.operation.clone();
                            let raw_id = event.id.clone();
                            let raw_tx = event.transaction_id.clone();
                            
                            // 1. Pipe through transaction buffer
                            let mutations = tx_buffer.process(event);
                            
                            if raw_op == source::Operation::Commit {
                                println!(
                                    "[PG Source] Received COMMIT for transaction {:?}. Flushing {} buffered mutations...",
                                    raw_tx, mutations.len()
                                );
                            } else if raw_op == source::Operation::Rollback {
                                println!(
                                    "[PG Source] Received ROLLBACK for transaction {:?}. Discarded buffered mutations.",
                                    raw_tx
                                );
                            }

                            // 2. Transform and dispatch flushed mutations
                            for mut_event in mutations {
                                match pg_transformer.transform(mut_event) {
                                    Ok(transformed) => {
                                        // Send to sinks
                                        let _ = pg_kafka.send(&transformed).await;
                                        let _ = pg_stdout.send(&transformed).await;
                                        
                                        // Save offset checkpoint
                                        if let Err(e) = pg_store.save_offset("postgres_users", &transformed.offset) {
                                            eprintln!("[PG Source] Failed to save offset: {:?}", e);
                                        }
                                    }
                                    Err(e) => {
                                        eprintln!("[PG Source] Transform failed for event {}: {:?}", raw_id, e);
                                    }
                                }
                            }
                        }
                        Err(e) => {
                            eprintln!("[PG Source] Stream error: {:?}", e);
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

    // Spawn Cassandra CommitLog ingestion task (non-transactional passthrough)
    let cass_store = Arc::clone(&state_store);
    let cass_transformer = Arc::clone(&transformer);
    let cass_stdout = Arc::clone(&stdout_sink);
    let cass_kafka = Arc::clone(&kafka_sink);
    let cass_handle = tokio::spawn(async move {
        println!("Starting Cassandra CommitLog parsing stream...");
        let last_offset = cass_store.get_offset("cassandra_sensors").unwrap_or(None);
        println!("Cassandra last checkpoint offset: {:?}", last_offset);

        let mut tx_buffer = TransactionBuffer::new();

        match cass_source.start_stream(last_offset).await {
            Ok(mut stream) => {
                while let Some(event_result) = stream.next().await {
                    match event_result {
                        Ok(event) => {
                            let raw_id = event.id.clone();
                            
                            // Cassandra events generally have transaction_id = None, bypassing buffer immediately
                            let mutations = tx_buffer.process(event);
                            
                            for mut_event in mutations {
                                match cass_transformer.transform(mut_event) {
                                    Ok(transformed) => {
                                        let _ = cass_kafka.send(&transformed).await;
                                        let _ = cass_stdout.send(&transformed).await;
                                        
                                        if let Err(e) = cass_store.save_offset("cassandra_sensors", &transformed.offset) {
                                            eprintln!("[Cassandra Source] Failed to save offset: {:?}", e);
                                        }
                                    }
                                    Err(e) => {
                                        eprintln!("[Cassandra Source] Transform failed for event {}: {:?}", raw_id, e);
                                    }
                                }
                            }
                        }
                        Err(e) => {
                            eprintln!("[Cassandra Source] Stream error: {:?}", e);
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

    // Abort active task loops
    pg_handle.abort();
    cass_handle.abort();

    Ok(())
}

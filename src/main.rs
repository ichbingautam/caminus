use std::sync::Arc;
use tokio::signal;
use futures_util::StreamExt;

mod source;
mod storage;

use source::{CdcSource, postgres::PostgresSource, cassandra::CassandraSource};
use storage::StateStore;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("Starting Caminus CDC Engine...");

    // Initialize state store directory under workspace
    let state_store_path = "./data/caminus_state";
    let state_store = Arc::new(StateStore::new(state_store_path)?);
    println!("Initialized local state store at {}", state_store_path);

    // Bootstrap sources
    let pg_source = PostgresSource::new(
        "postgresql://postgres:password@localhost:5432/caminus_db".to_string(),
        "caminus_slot".to_string(),
        "caminus_pub".to_string(),
    );

    let cass_source = CassandraSource::new("/var/lib/cassandra/cdc_raw".to_string());

    // Spawn PostgreSQL ingestion task
    let pg_store = Arc::clone(&state_store);
    let pg_handle = tokio::spawn(async move {
        println!("Starting PostgreSQL replication stream task...");
        let last_offset = pg_store.get_offset("postgres_users").unwrap_or(None);
        println!("Postgres last checkpoint offset: {:?}", last_offset);

        match pg_source.start_stream(last_offset).await {
            Ok(mut stream) => {
                while let Some(event_result) = stream.next().await {
                    match event_result {
                        Ok(event) => {
                            println!(
                                "[PG Source] Event received - ID: {}, Table: {}, Op: {:?}, Offset: {}",
                                event.id, event.source_table_or_collection, event.operation, event.offset
                            );
                            // Persist offset checkpoint
                            if let Err(e) = pg_store.save_offset("postgres_users", &event.offset) {
                                eprintln!("[PG Source] Failed to save offset: {:?}", e);
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

    // Spawn Cassandra ingestion task
    let cass_store = Arc::clone(&state_store);
    let cass_handle = tokio::spawn(async move {
        println!("Starting Cassandra CommitLog parsing task...");
        let last_offset = cass_store.get_offset("cassandra_sensors").unwrap_or(None);
        println!("Cassandra last checkpoint offset: {:?}", last_offset);

        match cass_source.start_stream(last_offset).await {
            Ok(mut stream) => {
                while let Some(event_result) = stream.next().await {
                    match event_result {
                        Ok(event) => {
                            println!(
                                "[Cassandra Source] Event received - ID: {}, Table: {}, Op: {:?}, Offset: {}",
                                event.id, event.source_table_or_collection, event.operation, event.offset
                            );
                            // Persist offset checkpoint
                            if let Err(e) = cass_store.save_offset("cassandra_sensors", &event.offset) {
                                eprintln!("[Cassandra Source] Failed to save offset: {:?}", e);
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

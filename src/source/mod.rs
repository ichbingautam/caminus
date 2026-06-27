use std::error::Error;
use serde::{Serialize, Deserialize};
use chrono::{DateTime, Utc};
use futures_util::stream::BoxStream;

pub mod postgres;
pub mod cassandra;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum Operation {
    Create,
    Update,
    Delete,
    Snapshot,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChangeEvent {
    pub id: String,
    pub source_database: String,
    pub source_table_or_collection: String,
    pub operation: Operation,
    pub timestamp: DateTime<Utc>,
    pub key: serde_json::Value,
    pub before: Option<serde_json::Value>,
    pub after: Option<serde_json::Value>,
    pub transaction_id: Option<String>,
    pub offset: String,
}

pub trait CdcSource: Send + Sync {
    type Error: Error + Send + Sync + 'static;

    fn start_stream(
        &self,
        start_offset: Option<String>,
    ) -> impl std::future::Future<
        Output = Result<BoxStream<'static, Result<ChangeEvent, Self::Error>>, Self::Error>
    > + Send;
}

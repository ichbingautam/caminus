use chrono::{DateTime, Utc};
use futures_util::stream::BoxStream;
use serde::{Deserialize, Serialize};
use std::error::Error;
use std::time::Duration;

pub mod cassandra;
pub mod postgres;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum Operation {
    Create,
    Update,
    Delete,
    Snapshot,
    Commit,
    Rollback,
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
        Output = Result<BoxStream<'static, Result<ChangeEvent, Self::Error>>, Self::Error>,
    > + Send;
}

pub async fn retry_with_backoff<F, Fut, T, E>(
    mut f: F,
    max_attempts: usize,
    initial_delay: Duration,
    multiplier: f64,
    max_delay: Duration,
) -> Result<T, E>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<T, E>>,
    E: std::fmt::Debug,
{
    let mut attempt = 0;
    let mut delay = initial_delay;
    loop {
        attempt += 1;
        match f().await {
            Ok(val) => return Ok(val),
            Err(err) => {
                if attempt >= max_attempts {
                    return Err(err);
                }
                // Generate simple pseudo-random jitter (between 0.75 and 1.25)
                let seed = chrono::Utc::now().timestamp_nanos_opt().unwrap_or(42) as u64;
                let jitter = 0.75 + ((seed % 1000) as f64 / 2000.0); // 0.75 to 1.25
                let current_delay = delay.mul_f64(jitter);
                println!(
                    "[Recovery] Connection attempt {} failed: {:?}. Retrying in {:?}",
                    attempt, err, current_delay
                );
                tokio::time::sleep(current_delay).await;
                delay = std::cmp::min(delay.mul_f64(multiplier), max_delay);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU64, Ordering};

    #[tokio::test]
    async fn test_retry_with_backoff_success() {
        let attempts = Arc::new(AtomicU64::new(0));
        let attempts_clone = attempts.clone();
        let result = retry_with_backoff(
            move || {
                let attempts = attempts_clone.clone();
                async move {
                    let a = attempts.fetch_add(1, Ordering::Relaxed) + 1;
                    if a < 3 {
                        Err("temporary error")
                    } else {
                        Ok("success")
                    }
                }
            },
            5,
            Duration::from_millis(10),
            2.0,
            Duration::from_millis(50),
        )
        .await;

        assert_eq!(result, Ok("success"));
        assert_eq!(attempts.load(Ordering::Relaxed), 3);
    }

    #[tokio::test]
    async fn test_retry_with_backoff_failure() {
        let attempts = Arc::new(AtomicU64::new(0));
        let attempts_clone = attempts.clone();
        let result: Result<(), &str> = retry_with_backoff(
            move || {
                let attempts = attempts_clone.clone();
                async move {
                    attempts.fetch_add(1, Ordering::Relaxed);
                    Err("persistent error")
                }
            },
            3,
            Duration::from_millis(10),
            2.0,
            Duration::from_millis(50),
        )
        .await;

        assert_eq!(result, Err("persistent error"));
        assert_eq!(attempts.load(Ordering::Relaxed), 3);
    }
}

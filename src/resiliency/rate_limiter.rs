use std::sync::Arc;
use tokio::sync::Mutex;
use std::time::{Duration, Instant};

pub struct TokenBucketLimiter {
    capacity: u32,
    tokens: Mutex<f64>,
    refill_rate_per_sec: f64,
    last_refill: Mutex<Instant>,
}

impl TokenBucketLimiter {
    pub fn new(capacity: u32, refill_rate_per_sec: f64) -> Self {
        Self {
            capacity,
            tokens: Mutex::new(capacity as f64),
            refill_rate_per_sec,
            last_refill: Mutex::new(Instant::now()),
        }
    }

    /// Acquires tokens from the bucket. Blocks asynchronously if insufficient tokens are available (throttling backpressure).
    pub async fn acquire(&self, count: u32) {
        loop {
            let mut tokens = self.tokens.lock().await;
            let mut last_refill = self.last_refill.lock().await;

            let now = Instant::now();
            let elapsed_secs = now.duration_since(*last_refill).as_secs_f64();
            
            let new_tokens = *tokens + (elapsed_secs * self.refill_rate_per_sec);
            *tokens = new_tokens.min(self.capacity as f64);
            *last_refill = now;

            if *tokens >= (count as f64) {
                *tokens -= count as f64;
                break;
            }

            let missing_tokens = (count as f64) - *tokens;
            let wait_secs = missing_tokens / self.refill_rate_per_sec;
            drop(tokens);
            drop(last_refill);

            tokio::time::sleep(Duration::from_secs_f64(wait_secs.max(0.001))).await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_token_bucket_limiter() {
        let limiter = Arc::new(TokenBucketLimiter::new(10, 100.0));

        let start = Instant::now();
        limiter.acquire(5).await;
        limiter.acquire(5).await;
        
        let elapsed = start.elapsed();
        assert!(elapsed < Duration::from_millis(50));
    }
}

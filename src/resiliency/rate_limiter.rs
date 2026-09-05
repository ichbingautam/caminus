use std::collections::HashMap;
use std::sync::RwLock;
use std::time::{Duration, Instant};
use tokio::sync::Mutex;

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

    /// Tries to acquire tokens from the bucket without blocking. Returns true if acquired, false otherwise.
    pub async fn try_acquire(&self, count: u32) -> bool {
        let mut tokens = self.tokens.lock().await;
        let mut last_refill = self.last_refill.lock().await;

        let now = Instant::now();
        let elapsed_secs = now.duration_since(*last_refill).as_secs_f64();

        let new_tokens = *tokens + (elapsed_secs * self.refill_rate_per_sec);
        *tokens = new_tokens.min(self.capacity as f64);
        *last_refill = now;

        if *tokens >= (count as f64) {
            *tokens -= count as f64;
            true
        } else {
            false
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

pub struct TenantQuotaLimiter {
    tenant_limiters: RwLock<HashMap<String, TokenBucketLimiter>>,
    default_capacity: u32,
    default_refill_rate: f64,
}

impl TenantQuotaLimiter {
    pub fn new(default_capacity: u32, default_refill_rate: f64) -> Self {
        Self {
            tenant_limiters: RwLock::new(HashMap::new()),
            default_capacity,
            default_refill_rate,
        }
    }

    pub fn set_tenant_quota(
        &self,
        tenant_id: impl Into<String>,
        capacity: u32,
        refill_rate_per_sec: f64,
    ) {
        let mut lock = self.tenant_limiters.write().unwrap();
        lock.insert(
            tenant_id.into(),
            TokenBucketLimiter::new(capacity, refill_rate_per_sec),
        );
    }

    pub async fn try_allow(&self, tenant_id: &str) -> bool {
        let lock = self.tenant_limiters.read().unwrap();
        if let Some(limiter) = lock.get(tenant_id) {
            limiter.try_acquire(1).await
        } else {
            drop(lock);
            let mut write_lock = self.tenant_limiters.write().unwrap();
            let limiter = write_lock.entry(tenant_id.to_string()).or_insert_with(|| {
                TokenBucketLimiter::new(self.default_capacity, self.default_refill_rate)
            });
            limiter.try_acquire(1).await
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_token_bucket_limiter() {
        let limiter = TokenBucketLimiter::new(10, 100.0);

        let start = Instant::now();
        limiter.acquire(5).await;
        limiter.acquire(5).await;

        let elapsed = start.elapsed();
        assert!(elapsed < Duration::from_millis(50));
    }

    #[tokio::test]
    async fn test_tenant_quota_limiter() {
        let quota_limiter = TenantQuotaLimiter::new(2, 10.0);
        quota_limiter.set_tenant_quota("tenant-A", 1, 1.0);

        // tenant-A quota capacity is 1
        assert!(quota_limiter.try_allow("tenant-A").await);
        assert!(!quota_limiter.try_allow("tenant-A").await);

        // tenant-B default capacity is 2
        assert!(quota_limiter.try_allow("tenant-B").await);
        assert!(quota_limiter.try_allow("tenant-B").await);
        assert!(!quota_limiter.try_allow("tenant-B").await);
    }
}

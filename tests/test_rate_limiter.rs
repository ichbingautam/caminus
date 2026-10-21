use std::sync::Arc;
use std::time::Duration;
use tokio::time::sleep;

use caminus::security::rate_limiter::{
    RateLimitError, RateLimiterConfig, TenantRateLimiterRegistry, TokenBucket,
};

#[test]
fn test_token_bucket_initial_capacity_and_burst() {
    let config = RateLimiterConfig::new(10, 50).with_initial_tokens(50);
    let bucket = TokenBucket::new("test-client", config);

    assert_eq!(bucket.key(), "test-client");
    assert_eq!(bucket.config().burst_capacity, 50);

    // Initial capacity should permit burst of 50
    let permit = bucket.try_acquire(50).expect("should acquire 50 tokens");
    assert_eq!(permit.tokens_acquired, 50);
    assert!(permit.remaining_tokens < 1.0);

    // Immediate subsequent acquisition should fail
    let err = bucket.try_acquire(1).unwrap_err();
    match err {
        RateLimitError::RateLimitExceeded {
            key,
            retry_after,
            available_tokens,
            requested_tokens,
        } => {
            assert_eq!(key, "test-client");
            assert_eq!(requested_tokens, 1);
            assert!(available_tokens < 1.0);
            assert!(retry_after > Duration::ZERO);
        }
        _ => panic!("unexpected error variant: {:?}", err),
    }
}

#[tokio::test]
async fn test_token_bucket_refill_rate() {
    // 50 tokens per sec -> 1 token every 20ms
    let config = RateLimiterConfig::new(50, 10).with_initial_tokens(0);
    let bucket = TokenBucket::new("refill-client", config);

    // Empty bucket rejects
    assert!(bucket.try_acquire(1).is_err());

    // Sleep for 60ms -> should refill ~3 tokens
    sleep(Duration::from_millis(60)).await;

    let permit = bucket.try_acquire(2).expect("should acquire refilled tokens");
    assert_eq!(permit.tokens_acquired, 2);
}

#[tokio::test]
async fn test_token_bucket_concurrent_cas_contention() {
    // 1000 tokens initial, no replenishment for test duration
    let config = RateLimiterConfig::new(0, 1000).with_initial_tokens(1000);
    let bucket = Arc::new(TokenBucket::new("concurrency-client", config));

    let mut handles = Vec::new();
    let num_tasks = 20;
    let tokens_per_task = 50; // 20 * 50 = 1000 total

    for _ in 0..num_tasks {
        let bucket_clone = Arc::clone(&bucket);
        handles.push(tokio::spawn(async move {
            let mut acquired = 0;
            for _ in 0..tokens_per_task {
                if bucket_clone.try_acquire(1).is_ok() {
                    acquired += 1;
                }
            }
            acquired
        }));
    }

    let mut total_acquired = 0;
    for handle in handles {
        total_acquired += handle.await.unwrap();
    }

    // All 1000 tokens must be acquired with zero lost or duplicated tokens
    assert_eq!(total_acquired, 1000);
    // Bucket is now completely exhausted
    assert!(bucket.try_acquire(1).is_err());
}

#[tokio::test]
async fn test_tenant_registry_isolation_and_overrides() {
    let default_config = RateLimiterConfig::new(10, 20);
    let registry = TenantRateLimiterRegistry::new(default_config);

    // Tenant A uses default quota (20 burst)
    let permit_a = registry
        .try_acquire("tenant-a", 20)
        .expect("tenant-a should acquire 20");
    assert_eq!(permit_a.tokens_acquired, 20);
    assert!(registry.try_acquire("tenant-a", 1).is_err());

    // Tenant B is isolated and unaffected by Tenant A
    let permit_b = registry
        .try_acquire("tenant-b", 15)
        .expect("tenant-b should acquire 15 independently");
    assert_eq!(permit_b.tokens_acquired, 15);

    // Tenant C gets custom high-volume quota override (100 burst)
    registry.set_tenant_config("tenant-c", RateLimiterConfig::new(500, 100));
    let permit_c = registry
        .try_acquire("tenant-c", 100)
        .expect("tenant-c should acquire 100 tokens");
    assert_eq!(permit_c.tokens_acquired, 100);

    // Verify registry accounting
    assert_eq!(registry.tenant_count(), 3);
    assert!(registry.remove_tenant("tenant-a"));
    assert_eq!(registry.tenant_count(), 2);
    assert!(!registry.remove_tenant("tenant-a"));
}

#[test]
fn test_rate_limit_error_display() {
    let err = RateLimitError::RateLimitExceeded {
        key: "api-key-123".to_string(),
        retry_after: Duration::from_millis(250),
        available_tokens: 0.125,
        requested_tokens: 5,
    };

    let msg = format!("{}", err);
    assert!(msg.contains("api-key-123"));
    assert!(msg.contains("requested 5"));
    assert!(msg.contains("retry after 0.250s"));
}

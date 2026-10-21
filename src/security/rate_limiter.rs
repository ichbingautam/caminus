//! Dynamic token bucket rate limiting per client API key using atomic CAS operations.
//!
//! Provides thread-safe, lock-free token consumption, tenant-aware burst capacities,
//! and accurate `Retry-After` estimation upon quota exhaustion.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};

/// Fixed-point scaling factor (1,000 units = 1.0 token) for fractional token replenishment.
const TOKEN_SCALE: u64 = 1_000;

/// Configuration defining sustained replenishment rate and burst ceiling for a token bucket.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RateLimiterConfig {
    /// Sustained token replenishment rate per second.
    pub rate_per_sec: u32,
    /// Maximum burst capacity (maximum tokens allowed in the bucket).
    pub burst_capacity: u32,
    /// Initial token count on bucket creation (default equals burst_capacity).
    pub initial_tokens: u32,
}

impl RateLimiterConfig {
    /// Create a new configuration with given sustained rate and burst capacity.
    pub fn new(rate_per_sec: u32, burst_capacity: u32) -> Self {
        Self {
            rate_per_sec,
            burst_capacity,
            initial_tokens: burst_capacity,
        }
    }

    /// Set initial token level.
    pub fn with_initial_tokens(mut self, initial: u32) -> Self {
        self.initial_tokens = initial.min(self.burst_capacity);
        self
    }
}

impl Default for RateLimiterConfig {
    fn default() -> Self {
        Self {
            rate_per_sec: 100,
            burst_capacity: 200,
            initial_tokens: 200,
        }
    }
}

/// Information returned upon successful token acquisition.
#[derive(Debug, Clone, PartialEq)]
pub struct RateLimitPermit {
    /// Client API key or tenant identifier.
    pub key: String,
    /// Number of tokens consumed in this request.
    pub tokens_acquired: u32,
    /// Estimated remaining tokens in the bucket after acquisition.
    pub remaining_tokens: f64,
    /// Exact instant the permit was issued.
    pub acquired_at: Instant,
}

/// Errors returned when a rate limiting request cannot be fulfilled.
#[derive(Debug, Clone, PartialEq)]
pub enum RateLimitError {
    /// Quota exceeded for the specified API key or tenant.
    RateLimitExceeded {
        /// Client API key or tenant identifier.
        key: String,
        /// Recommended backoff duration before retrying (`Retry-After`).
        retry_after: Duration,
        /// Number of tokens currently available in the bucket.
        available_tokens: f64,
        /// Number of tokens requested.
        requested_tokens: u32,
    },
    /// Invalid rate limiter parameters specified.
    InvalidQuota {
        /// Detailed description of the configuration error.
        message: String,
    },
}

impl std::fmt::Display for RateLimitError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::RateLimitExceeded {
                key,
                retry_after,
                available_tokens,
                requested_tokens,
            } => {
                write!(
                    f,
                    "rate limit exceeded for key '{}': requested {} tokens but only {:.3} available; retry after {:.3}s",
                    key,
                    requested_tokens,
                    available_tokens,
                    retry_after.as_secs_f64()
                )
            }
            Self::InvalidQuota { message } => write!(f, "invalid rate limiter quota: {}", message),
        }
    }
}

impl std::error::Error for RateLimitError {}

/// A high-performance lock-free token bucket utilizing a 64-bit atomic CAS loop.
///
/// State packing in `AtomicU64`:
/// - High 32 bits: `last_refill_ms` (elapsed milliseconds relative to creation base instant).
/// - Low 32 bits: `tokens_fixed` (tokens scaled by `TOKEN_SCALE` = 1,000 for fractional precision).
#[derive(Debug)]
pub struct TokenBucket {
    key: String,
    config: RateLimiterConfig,
    base_instant: Instant,
    state: AtomicU64,
}

impl TokenBucket {
    /// Create a new token bucket with the given identifier and configuration.
    pub fn new(key: impl Into<String>, config: RateLimiterConfig) -> Self {
        let key = key.into();
        let base_instant = Instant::now();
        let initial_fixed = (config.initial_tokens as u64) * TOKEN_SCALE;
        // High 32 bits = 0 (0 ms elapsed at start), low 32 bits = initial_fixed
        let initial_state = initial_fixed & 0xFFFF_FFFF;

        Self {
            key,
            config,
            base_instant,
            state: AtomicU64::new(initial_state),
        }
    }

    /// Client identifier associated with this bucket.
    pub fn key(&self) -> &str {
        &self.key
    }

    /// Configuration parameters for this bucket.
    pub fn config(&self) -> &RateLimiterConfig {
        &self.config
    }

    /// Current available tokens in the bucket (calculating lazy replenishment without consuming).
    pub fn available_tokens(&self) -> f64 {
        let now_ms = (self.base_instant.elapsed().as_millis() as u64) as u32;
        let curr = self.state.load(Ordering::Acquire);
        let last_ms = (curr >> 32) as u32;
        let tokens_fixed = (curr & 0xFFFF_FFFF) as u64;

        let delta_ms = now_ms.saturating_sub(last_ms);
        let refilled_fixed =
            (delta_ms as u64 * (self.config.rate_per_sec as u64) * TOKEN_SCALE) / 1000;
        let max_fixed = (self.config.burst_capacity as u64) * TOKEN_SCALE;
        let total_fixed = (tokens_fixed + refilled_fixed).min(max_fixed);

        (total_fixed as f64) / (TOKEN_SCALE as f64)
    }

    /// Attempt to acquire `requested_tokens` using a lock-free atomic CAS loop.
    pub fn try_acquire(&self, requested_tokens: u32) -> Result<RateLimitPermit, RateLimitError> {
        if requested_tokens == 0 {
            return Ok(RateLimitPermit {
                key: self.key.clone(),
                tokens_acquired: 0,
                remaining_tokens: self.available_tokens(),
                acquired_at: Instant::now(),
            });
        }

        let req_fixed = (requested_tokens as u64) * TOKEN_SCALE;
        let max_fixed = (self.config.burst_capacity as u64) * TOKEN_SCALE;
        let rate_fixed_per_sec = (self.config.rate_per_sec as u64) * TOKEN_SCALE;

        loop {
            let now_ms = (self.base_instant.elapsed().as_millis() as u64) as u32;
            let curr = self.state.load(Ordering::Acquire);
            let last_ms = (curr >> 32) as u32;
            let current_tokens_fixed = (curr & 0xFFFF_FFFF) as u64;

            // Replenish tokens based on elapsed milliseconds
            let delta_ms = now_ms.saturating_sub(last_ms);
            let refilled_fixed =
                (delta_ms as u64 * (self.config.rate_per_sec as u64) * TOKEN_SCALE) / 1000;
            let new_tokens_fixed = (current_tokens_fixed + refilled_fixed).min(max_fixed);

            if new_tokens_fixed < req_fixed {
                // Insufficient tokens: compute required cooldown/backoff for Retry-After
                let needed_fixed = req_fixed - new_tokens_fixed;
                let wait_ms = if rate_fixed_per_sec > 0 {
                    (needed_fixed * 1000 + rate_fixed_per_sec - 1) / rate_fixed_per_sec
                } else {
                    1000
                };

                let available_tokens = (new_tokens_fixed as f64) / (TOKEN_SCALE as f64);
                return Err(RateLimitError::RateLimitExceeded {
                    key: self.key.clone(),
                    retry_after: Duration::from_millis(wait_ms.max(1)),
                    available_tokens,
                    requested_tokens,
                });
            }

            // Tokens sufficient: deduct requested amount and update timestamp atomically
            let remaining_fixed = new_tokens_fixed - req_fixed;
            let next_state = ((now_ms as u64) << 32) | (remaining_fixed & 0xFFFF_FFFF);

            match self.state.compare_exchange_weak(
                curr,
                next_state,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => {
                    return Ok(RateLimitPermit {
                        key: self.key.clone(),
                        tokens_acquired: requested_tokens,
                        remaining_tokens: (remaining_fixed as f64) / (TOKEN_SCALE as f64),
                        acquired_at: Instant::now(),
                    });
                }
                Err(_) => {
                    // Contention encountered: spin and retry CAS
                    std::hint::spin_loop();
                }
            }
        }
    }
}

/// Registry managing tenant-aware and API-key-specific token buckets.
#[derive(Debug)]
pub struct TenantRateLimiterRegistry {
    default_config: RateLimiterConfig,
    buckets: RwLock<HashMap<String, Arc<TokenBucket>>>,
    tenant_configs: RwLock<HashMap<String, RateLimiterConfig>>,
}

impl Default for TenantRateLimiterRegistry {
    fn default() -> Self {
        Self::new(RateLimiterConfig::default())
    }
}

impl TenantRateLimiterRegistry {
    /// Create a new registry with a fallback default rate limit configuration.
    pub fn new(default_config: RateLimiterConfig) -> Self {
        Self {
            default_config,
            buckets: RwLock::new(HashMap::new()),
            tenant_configs: RwLock::new(HashMap::new()),
        }
    }

    /// Set or override custom rate limit configuration for a specific tenant or API key.
    pub fn set_tenant_config(&self, key: impl Into<String>, config: RateLimiterConfig) {
        let key = key.into();
        {
            let mut configs = self.tenant_configs.write().unwrap();
            configs.insert(key.clone(), config);
        }
        // Invalidate or replace existing bucket so new config takes effect immediately
        let mut buckets = self.buckets.write().unwrap();
        buckets.insert(key.clone(), Arc::new(TokenBucket::new(key, config)));
    }

    /// Retrieve or instantiate the token bucket associated with a given client API key.
    pub fn get_or_create(&self, key: &str) -> Arc<TokenBucket> {
        {
            let buckets = self.buckets.read().unwrap();
            if let Some(bucket) = buckets.get(key) {
                return Arc::clone(bucket);
            }
        }

        let mut buckets = self.buckets.write().unwrap();
        if let Some(bucket) = buckets.get(key) {
            return Arc::clone(bucket);
        }

        let config = {
            let configs = self.tenant_configs.read().unwrap();
            configs.get(key).copied().unwrap_or(self.default_config)
        };

        let bucket = Arc::new(TokenBucket::new(key, config));
        buckets.insert(key.to_string(), Arc::clone(&bucket));
        bucket
    }

    /// Check and acquire 1 token for the specified client API key.
    pub fn try_acquire_one(&self, key: &str) -> Result<RateLimitPermit, RateLimitError> {
        self.try_acquire(key, 1)
    }

    /// Check and acquire `tokens` for the specified client API key.
    pub fn try_acquire(&self, key: &str, tokens: u32) -> Result<RateLimitPermit, RateLimitError> {
        let bucket = self.get_or_create(key);
        bucket.try_acquire(tokens)
    }

    /// Query available tokens for an API key.
    pub fn available_tokens(&self, key: &str) -> f64 {
        let bucket = self.get_or_create(key);
        bucket.available_tokens()
    }

    /// Return total active tenant buckets tracked in memory.
    pub fn tenant_count(&self) -> usize {
        let buckets = self.buckets.read().unwrap();
        buckets.len()
    }

    /// Remove a tenant bucket from the registry.
    pub fn remove_tenant(&self, key: &str) -> bool {
        let mut buckets = self.buckets.write().unwrap();
        buckets.remove(key).is_some()
    }

    /// Clear all tenant buckets.
    pub fn clear(&self) {
        let mut buckets = self.buckets.write().unwrap();
        buckets.clear();
    }
}

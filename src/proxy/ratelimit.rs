//! Per-tenant admission control: RPM, TPM, and a daily USD budget.
//!
//! `governor`'s GCRA cells model *requests* well, so RPM uses it directly. TPM
//! does not fit: GCRA has no cost-weighted cell, and fanning out one cell per
//! token is unworkable at 250k TPM. TPM therefore uses a small atomic token
//! bucket that refills continuously at `tpm/60s`, which also avoids the
//! double-burst artefact of a fixed one-minute window.
//!
//! # Budget gap
//!
//! `daily_budget_usd` is enforced against a counter that nothing currently
//! increments, because the config schema has no per-token price table. The
//! plumbing (`record_spend_usd`) is in place and exhaustion denies admission;
//! wiring a price source is a follow-up that needs a schema addition.

use std::num::NonZeroU32;
use std::sync::atomic::{AtomicU64, Ordering};

use dashmap::DashMap;
use governor::clock::DefaultClock;
use governor::state::{InMemoryState, NotKeyed};
use governor::{Quota, RateLimiter};
use http::HeaderMap;

use crate::config::{RateLimitConfig, RouterConfig};
use crate::error::RouterError;
use crate::state::now_ms;

/// Fixed-point scale for the token bucket, giving millitoken precision.
const TOKEN_SCALE: u64 = 1_000;

/// Micro-dollars per dollar, for atomic spend accounting.
const MICROS_PER_USD: f64 = 1_000_000.0;

type RpmLimiter = RateLimiter<NotKeyed, InMemoryState, DefaultClock>;

fn nonzero(value: u32) -> NonZeroU32 {
    NonZeroU32::new(value).unwrap_or(NonZeroU32::MIN)
}

/// A continuously-refilling token bucket for cost-weighted limits.
#[derive(Debug)]
struct TokenBucket {
    tokens_scaled: AtomicU64,
    last_refill_ms: AtomicU64,
    capacity_scaled: u64,
    refill_per_ms: u64,
}

impl TokenBucket {
    fn new(per_minute: u64, burst_multiplier: f64) -> Self {
        let per_minute = per_minute.max(1);
        let multiplier = if burst_multiplier.is_finite() && burst_multiplier >= 1.0 {
            burst_multiplier
        } else {
            1.0
        };

        let capacity = ((per_minute as f64) * multiplier).ceil().max(1.0) as u64;
        let capacity_scaled = capacity.saturating_mul(TOKEN_SCALE);

        // At least one millitoken per millisecond, so very small TPM limits
        // still make progress instead of stalling forever.
        let refill_per_ms = ((per_minute as f64 * TOKEN_SCALE as f64) / 60_000.0).ceil() as u64;

        Self {
            tokens_scaled: AtomicU64::new(capacity_scaled),
            last_refill_ms: AtomicU64::new(now_ms()),
            capacity_scaled,
            refill_per_ms: refill_per_ms.max(1),
        }
    }

    fn try_consume(&self, tokens: u64) -> bool {
        self.refill(now_ms());

        let cost = tokens.saturating_mul(TOKEN_SCALE);
        let mut current = self.tokens_scaled.load(Ordering::Relaxed);

        loop {
            if current < cost {
                return false;
            }

            match self.tokens_scaled.compare_exchange_weak(
                current,
                current - cost,
                Ordering::AcqRel,
                Ordering::Relaxed,
            ) {
                Ok(_) => return true,
                Err(observed) => current = observed,
            }
        }
    }

    fn refill(&self, now: u64) {
        let last = self.last_refill_ms.load(Ordering::Relaxed);
        if now <= last {
            return;
        }

        // Only one thread advances the clock; the rest read the fresher value.
        if self
            .last_refill_ms
            .compare_exchange(last, now, Ordering::AcqRel, Ordering::Relaxed)
            .is_err()
        {
            return;
        }

        let added = (now - last).saturating_mul(self.refill_per_ms);

        // Explicit CAS loop rather than `fetch_update`: the latter is deprecated
        // in favour of `try_update`, which does not exist at the declared MSRV.
        let mut current = self.tokens_scaled.load(Ordering::Relaxed);
        loop {
            let next = current.saturating_add(added).min(self.capacity_scaled);
            if next == current {
                break;
            }
            match self.tokens_scaled.compare_exchange_weak(
                current,
                next,
                Ordering::AcqRel,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(observed) => current = observed,
            }
        }
    }
}

/// All limiters for one tenant.
#[derive(Debug)]
struct TenantBuckets {
    rpm: RpmLimiter,
    tpm: TokenBucket,
}

impl TenantBuckets {
    fn new(limits: &RateLimitConfig) -> Self {
        let burst = ((limits.rpm as f64) * limits.burst_multiplier.max(1.0))
            .ceil()
            .max(1.0) as u32;

        let quota = Quota::per_minute(nonzero(limits.rpm)).allow_burst(nonzero(burst));

        Self {
            // `Quota` is built from `NonZeroU32`s, so the limiter is valid by
            // construction and cannot panic here.
            rpm: RateLimiter::direct(quota),
            tpm: TokenBucket::new(limits.tpm, limits.burst_multiplier),
        }
    }
}

/// Per-tenant admission controller.
#[derive(Debug, Default)]
pub struct TenantRateLimiter {
    tenants: DashMap<String, TenantBuckets>,
    /// Spend is tracked independently of quota buckets: a tenant can be charged
    /// (or have a budget enforced) before any limiter row exists for it.
    spend: DashMap<String, AtomicU64>,
}

impl TenantRateLimiter {
    /// Creates an empty limiter.
    pub fn new() -> Self {
        Self::default()
    }

    /// Drops all bucket state. Called on config reload, since quotas may change.
    ///
    /// Spend survives: resetting quotas must not forgive a tenant's daily budget.
    pub fn reset(&self) {
        self.tenants.clear();
    }

    /// Admits or rejects one request for `tenant`.
    ///
    /// Checks RPM first (cheapest and most common limit), then reserves prompt
    /// tokens, then verifies the daily budget.
    ///
    /// # Errors
    ///
    /// Returns [`RouterError::RateLimitExceeded`] when any limit is exhausted.
    pub fn enforce(
        &self,
        tenant: &str,
        limits: &RateLimitConfig,
        estimated_tokens: usize,
        estimated_cost_usd: f64,
    ) -> Result<(), RouterError> {
        let entry = self
            .tenants
            .entry(tenant.to_string())
            .or_insert_with(|| TenantBuckets::new(limits));

        if entry.rpm.check().is_err() {
            return Err(RouterError::RateLimitExceeded {
                tenant: tenant.to_string(),
                detail: format!("requests per minute limit of {} exceeded", limits.rpm),
            });
        }

        if !entry.tpm.try_consume(estimated_tokens as u64) {
            return Err(RouterError::RateLimitExceeded {
                tenant: tenant.to_string(),
                detail: format!(
                    "tokens per minute limit of {} exceeded by a request estimated at {} tokens",
                    limits.tpm, estimated_tokens
                ),
            });
        }

        let budget = usd_to_micros(limits.daily_budget_usd);
        let spent = self.spent_micros(tenant);
        // Admission reserves the estimate so a burst cannot overrun the budget
        // by one request's worth before any spend is recorded. An already
        // exhausted budget denies even a zero-cost request, otherwise unpriced
        // traffic would sail past a spent budget forever.
        if spent >= budget || spent.saturating_add(usd_to_micros(estimated_cost_usd)) > budget {
            return Err(RouterError::RateLimitExceeded {
                tenant: tenant.to_string(),
                detail: format!(
                    "daily budget of ${:.2} exhausted (spent ${:.2})",
                    limits.daily_budget_usd,
                    spent as f64 / MICROS_PER_USD
                ),
            });
        }

        Ok(())
    }

    /// Records realised spend for a tenant, whether or not it has limits.
    pub fn record_spend_usd(&self, tenant: &str, usd: f64) {
        let micros = usd_to_micros(usd);
        if micros == 0 {
            return;
        }

        let entry = self
            .spend
            .entry(tenant.to_string())
            .or_insert_with(|| AtomicU64::new(0));

        entry.fetch_add(micros, Ordering::Relaxed);
    }

    /// Spend observed so far for a tenant, in US dollars.
    pub fn spent_usd(&self, tenant: &str) -> f64 {
        self.spent_micros(tenant) as f64 / MICROS_PER_USD
    }

    fn spent_micros(&self, tenant: &str) -> u64 {
        self.spend
            .get(tenant)
            .map(|entry| entry.load(Ordering::Relaxed))
            .unwrap_or(0)
    }
}

fn usd_to_micros(usd: f64) -> u64 {
    if !usd.is_finite() || usd <= 0.0 {
        return 0;
    }
    (usd * MICROS_PER_USD).round() as u64
}

/// Resolves which configured limit set applies to a request.
///
/// The tenant identity is the value of the configured match header, so one
/// `[rate_limits.<name>]` block serves every customer. When no configured
/// header is present, the `default_tenant` block applies under the name
/// `anonymous`.
pub fn resolve_tenant<'a>(
    config: &'a RouterConfig,
    headers: &HeaderMap,
) -> Option<(String, &'a RateLimitConfig)> {
    for limits in config.rate_limits.values() {
        let value = headers
            .get(limits.match_header.as_str())
            .and_then(|value| value.to_str().ok())
            .map(str::trim)
            .filter(|value| !value.is_empty());

        if let Some(value) = value {
            return Some((value.to_string(), limits));
        }
    }

    config
        .rate_limits
        .get("default_tenant")
        .map(|limits| ("anonymous".to_string(), limits))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::RateLimitConfig;

    fn limits(rpm: u32, tpm: u64) -> RateLimitConfig {
        RateLimitConfig {
            match_header: "X-Tenant-ID".to_string(),
            rpm,
            tpm,
            daily_budget_usd: 100.0,
            burst_multiplier: 1.0,
        }
    }

    #[test]
    fn rpm_admits_up_to_the_quota_then_rejects() {
        let limiter = TenantRateLimiter::new();
        let limits = limits(3, 1_000_000);

        for _ in 0..3 {
            assert!(limiter.enforce("acme", &limits, 1, 0.0).is_ok());
        }

        let error = limiter
            .enforce("acme", &limits, 1, 0.0)
            .expect_err("fourth request exceeds rpm");
        assert!(error.to_string().contains("requests per minute"));
    }

    #[test]
    fn tpm_rejects_a_single_oversized_request() {
        let limiter = TenantRateLimiter::new();
        let limits = limits(1_000, 100);

        let error = limiter
            .enforce("acme", &limits, 500, 0.0)
            .expect_err("500 tokens exceeds a 100 token budget");
        assert!(error.to_string().contains("tokens per minute"));
    }

    #[test]
    fn tpm_accumulates_across_requests() {
        let limiter = TenantRateLimiter::new();
        let limits = limits(1_000, 100);

        assert!(limiter.enforce("acme", &limits, 60, 0.0).is_ok());
        // The bucket refills at ~1.6 tokens/ms, so this is effectively immediate.
        let error = limiter
            .enforce("acme", &limits, 60, 0.0)
            .expect_err("second request overdraws the bucket");
        assert!(error.to_string().contains("tokens per minute"));
    }

    #[test]
    fn tenants_have_independent_buckets() {
        let limiter = TenantRateLimiter::new();
        let limits = limits(1, 100);

        assert!(limiter.enforce("acme", &limits, 1, 0.0).is_ok());
        assert!(limiter.enforce("globex", &limits, 1, 0.0).is_ok());
        assert!(limiter.enforce("acme", &limits, 1, 0.0).is_err());
    }

    #[test]
    fn exhausted_budget_denies_admission() {
        let limiter = TenantRateLimiter::new();
        let mut limits = limits(1_000, 1_000_000);
        limits.daily_budget_usd = 1.0;

        assert!(limiter.enforce("acme", &limits, 1, 0.0).is_ok());
        limiter.record_spend_usd("acme", 1.0);
        assert!((limiter.spent_usd("acme") - 1.0).abs() < 1e-9);

        let error = limiter
            .enforce("acme", &limits, 1, 0.0)
            .expect_err("budget is spent");
        assert!(error.to_string().contains("daily budget"));
    }

    #[test]
    fn reset_clears_quota_state() {
        let limiter = TenantRateLimiter::new();
        let limits = limits(1, 100);

        assert!(limiter.enforce("acme", &limits, 1, 0.0).is_ok());
        assert!(limiter.enforce("acme", &limits, 1, 0.0).is_err());

        limiter.reset();

        assert!(limiter.enforce("acme", &limits, 1, 0.0).is_ok());
    }

    #[test]
    fn resolve_tenant_prefers_the_match_header() {
        let mut config = RouterConfig::default();
        config
            .rate_limits
            .insert("default_tenant".to_string(), limits(10, 100));

        let mut headers = HeaderMap::new();
        headers.insert("x-tenant-id", "acme".parse().expect("header"));

        let (tenant, resolved) = resolve_tenant(&config, &headers).expect("resolves");
        assert_eq!(tenant, "acme");
        assert_eq!(resolved.rpm, 10);
    }

    #[test]
    fn resolve_tenant_falls_back_to_the_default_block() {
        let mut config = RouterConfig::default();
        config
            .rate_limits
            .insert("default_tenant".to_string(), limits(10, 100));

        let (tenant, _) = resolve_tenant(&config, &HeaderMap::new()).expect("resolves");
        assert_eq!(tenant, "anonymous");
    }

    #[test]
    fn resolve_tenant_returns_none_without_configuration() {
        assert!(resolve_tenant(&RouterConfig::default(), &HeaderMap::new()).is_none());
    }

    #[test]
    fn bucket_caps_refill_at_capacity() {
        let bucket = TokenBucket::new(60, 1.0);
        // Simulate a long idle period; refill must saturate, not overflow.
        bucket.refill(now_ms() + 10_000_000);

        assert!(bucket.try_consume(60));
        assert!(!bucket.try_consume(1_000));
    }
}

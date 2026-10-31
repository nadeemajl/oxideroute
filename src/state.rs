//! Shared application state.
//!
//! Three hot-path structures, all designed for wait-free reads:
//!
//! * [`RouterConfig`] lives behind an [`ArcSwap`], so a request grabs an `Arc`
//!   in a few nanoseconds and hot reloads never disturb in-flight streams.
//! * The provider registry and the per-upstream health table are [`DashMap`]s,
//!   so lookups do not serialise behind a global lock.
//! * Health counters are plain atomics; the EWMA update is a compare-exchange
//!   loop rather than a lock.

use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use arc_swap::ArcSwap;
use dashmap::DashMap;

use crate::config::RouterConfig;
use crate::error::RouterError;
use crate::providers::{build_provider, LlmProvider};
use crate::proxy::ratelimit::TenantRateLimiter;

/// Current wall-clock time in milliseconds since the Unix epoch.
pub fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|elapsed| elapsed.as_millis() as u64)
        .unwrap_or(0)
}

/// Lock-free health telemetry for one upstream (`provider/model`).
#[derive(Debug, Default)]
pub struct UpstreamHealthMetrics {
    /// Exponentially weighted moving average of time-to-first-token, in µs.
    ewma_ttft_us: AtomicU64,
    /// Failures since the last success.
    consecutive_failures: AtomicU32,
    /// Epoch milliseconds at which the circuit breaker may be probed again.
    circuit_open_until_ms: AtomicU64,
    /// Requests routed to this upstream.
    total_requests: AtomicU64,
    /// Requests that failed at this upstream.
    total_failures: AtomicU64,
}

impl UpstreamHealthMetrics {
    /// Creates a fresh, healthy metric set.
    pub fn new() -> Self {
        Self::default()
    }

    /// Current EWMA TTFT in microseconds; `0` means "never observed".
    pub fn ewma_ttft_us(&self) -> u64 {
        self.ewma_ttft_us.load(Ordering::Relaxed)
    }

    /// Failures since the last success.
    pub fn consecutive_failures(&self) -> u32 {
        self.consecutive_failures.load(Ordering::Relaxed)
    }

    /// Total requests routed here.
    pub fn total_requests(&self) -> u64 {
        self.total_requests.load(Ordering::Relaxed)
    }

    /// Total failures observed here.
    pub fn total_failures(&self) -> u64 {
        self.total_failures.load(Ordering::Relaxed)
    }

    /// Whether the circuit breaker currently admits traffic.
    ///
    /// Once `circuit_open_until_ms` has passed the upstream is half-open: the
    /// next request is allowed through as a probe.
    pub fn is_healthy(&self, at_ms: u64) -> bool {
        at_ms >= self.circuit_open_until_ms.load(Ordering::Relaxed)
    }

    /// Records a successful first-token delivery and folds it into the EWMA.
    ///
    /// `EWMA_new = Δt` when nothing has been observed yet, otherwise
    /// `floor(0.8·EWMA_old + 0.2·Δt)`, per `docs/ARCHITECTURE.md`.
    pub fn record_success(&self, ttft: Duration) {
        // Sub-microsecond TTFTs would otherwise leave the EWMA at its
        // "unknown" sentinel of zero.
        let sample = (ttft.as_micros() as u64).max(1);

        let mut current = self.ewma_ttft_us.load(Ordering::Relaxed);
        loop {
            let next = if current == 0 {
                sample
            } else {
                current
                    .saturating_mul(8)
                    .saturating_add(sample.saturating_mul(2))
                    / 10
            };

            match self.ewma_ttft_us.compare_exchange_weak(
                current,
                next,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(observed) => current = observed,
            }
        }

        self.consecutive_failures.store(0, Ordering::Relaxed);
        self.circuit_open_until_ms.store(0, Ordering::Relaxed);
        self.total_requests.fetch_add(1, Ordering::Relaxed);
    }

    /// Records a success that carries no latency observation (non-streaming).
    pub fn record_healthy(&self) {
        self.consecutive_failures.store(0, Ordering::Relaxed);
        self.circuit_open_until_ms.store(0, Ordering::Relaxed);
        self.total_requests.fetch_add(1, Ordering::Relaxed);
    }

    /// Records a failure, tripping the breaker at `threshold` consecutive misses.
    pub fn record_failure(&self, threshold: u32, cooldown: Duration) {
        self.total_requests.fetch_add(1, Ordering::Relaxed);
        self.total_failures.fetch_add(1, Ordering::Relaxed);

        let failures = self.consecutive_failures.fetch_add(1, Ordering::Relaxed) + 1;
        if failures >= threshold.max(1) {
            let open_until = now_ms().saturating_add(cooldown.as_millis() as u64);
            self.circuit_open_until_ms
                .store(open_until, Ordering::Relaxed);
        }
    }

    /// Clears all observations, used when an operator resets the breaker.
    pub fn reset(&self) {
        self.ewma_ttft_us.store(0, Ordering::Relaxed);
        self.consecutive_failures.store(0, Ordering::Relaxed);
        self.circuit_open_until_ms.store(0, Ordering::Relaxed);
    }
}

/// Process-wide state shared by every request handler.
pub struct AppState {
    config: ArcSwap<RouterConfig>,
    providers: DashMap<String, Arc<dyn LlmProvider>>,
    health: DashMap<String, Arc<UpstreamHealthMetrics>>,
    rate_limiter: TenantRateLimiter,
    started_at: Instant,
}

impl AppState {
    /// Builds state from a validated config, instantiating every provider.
    ///
    /// # Errors
    ///
    /// Fails if any provider adapter cannot be constructed; a partially built
    /// registry is never published.
    pub fn new(config: RouterConfig) -> Result<Self, RouterError> {
        let providers = DashMap::new();
        for (name, provider_config) in &config.providers {
            providers.insert(name.clone(), build_provider(name, provider_config)?);
        }

        Ok(Self {
            config: ArcSwap::from_pointee(config),
            providers,
            health: DashMap::new(),
            rate_limiter: TenantRateLimiter::new(),
            started_at: Instant::now(),
        })
    }

    /// Wait-free snapshot of the active configuration.
    ///
    /// Callers keep this `Arc` for the lifetime of a request so a concurrent
    /// reload cannot change the rules mid-flight.
    pub fn config(&self) -> Arc<RouterConfig> {
        self.config.load_full()
    }

    /// Resolves a provider by config key.
    pub fn provider(&self, name: &str) -> Option<Arc<dyn LlmProvider>> {
        self.providers
            .get(name)
            .map(|entry| Arc::clone(entry.value()))
    }

    /// Health metrics for a `provider/model` reference, creating them on first use.
    pub fn health_for(&self, reference: &str) -> Arc<UpstreamHealthMetrics> {
        if let Some(existing) = self.health.get(reference) {
            return Arc::clone(existing.value());
        }

        let metrics = Arc::new(UpstreamHealthMetrics::new());
        self.health
            .entry(reference.to_string())
            .or_insert_with(|| Arc::clone(&metrics))
            .value()
            .clone()
    }

    /// Read-only view of the health table, used by the routing engine.
    pub fn health(&self) -> &DashMap<String, Arc<UpstreamHealthMetrics>> {
        &self.health
    }

    /// Per-tenant rate limiters and spend trackers.
    pub fn rate_limiter(&self) -> &TenantRateLimiter {
        &self.rate_limiter
    }

    /// Time since the process started serving.
    pub fn uptime(&self) -> Duration {
        self.started_at.elapsed()
    }

    /// Validates and adopts a new configuration.
    ///
    /// Provider adapters are constructed *before* the config is published, so a
    /// reload can never expose a config whose providers failed to build. The
    /// swap itself is a single atomic store: in-flight streams keep their
    /// previously cloned `Arc<RouterConfig>` and finish undisturbed.
    ///
    /// # Errors
    ///
    /// Returns an error and leaves the active configuration untouched.
    pub fn apply_config(&self, config: RouterConfig) -> Result<(), RouterError> {
        let mut built = Vec::with_capacity(config.providers.len());
        for (name, provider_config) in &config.providers {
            built.push((name.clone(), build_provider(name, provider_config)?));
        }

        for (name, provider) in built {
            self.providers.insert(name, provider);
        }

        // Quotas may have changed; bucket state is not carried across reloads.
        self.rate_limiter.reset();

        self.config.store(Arc::new(config));
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{ProviderConfig, ProviderKind, RouterConfig};

    fn config_with_openai_compatible() -> RouterConfig {
        let mut config = RouterConfig::default();
        config.providers.insert(
            "local".to_string(),
            ProviderConfig {
                kind: ProviderKind::OpenaiCompatible,
                api_key: None,
                base_url: Some("http://127.0.0.1:11434/v1".to_string()),
                timeout_ms: 1_000,
                ttft_timeout_ms: 500,
            },
        );
        config
    }

    #[test]
    fn ewma_starts_at_the_first_sample() {
        let metrics = UpstreamHealthMetrics::new();

        metrics.record_success(Duration::from_micros(1_000));
        assert_eq!(metrics.ewma_ttft_us(), 1_000);
    }

    #[test]
    fn ewma_weights_the_new_sample_at_twenty_percent() {
        let metrics = UpstreamHealthMetrics::new();

        metrics.record_success(Duration::from_micros(1_000));
        metrics.record_success(Duration::from_micros(2_000));

        // floor(0.8 * 1000 + 0.2 * 2000) = 1200
        assert_eq!(metrics.ewma_ttft_us(), 1_200);
    }

    #[test]
    fn sub_microsecond_ttft_does_not_stay_at_the_sentinel() {
        let metrics = UpstreamHealthMetrics::new();
        metrics.record_success(Duration::from_nanos(10));

        assert_eq!(metrics.ewma_ttft_us(), 1);
    }

    #[test]
    fn breaker_opens_after_the_threshold_and_closes_after_cooldown() {
        let metrics = UpstreamHealthMetrics::new();
        let now = now_ms();

        for _ in 0..3 {
            metrics.record_failure(3, Duration::from_millis(50));
        }

        assert!(!metrics.is_healthy(now));
        assert_eq!(metrics.consecutive_failures(), 3);
        assert!(metrics.is_healthy(now + 10_000), "cooldown must expire");
    }

    #[test]
    fn success_resets_failures_and_closes_the_breaker() {
        let metrics = UpstreamHealthMetrics::new();
        metrics.record_failure(2, Duration::from_millis(60_000));
        metrics.record_failure(2, Duration::from_millis(60_000));
        assert!(!metrics.is_healthy(now_ms()));

        metrics.record_success(Duration::from_millis(5));

        assert_eq!(metrics.consecutive_failures(), 0);
        assert!(metrics.is_healthy(now_ms()));
    }

    #[test]
    fn counters_track_requests_and_failures() {
        let metrics = UpstreamHealthMetrics::new();

        metrics.record_success(Duration::from_millis(1));
        metrics.record_failure(10, Duration::from_secs(1));

        assert_eq!(metrics.total_requests(), 2);
        assert_eq!(metrics.total_failures(), 1);
    }

    #[test]
    fn state_builds_and_resolves_providers() {
        let state = AppState::new(config_with_openai_compatible()).expect("state builds");

        assert!(state.provider("local").is_some());
        assert!(state.provider("missing").is_none());
        assert_eq!(state.config().providers.len(), 1);
    }

    #[test]
    fn health_entries_are_shared_per_reference() {
        let state = AppState::new(config_with_openai_compatible()).expect("state builds");

        let first = state.health_for("local/model-a");
        first.record_success(Duration::from_micros(700));
        let second = state.health_for("local/model-a");

        assert_eq!(second.ewma_ttft_us(), 700);
        assert_eq!(state.health().len(), 1);
    }

    #[test]
    fn apply_config_swaps_atomically() {
        let state = AppState::new(config_with_openai_compatible()).expect("state builds");
        let before = state.config();

        let mut updated = config_with_openai_compatible();
        updated.listen = "127.0.0.1:9999".to_string();
        state.apply_config(updated).expect("reload succeeds");

        assert_eq!(state.config().listen, "127.0.0.1:9999");
        // The previously handed-out Arc still sees the old config.
        assert_eq!(before.listen, "0.0.0.0:8080");
    }

    #[test]
    fn failed_reload_keeps_the_previous_config() {
        let state = AppState::new(config_with_openai_compatible()).expect("state builds");
        let before = state.config();

        let mut broken = config_with_openai_compatible();
        broken.listen = "127.0.0.1:9999".to_string();
        broken.providers.insert(
            "bedrock".to_string(),
            ProviderConfig {
                kind: ProviderKind::Bedrock,
                api_key: None,
                base_url: None,
                timeout_ms: 1_000,
                ttft_timeout_ms: 500,
            },
        );

        assert!(state.apply_config(broken).is_err());
        assert_eq!(state.config().listen, before.listen);
    }
}

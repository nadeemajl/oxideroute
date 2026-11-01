//! Prometheus metrics exposed on the scrape endpoint.
//!
//! Recording goes through the `metrics` facade, so the hot path stays a no-op
//! (a relaxed atomic increment) when no recorder is installed — which is what
//! keeps the unit tests free of global state.
//!
//! The first four families are the ones named in
//! `docs/IMPLEMENTATION_PLAN.md`. The next four exist because the stress plan
//! (`docs/STRESS_TEST_PLAN.md` §3.1) cannot answer "did the router throttle
//! this, or the upstream?" without them:
//!
//! | Family | Question it answers |
//! |---|---|
//! | [`RATE_LIMITED_TOTAL`] | did *we* return the `429`, and for which limit? |
//! | [`INFLIGHT_REQUESTS`] | how saturated is the ingress right now? |
//! | [`RELOAD_TOTAL`] | did the last config reload land, and from where? |
//! | [`BREAKER_OPEN`] | which upstreams are ejected at this moment? |

use std::collections::HashSet;
use std::sync::{Arc, Mutex};

use metrics::{counter, gauge, histogram};
use metrics_exporter_prometheus::{PrometheusBuilder, PrometheusHandle};

use crate::error::RouterError;

/// Time-to-first-token histogram, labelled by provider and model.
pub const TTFT_SECONDS: &str = "oxide_ttft_seconds";
/// Request counter, labelled by provider, model, and outcome.
pub const REQUESTS_TOTAL: &str = "oxide_requests_total";
/// Failover counter, labelled by the from/to upstreams and the reason.
pub const FALLBACKS_TOTAL: &str = "oxide_fallback_triggered_total";
/// Token counter, labelled by tenant, provider, and token kind.
pub const TOKENS_TOTAL: &str = "oxide_tokens_total";
/// Admission-rejection counter, labelled by tenant and limiting dimension.
///
/// `reason` is `rpm`, `tpm`, or `budget`. A rejection here is one *we* produced,
/// which distinguishes it from an upstream returning its own `429`.
pub const RATE_LIMITED_TOTAL: &str = "oxide_rate_limited_total";
/// Gauge of requests currently being served, including open stream bodies.
pub const INFLIGHT_REQUESTS: &str = "oxide_inflight_requests";
/// Config-reload counter, labelled by `result` (`success`/`rejected`) and
/// `source` (`file`, `signal`, `manual`).
pub const RELOAD_TOTAL: &str = "oxide_reload_total";
/// Per-upstream circuit-breaker gauge: `1` tripped, `0` closed.
///
/// Sampled from the health table on each scrape, so it reflects the cooldown
/// expiring even when no traffic reaches that upstream.
pub const BREAKER_OPEN: &str = "oxide_breaker_open";

/// The installed recorder plus the breaker series it has published.
struct Installed {
    handle: PrometheusHandle,
    seen_breakers: Arc<Mutex<HashSet<String>>>,
}

/// Holds the single installed recorder so repeated `install` calls are harmless
/// (the CLI and tests both reach for one).
static INSTALLED: Mutex<Option<Installed>> = Mutex::new(None);

/// Handle to the process-wide metric registry.
#[derive(Clone)]
pub struct Metrics {
    handle: PrometheusHandle,
    /// Breaker series already emitted, so a provider removed from the config can
    /// be reset to `0` instead of going stale at `1` forever.
    seen_breakers: Arc<Mutex<HashSet<String>>>,
}

impl Metrics {
    /// Installs the global Prometheus recorder, or returns the existing handle.
    ///
    /// # Errors
    ///
    /// Fails only if a *different* recorder already owns the global slot.
    pub fn install() -> Result<Self, RouterError> {
        let mut installed = match INSTALLED.lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        };

        if let Some(installed) = installed.as_ref() {
            return Ok(Self {
                handle: installed.handle.clone(),
                seen_breakers: Arc::clone(&installed.seen_breakers),
            });
        }

        let recorder = PrometheusBuilder::new().build_recorder();
        let handle = recorder.handle();

        metrics::set_global_recorder(recorder).map_err(|error| RouterError::Internal {
            message: format!("failed to install the metrics recorder: {error}"),
        })?;

        let seen_breakers = Arc::new(Mutex::new(HashSet::new()));
        *installed = Some(Installed {
            handle: handle.clone(),
            seen_breakers: Arc::clone(&seen_breakers),
        });

        Ok(Self {
            handle,
            seen_breakers,
        })
    }

    /// Renders the exposition payload, bringing histograms up to date first.
    pub fn render(&self) -> String {
        // Histograms are aggregated in a background upkeep pass in normal
        // operation; doing it here means a scrape reflects the latest samples
        // instead of the last periodic flush.
        self.handle.run_upkeep();
        self.handle.render()
    }

    /// Republishes [`BREAKER_OPEN`] from the caller's health table.
    ///
    /// A breaker is a *state*, not an event, so it is exported as a gauge that is
    /// refreshed at scrape time rather than mutated from the request path. That
    /// keeps the hot path free of metric bookkeeping and means a cooldown that
    /// expires between requests still shows as closed.
    ///
    /// Series for upstreams that are no longer reported are reset to `0` rather
    /// than left at their last value: an upstream removed by a reload must not
    /// look ejected forever.
    pub fn sync_breakers(&self, observed: impl IntoIterator<Item = (String, bool)>) {
        let mut current = HashSet::new();

        for (upstream, open) in observed {
            gauge!(BREAKER_OPEN, "upstream" => upstream.clone()).set(if open { 1.0 } else { 0.0 });
            current.insert(upstream);
        }

        let mut previous = match self.seen_breakers.lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        };

        for departed in previous.difference(&current) {
            gauge!(BREAKER_OPEN, "upstream" => departed.clone()).set(0.0);
        }

        *previous = current;
    }

    /// Publishes the gauge families at their idle value before any traffic.
    ///
    /// A registry with no samples renders an empty page, so a freshly started
    /// process looks indistinguishable from a broken one. Seeding gives the first
    /// scrape a healthy zero for every configured upstream.
    pub fn seed_idle(&self, upstreams: impl IntoIterator<Item = String>) {
        gauge!(INFLIGHT_REQUESTS).set(0.0);
        self.sync_breakers(upstreams.into_iter().map(|upstream| (upstream, false)));
    }
}

/// Guard that keeps [`INFLIGHT_REQUESTS`] accurate for as long as a request is
/// being served.
///
/// Drop it when the response body finishes streaming, not when the handler
/// returns, or concurrent streams are undercounted.
#[derive(Debug)]
pub struct InflightGuard {
    _private: (),
}

impl InflightGuard {
    /// Counts one request as in flight until the guard is dropped.
    pub fn new() -> Self {
        gauge!(INFLIGHT_REQUESTS).increment(1.0);
        Self { _private: () }
    }
}

impl Drop for InflightGuard {
    fn drop(&mut self) {
        gauge!(INFLIGHT_REQUESTS).decrement(1.0);
    }
}

impl Default for InflightGuard {
    fn default() -> Self {
        Self::new()
    }
}

/// Records an observed time-to-first-token, in seconds.
pub fn record_ttft(provider: &str, model: &str, seconds: f64) {
    histogram!(
        TTFT_SECONDS,
        "provider" => provider.to_string(),
        "model" => model.to_string(),
    )
    .record(seconds);
}

/// Records one request outcome for an upstream.
///
/// `status` is deliberately a small vocabulary (`success`, `error`) rather than
/// an HTTP code: the upstream status is already visible in the attempt log.
pub fn record_request(provider: &str, model: &str, status: &str) {
    counter!(
        REQUESTS_TOTAL,
        "provider" => provider.to_string(),
        "model" => model.to_string(),
        "status" => status.to_string(),
    )
    .increment(1);
}

/// Records a failover from one upstream to the next.
pub fn record_fallback(from_provider: &str, to_provider: &str, reason: &str) {
    counter!(
        FALLBACKS_TOTAL,
        "from_provider" => from_provider.to_string(),
        "to_provider" => to_provider.to_string(),
        "reason" => reason.to_string(),
    )
    .increment(1);
}

/// Records token consumption.
///
/// `token_type` is `prompt` or `completion`.
pub fn record_tokens(tenant: &str, provider: &str, token_type: &str, count: u64) {
    counter!(
        TOKENS_TOTAL,
        "tenant" => tenant.to_string(),
        "provider" => provider.to_string(),
        "token_type" => token_type.to_string(),
    )
    .increment(count);
}

/// Records an admission rejection *we* produced.
///
/// `reason` is `rpm`, `tpm`, or `budget`. Without this the scrape cannot tell a
/// router-imposed throttle from an upstream's own `429`.
pub fn record_rate_limited(tenant: &str, reason: &str) {
    counter!(
        RATE_LIMITED_TOTAL,
        "tenant" => tenant.to_string(),
        "reason" => reason.to_string(),
    )
    .increment(1);
}

/// Records one reload attempt.
///
/// `result` is `success` or `rejected`; `source` is `file`, `signal`, or
/// `manual`. A rejected reload is not an error page — the active config keeps
/// serving — so it needs to be visible here rather than only in the log.
pub fn record_reload(result: &str, source: &str) {
    counter!(
        RELOAD_TOTAL,
        "result" => result.to_string(),
        "source" => source.to_string(),
    )
    .increment(1);
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Renders under a throwaway recorder so tests never share global state.
    fn render_with(record: impl FnOnce()) -> String {
        let recorder = PrometheusBuilder::new().build_recorder();
        let handle = recorder.handle();

        metrics::with_local_recorder(&recorder, record);

        handle.run_upkeep();
        handle.render()
    }

    /// Extracts a gauge's value from an exposition payload.
    fn gauge_value(rendered: &str, series: &str) -> Option<f64> {
        rendered
            .lines()
            .find(|line| line.starts_with(series))
            .and_then(|line| line.rsplit(' ').next())
            .and_then(|value| value.parse().ok())
    }

    #[test]
    fn ttft_is_exported_with_provider_and_model_labels() {
        let rendered = render_with(|| record_ttft("openai_us", "gpt-4o", 0.25));

        assert!(rendered.contains(TTFT_SECONDS), "{rendered}");
        assert!(rendered.contains("provider=\"openai_us\""), "{rendered}");
        assert!(rendered.contains("model=\"gpt-4o\""), "{rendered}");
    }

    #[test]
    fn request_counter_is_exported() {
        let rendered = render_with(|| {
            record_request("anthropic_prod", "claude-3-7", "success");
        });

        assert!(rendered.contains(REQUESTS_TOTAL), "{rendered}");
        assert!(rendered.contains("status=\"success\""), "{rendered}");
    }

    #[test]
    fn fallback_counter_is_exported_with_both_sides() {
        let rendered = render_with(|| {
            record_fallback(
                "openai_us/gpt-4o",
                "anthropic_prod/claude",
                "http_status_429",
            );
        });

        assert!(rendered.contains(FALLBACKS_TOTAL), "{rendered}");
        assert!(rendered.contains("from_provider=\"openai_us/gpt-4o\""));
        assert!(rendered.contains("to_provider=\"anthropic_prod/claude\""));
        assert!(rendered.contains("reason=\"http_status_429\""));
    }

    #[test]
    fn token_counter_accumulates() {
        let rendered = render_with(|| {
            record_tokens("acme", "openai_us", "prompt", 10);
            record_tokens("acme", "openai_us", "prompt", 5);
        });

        assert!(rendered.contains(TOKENS_TOTAL), "{rendered}");
        assert!(rendered.contains("token_type=\"prompt\""), "{rendered}");
        assert!(rendered.contains("tenant=\"acme\""), "{rendered}");
        // 10 + 5 tokens on one series.
        assert!(
            rendered.contains(&format!("{TOKENS_TOTAL}{{")),
            "expected a series: {rendered}"
        );
    }

    #[test]
    fn install_yields_a_usable_handle() {
        let metrics = Metrics::install().expect("installs once");
        record_request("test_install", "m", "success");

        let rendered = metrics.render();
        assert!(rendered.contains(REQUESTS_TOTAL));
    }

    #[test]
    fn second_install_reuses_the_recorder() {
        let first = Metrics::install().expect("installs");
        let second = Metrics::install().expect("reuses");

        record_request("reuse_check", "m", "success");

        // A sample recorded once is visible through both handles, which is what
        // "same recorder" means. Comparing entire renderings would be racy:
        // other tests in this binary record to the same global registry at the
        // same time.
        for metrics in [&first, &second] {
            assert!(
                metrics.render().contains("provider=\"reuse_check\""),
                "both handles must read one registry"
            );
        }
    }

    #[test]
    fn rate_limit_rejections_name_the_tenant_and_the_dimension() {
        let rendered = render_with(|| {
            record_rate_limited("acme", "rpm");
            record_rate_limited("acme", "budget");
        });

        assert!(rendered.contains(RATE_LIMITED_TOTAL), "{rendered}");
        assert!(rendered.contains("tenant=\"acme\""), "{rendered}");
        assert!(rendered.contains("reason=\"rpm\""), "{rendered}");
        assert!(rendered.contains("reason=\"budget\""), "{rendered}");
    }

    #[test]
    fn reload_counter_separates_result_from_source() {
        let rendered = render_with(|| {
            record_reload("success", "file");
            record_reload("rejected", "signal");
        });

        assert!(rendered.contains(RELOAD_TOTAL), "{rendered}");
        assert!(rendered.contains("result=\"success\""), "{rendered}");
        assert!(rendered.contains("result=\"rejected\""), "{rendered}");
        assert!(rendered.contains("source=\"signal\""), "{rendered}");
    }

    #[test]
    fn inflight_gauge_tracks_the_guard_lifetime() {
        let recorder = PrometheusBuilder::new().build_recorder();
        let handle = recorder.handle();

        metrics::with_local_recorder(&recorder, || {
            let guard = InflightGuard::new();

            handle.run_upkeep();
            assert_eq!(
                gauge_value(&handle.render(), INFLIGHT_REQUESTS),
                Some(1.0),
                "a held guard counts as one in-flight request"
            );

            drop(guard);

            handle.run_upkeep();
            assert_eq!(
                gauge_value(&handle.render(), INFLIGHT_REQUESTS),
                Some(0.0),
                "dropping the guard must decrement"
            );
        });
    }

    #[test]
    fn breaker_gauge_reflects_the_reported_state() {
        let recorder = PrometheusBuilder::new().build_recorder();
        let handle = recorder.handle();
        let metrics = Metrics {
            handle: handle.clone(),
            seen_breakers: Arc::new(Mutex::new(HashSet::new())),
        };

        metrics::with_local_recorder(&recorder, || {
            metrics.sync_breakers([
                ("openai/gpt-4o".to_string(), true),
                ("anthropic/claude".to_string(), false),
            ]);
        });
        handle.run_upkeep();
        let rendered = handle.render();

        assert_eq!(
            gauge_value(
                &rendered,
                &format!("{BREAKER_OPEN}{{upstream=\"openai/gpt-4o\"}}")
            ),
            Some(1.0),
            "{rendered}"
        );
        assert_eq!(
            gauge_value(
                &rendered,
                &format!("{BREAKER_OPEN}{{upstream=\"anthropic/claude\"}}")
            ),
            Some(0.0),
            "{rendered}"
        );
    }

    #[test]
    fn seeding_publishes_idle_gauges_before_any_traffic() {
        let recorder = PrometheusBuilder::new().build_recorder();
        let handle = recorder.handle();
        let metrics = Metrics {
            handle: handle.clone(),
            seen_breakers: Arc::new(Mutex::new(HashSet::new())),
        };

        metrics::with_local_recorder(&recorder, || {
            metrics.seed_idle(vec!["openai/gpt-4o".to_string()]);
        });

        handle.run_upkeep();
        let rendered = handle.render();

        assert_eq!(
            gauge_value(&rendered, INFLIGHT_REQUESTS),
            Some(0.0),
            "a fresh process must report zero in-flight requests: {rendered}"
        );
        assert_eq!(
            gauge_value(
                &rendered,
                &format!("{BREAKER_OPEN}{{upstream=\"openai/gpt-4o\"}}")
            ),
            Some(0.0),
            "configured upstreams must be present and closed: {rendered}"
        );
    }

    #[test]
    fn breaker_gauge_is_reset_for_upstreams_that_stop_reporting() {
        let recorder = PrometheusBuilder::new().build_recorder();
        let handle = recorder.handle();
        let metrics = Metrics {
            handle: handle.clone(),
            seen_breakers: Arc::new(Mutex::new(HashSet::new())),
        };

        metrics::with_local_recorder(&recorder, || {
            metrics.sync_breakers([("retired/gpt-4o".to_string(), true)]);
        });
        // A reload removed the provider: the next scrape reports nothing for it.
        metrics::with_local_recorder(&recorder, || {
            metrics.sync_breakers(Vec::new());
        });

        handle.run_upkeep();
        let rendered = handle.render();

        assert_eq!(
            gauge_value(
                &rendered,
                &format!("{BREAKER_OPEN}{{upstream=\"retired/gpt-4o\"}}")
            ),
            Some(0.0),
            "a departed upstream must not look ejected forever: {rendered}"
        );
    }
}

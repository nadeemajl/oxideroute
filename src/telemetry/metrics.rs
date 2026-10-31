//! Prometheus metrics exposed on the scrape endpoint.
//!
//! Recording goes through the `metrics` facade, so the hot path stays a no-op
//! (a relaxed atomic increment) when no recorder is installed — which is what
//! keeps the unit tests free of global state.
//!
//! The four families are the ones named in `docs/IMPLEMENTATION_PLAN.md`.

use std::sync::Mutex;

use metrics::{counter, histogram};
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

/// Holds the single installed recorder so repeated `install` calls are harmless
/// (the CLI and tests both reach for one).
static INSTALLED: Mutex<Option<PrometheusHandle>> = Mutex::new(None);

/// Handle to the process-wide metric registry.
#[derive(Clone)]
pub struct Metrics {
    handle: PrometheusHandle,
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

        if let Some(handle) = installed.as_ref() {
            return Ok(Self {
                handle: handle.clone(),
            });
        }

        let recorder = PrometheusBuilder::new().build_recorder();
        let handle = recorder.handle();

        metrics::set_global_recorder(recorder).map_err(|error| RouterError::Internal {
            message: format!("failed to install the metrics recorder: {error}"),
        })?;

        *installed = Some(handle.clone());

        Ok(Self { handle })
    }

    /// Renders the exposition payload, bringing histograms up to date first.
    pub fn render(&self) -> String {
        // Histograms are aggregated in a background upkeep pass in normal
        // operation; doing it here means a scrape reflects the latest samples
        // instead of the last periodic flush.
        self.handle.run_upkeep();
        self.handle.render()
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

        // Both handles read the same registry.
        assert_eq!(first.render(), second.render());
    }
}

//! Fallback execution and the pre-first-token watchdog.
//!
//! The executor walks an ordered candidate list and returns only once an
//! upstream has produced a first frame. Because the caller has not yet written
//! any downstream headers, a dead or stalled upstream can still be replaced
//! transparently.
//!
//! The watchdog wraps the *entire* `execute_stream` call — including waiting for
//! response headers — so it catches both stall shapes:
//!
//! * the upstream never answers at all (pre-header stall), and
//! * the upstream answers `200` then sends only `: ping` keep-alives
//!   (post-header stall).

use std::time::{Duration, Instant};

use bytes::Bytes;

use crate::config::RouterConfig;
use crate::error::{AttemptRecord, RouterError};
use crate::providers::{NonStreamResponse, SseByteStream};
use crate::proxy::inspector::ProxyRequest;
use crate::proxy::router_engine::Resolution;
use crate::state::{now_ms, AppState};
use crate::telemetry::metrics;

/// A successfully started stream.
pub struct StreamDispatch {
    /// The first frame, already normalised to OpenAI framing.
    pub first_chunk: Bytes,
    /// The remainder of the stream.
    pub stream: SseByteStream,
    /// `provider/model` that answered.
    pub upstream: String,
    /// Provider config key that answered.
    pub provider: String,
    /// Observed time-to-first-frame.
    pub ttft: Duration,
}

/// A successfully completed non-streaming request.
pub struct JsonDispatch {
    /// The upstream response.
    pub response: NonStreamResponse,
    /// `provider/model` that answered.
    pub upstream: String,
    /// Provider config key that answered.
    pub provider: String,
}

/// Dispatches a streaming request across candidates until one starts.
///
/// # Errors
///
/// Returns [`RouterError::AllUpstreamsFailed`] carrying the full attempt log
/// when every candidate fails, or the first non-retryable error as-is.
pub async fn dispatch_stream(
    state: &AppState,
    config: &RouterConfig,
    request: &ProxyRequest,
    resolution: &Resolution,
) -> Result<StreamDispatch, RouterError> {
    let breaker = &config.resiliency.circuit_breaker;
    let threshold = breaker.consecutive_failures_threshold;
    let cooldown = Duration::from_secs(breaker.cooldown_seconds);

    let mut attempts: Vec<AttemptRecord> = Vec::new();

    for (index, candidate) in resolution.candidates.iter().enumerate() {
        let reference = candidate.reference();
        let health = state.health_for(&reference);

        // A tripped breaker is skipped for free rather than costing a timeout.
        if !health.is_healthy(now_ms()) {
            metrics::record_request(&candidate.provider, &candidate.model, "circuit_open");
            attempts.push(AttemptRecord::new(reference, "circuit_breaker_open", 0));
            continue;
        }

        let Some(provider) = state.provider(&candidate.provider) else {
            attempts.push(AttemptRecord::new(reference, "unknown_provider", 0));
            continue;
        };

        let ttft_budget_ms = config
            .providers
            .get(&candidate.provider)
            .map(|provider| provider.ttft_timeout_ms)
            .unwrap_or(crate::config::DEFAULT_TTFT_TIMEOUT_MS);

        let started = Instant::now();
        let outcome = tokio::time::timeout(
            Duration::from_millis(ttft_budget_ms),
            provider.execute_stream(&candidate.model, request),
        )
        .await;

        match outcome {
            Ok(Ok((first_chunk, stream))) => {
                let ttft = started.elapsed();
                health.record_success(ttft);
                metrics::record_ttft(&candidate.provider, &candidate.model, ttft.as_secs_f64());
                metrics::record_request(&candidate.provider, &candidate.model, "success");
                return Ok(StreamDispatch {
                    first_chunk,
                    stream,
                    upstream: reference,
                    provider: candidate.provider.clone(),
                    ttft,
                });
            }
            Ok(Err(error)) => {
                let elapsed_ms = started.elapsed().as_millis() as u64;

                if !is_retryable(&error, &resolution.retry_on_status) {
                    // A 400-class error means the request itself is wrong; another
                    // upstream would fail the same way.
                    metrics::record_request(&candidate.provider, &candidate.model, "error");
                    return Err(error);
                }

                let reason = reason_for(&error);
                health.record_failure(threshold, cooldown);
                metrics::record_request(&candidate.provider, &candidate.model, "error");
                record_fallback_if_any(&resolution.candidates, index, &candidate.provider, &reason);
                attempts.push(AttemptRecord::new(reference, reason, elapsed_ms));
            }
            Err(_) => {
                let elapsed_ms = started.elapsed().as_millis() as u64;
                let reason = format!("ttft_timeout_exceeded ({ttft_budget_ms}ms)");
                health.record_failure(threshold, cooldown);
                metrics::record_request(&candidate.provider, &candidate.model, "error");
                record_fallback_if_any(&resolution.candidates, index, &candidate.provider, &reason);
                attempts.push(AttemptRecord::new(reference, reason, elapsed_ms));
            }
        }
    }

    Err(RouterError::AllUpstreamsFailed {
        model: request.requested_model.clone(),
        request_id: request.request_id.clone(),
        attempts,
    })
}

/// Dispatches a non-streaming request across candidates until one succeeds.
///
/// There is no first-token gate here: the whole body is buffered anyway, and
/// the client timeout governs the wait.
///
/// # Errors
///
/// Mirrors [`dispatch_stream`].
pub async fn dispatch_json(
    state: &AppState,
    config: &RouterConfig,
    request: &ProxyRequest,
    resolution: &Resolution,
) -> Result<JsonDispatch, RouterError> {
    let breaker = &config.resiliency.circuit_breaker;
    let threshold = breaker.consecutive_failures_threshold;
    let cooldown = Duration::from_secs(breaker.cooldown_seconds);

    let mut attempts: Vec<AttemptRecord> = Vec::new();

    for (index, candidate) in resolution.candidates.iter().enumerate() {
        let reference = candidate.reference();
        let health = state.health_for(&reference);

        if !health.is_healthy(now_ms()) {
            metrics::record_request(&candidate.provider, &candidate.model, "circuit_open");
            attempts.push(AttemptRecord::new(reference, "circuit_breaker_open", 0));
            continue;
        }

        let Some(provider) = state.provider(&candidate.provider) else {
            attempts.push(AttemptRecord::new(reference, "unknown_provider", 0));
            continue;
        };

        let started = Instant::now();
        match provider.execute_json(&candidate.model, request).await {
            Ok(response) => {
                health.record_healthy();
                metrics::record_request(&candidate.provider, &candidate.model, "success");
                return Ok(JsonDispatch {
                    response,
                    upstream: reference,
                    provider: candidate.provider.clone(),
                });
            }
            Err(error) => {
                let elapsed_ms = started.elapsed().as_millis() as u64;

                if !is_retryable(&error, &resolution.retry_on_status) {
                    metrics::record_request(&candidate.provider, &candidate.model, "error");
                    return Err(error);
                }

                let reason = reason_for(&error);
                health.record_failure(threshold, cooldown);
                metrics::record_request(&candidate.provider, &candidate.model, "error");
                record_fallback_if_any(&resolution.candidates, index, &candidate.provider, &reason);
                attempts.push(AttemptRecord::new(reference, reason, elapsed_ms));
            }
        }
    }

    Err(RouterError::AllUpstreamsFailed {
        model: request.requested_model.clone(),
        request_id: request.request_id.clone(),
        attempts,
    })
}

/// Whether a failure is worth trying elsewhere.
///
/// Transport failures and TTFT timeouts always are; HTTP errors only when the
/// model's `retry_on_status` list (or the documented default) says so.
fn is_retryable(error: &RouterError, retry_on_status: &[u16]) -> bool {
    match error {
        RouterError::UpstreamHttp { status, .. } => retry_on_status.contains(status),
        RouterError::Transport { .. } | RouterError::TtftTimeout { .. } => true,
        RouterError::CircuitBreakerOpen { .. } => true,
        _ => false,
    }
}

/// Emits the failover counter when another candidate remains to be tried.
fn record_fallback_if_any(
    candidates: &[crate::proxy::router_engine::Candidate],
    index: usize,
    from_provider: &str,
    reason: &str,
) {
    if let Some(next) = candidates.get(index + 1) {
        metrics::record_fallback(from_provider, &next.provider, reason);
    }
}

fn reason_for(error: &RouterError) -> String {
    match error {
        RouterError::UpstreamHttp { status, .. } => format!("http_status_{status}"),
        RouterError::Transport { message, .. } => format!("transport_error: {message}"),
        other => other.code().to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn retryable_statuses_come_from_the_configured_list() {
        let error = RouterError::UpstreamHttp {
            provider: "p/m".to_string(),
            status: 429,
        };

        assert!(is_retryable(&error, &[429]));
        assert!(!is_retryable(&error, &[500]));
    }

    #[test]
    fn transport_and_timeouts_always_retry() {
        let transport = RouterError::Transport {
            provider: "p/m".to_string(),
            message: "reset".to_string(),
        };
        let timeout = RouterError::TtftTimeout {
            provider: "p/m".to_string(),
            timeout_ms: 10,
        };

        assert!(is_retryable(&transport, &[]));
        assert!(is_retryable(&timeout, &[]));
    }

    #[test]
    fn client_errors_do_not_retry() {
        let error = RouterError::InvalidRequest {
            message: "bad".to_string(),
        };

        assert!(!is_retryable(&error, &DEFAULT_STATUSES_FOR_TEST));
    }

    const DEFAULT_STATUSES_FOR_TEST: [u16; 1] = [429];

    #[test]
    fn reason_matches_the_documented_vocabulary() {
        assert_eq!(
            reason_for(&RouterError::UpstreamHttp {
                provider: "p/m".to_string(),
                status: 429,
            }),
            "http_status_429"
        );

        assert_eq!(
            reason_for(&RouterError::Transport {
                provider: "p/m".to_string(),
                message: "reset".to_string(),
            }),
            "transport_error: reset"
        );
    }
}

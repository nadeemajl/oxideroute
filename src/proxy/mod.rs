//! HTTP ingress: routing, dispatch, and the OpenAI-compatible surface.
//!
//! # Where `200 OK` is decided
//!
//! [`handle_chat`] does **not** build a response until
//! [`executor::dispatch_stream`] has returned a first frame. That ordering is
//! what lets a stalled upstream be replaced before the client sees headers, and
//! it is the single most important property of this module — do not move
//! response construction above the dispatch call.

pub mod executor;
pub mod inspector;
pub mod ratelimit;
pub mod router_engine;

use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use axum::body::Body;
use axum::extract::State;
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::Router;
use bytes::Bytes;
use futures_util::{Stream, StreamExt};
use http::header::{CACHE_CONTROL, CONTENT_TYPE};
use http::{HeaderMap, HeaderValue, StatusCode};
use tracing::Instrument;

use self::executor::{
    dispatch_embeddings, dispatch_json, dispatch_stream, JsonDispatch, StreamDispatch,
};
use self::inspector::{inspect_request, ProxyRequest};
use self::ratelimit::resolve_tenant;
use self::router_engine::{resolve_candidates, Resolution};
use crate::config::RouterConfig;
use crate::error::RouterError;
use crate::state::{now_ms, AppState};
use crate::telemetry::metrics::{self, InflightGuard, Metrics};

/// Builds the public ingress router.
///
/// Deliberately excludes `/metrics`: the scrape endpoint belongs on the admin
/// listener (see [`build_admin_router`]) so it is not internet-facing.
pub fn build_router(state: Arc<AppState>) -> Router {
    ingress_router(state, None)
}

/// Builds the admin router: the Prometheus scrape endpoint and a liveness probe.
///
/// `serve` binds this on `admin_listen` when configured. Exposing `/metrics` on
/// the ingress would publish per-tenant and per-upstream traffic shape to anyone
/// who can reach the public port.
///
/// The route honours `telemetry.prometheus_endpoint`. It is resolved once, at
/// build time, alongside the listener: changing either on a reload takes effect
/// on restart, not mid-stream.
pub fn build_admin_router(state: Arc<AppState>, metrics: Metrics) -> Router {
    let path = scrape_path(&state);

    Router::new()
        .route(&path, scrape_route(metrics))
        .route("/healthz", get(healthz))
        .with_state(state)
}

/// Builds a single-listener router with the scrape endpoint attached.
///
/// Use this only when no separate admin port is configured. Prefer
/// [`build_router`] + [`build_admin_router`].
pub fn build_router_with_metrics(state: Arc<AppState>, metrics: Metrics) -> Router {
    ingress_router(state, Some(metrics))
}

fn ingress_router(state: Arc<AppState>, scrape: Option<Metrics>) -> Router {
    let router = Router::new()
        .route("/v1/chat/completions", post(chat_completions))
        .route("/v1/embeddings", post(embeddings))
        .route("/healthz", get(healthz));

    let router = match scrape {
        Some(metrics) => router.route(&scrape_path(&state), scrape_route(metrics)),
        None => router,
    };

    router.with_state(state)
}

/// The configured scrape path, defaulting when it is not a usable route.
///
/// The loader rejects a non-absolute path, but `AppState` can be built from a
/// config that never went through validation, and axum panics on a route that
/// does not start with `/` — so the fallback is a correctness guard, not a
/// nicety.
fn scrape_path(state: &AppState) -> String {
    let configured = state.config().telemetry.prometheus_endpoint.clone();

    if configured.starts_with('/') {
        configured
    } else {
        "/metrics".to_string()
    }
}

/// The scrape handler: refresh breaker gauges from live state, then render.
///
/// Breakers are exported at scrape time rather than mutated on the request path
/// because they are state, not events — a cooldown that lapses between requests
/// must still show as closed.
fn scrape_route(metrics: Metrics) -> axum::routing::MethodRouter<Arc<AppState>> {
    get(move |State(state): State<Arc<AppState>>| {
        let metrics = metrics.clone();

        async move {
            let at = now_ms();
            let observed: Vec<(String, bool)> = state
                .health()
                .iter()
                .map(|entry| (entry.key().clone(), !entry.value().is_healthy(at)))
                .collect();

            metrics.sync_breakers(observed);

            (
                [(CONTENT_TYPE, "text/plain; version=0.0.4; charset=utf-8")],
                metrics.render(),
            )
        }
    })
}

async fn healthz() -> &'static str {
    "ok"
}

async fn chat_completions(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    // Held until the response body finishes, so a long stream counts as in
    // flight for its whole duration rather than only while the handler runs.
    let inflight = InflightGuard::new();

    match handle_chat(state, headers, body, inflight).await {
        Ok(response) => response,
        Err(error) => error.into_response(),
    }
}

async fn embeddings(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let inflight = InflightGuard::new();

    match handle_embeddings(state, headers, body, inflight).await {
        Ok(response) => response,
        Err(error) => error.into_response(),
    }
}

/// Everything a request needs before dispatch, all derived from **one** config
/// snapshot so a concurrent reload cannot split the decision.
struct Prepared {
    request: ProxyRequest,
    config: Arc<RouterConfig>,
    tenant: String,
    resolution: Resolution,
}

async fn prepare(
    state: &AppState,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Prepared, RouterError> {
    let request = inspect_request(headers, body)?;
    let config = state.config();

    // Resolution precedes admission because the daily-budget reservation is
    // priced from the candidate that will actually serve the request.
    let resolution = resolve_candidates(&config, &request, state.health())?;

    if resolution.candidates.is_empty() {
        return Err(RouterError::InvalidRequest {
            message: format!(
                "no upstream candidates configured for model `{}`",
                request.requested_model
            ),
        });
    }

    let tenant = resolve_tenant(&config, &request.headers);
    let tenant_name = tenant
        .as_ref()
        .map(|(name, _)| name.clone())
        .unwrap_or_else(|| "anonymous".to_string());

    if let Some((name, limits)) = &tenant {
        let estimated_cost = resolution
            .candidates
            .first()
            .map(|candidate| {
                config.cost_usd(
                    &candidate.provider,
                    &candidate.model,
                    request.estimated_prompt_tokens,
                    0,
                )
            })
            .unwrap_or(0.0);

        state.rate_limiter().enforce(
            name,
            limits,
            request.estimated_prompt_tokens,
            estimated_cost,
        )?;
    }

    Ok(Prepared {
        request,
        config,
        tenant: tenant_name,
        resolution,
    })
}

async fn handle_chat(
    state: Arc<AppState>,
    headers: HeaderMap,
    body: Bytes,
    inflight: InflightGuard,
) -> Result<Response, RouterError> {
    let Prepared {
        request,
        config,
        tenant,
        resolution,
    } = prepare(&state, headers, body).await?;

    let span = tracing::info_span!(
        "upstream.dispatch",
        request_id = %request.request_id,
        model = %request.requested_model,
        tenant = %tenant,
        stream = request.is_stream,
    );

    if request.is_stream {
        let outcome = dispatch_stream(&state, &config, &request, &resolution)
            .instrument(span)
            .await?;

        // Streaming responses carry no usage, so only the prompt estimate can be
        // billed.
        record_usage(
            &state,
            &config,
            &tenant,
            &outcome.provider,
            &outcome.model,
            request.estimated_prompt_tokens,
            0,
        );

        Ok(sse_response(&request.request_id, outcome, inflight))
    } else {
        let outcome = dispatch_json(&state, &config, &request, &resolution)
            .instrument(span)
            .await?;

        record_usage(
            &state,
            &config,
            &tenant,
            &outcome.provider,
            &outcome.model,
            outcome.response.prompt_tokens,
            outcome.response.completion_tokens,
        );

        Ok(json_response(&request.request_id, outcome))
    }
}

async fn handle_embeddings(
    state: Arc<AppState>,
    headers: HeaderMap,
    body: Bytes,
    // Held for its `Drop`: the JSON body is fully buffered before the response
    // exists, so the request stops being in flight when this returns.
    _inflight: InflightGuard,
) -> Result<Response, RouterError> {
    let Prepared {
        request,
        config,
        tenant,
        resolution,
    } = prepare(&state, headers, body).await?;

    let span = tracing::info_span!(
        "upstream.dispatch",
        request_id = %request.request_id,
        model = %request.requested_model,
        tenant = %tenant,
        stream = false,
    );

    let outcome = dispatch_embeddings(&state, &config, &request, &resolution)
        .instrument(span)
        .await?;

    record_usage(
        &state,
        &config,
        &tenant,
        &outcome.provider,
        &outcome.model,
        outcome.response.prompt_tokens,
        0,
    );

    Ok(json_response(&request.request_id, outcome))
}

/// Emits token metrics and charges the tenant's daily budget.
fn record_usage(
    state: &AppState,
    config: &RouterConfig,
    tenant: &str,
    provider: &str,
    model: &str,
    prompt_tokens: usize,
    completion_tokens: usize,
) {
    metrics::record_tokens(tenant, provider, "prompt", prompt_tokens as u64);
    if completion_tokens > 0 {
        metrics::record_tokens(tenant, provider, "completion", completion_tokens as u64);
    }

    let cost = config.cost_usd(provider, model, prompt_tokens, completion_tokens);
    if cost > 0.0 {
        state.rate_limiter().record_spend_usd(tenant, cost);
    }
}

/// Wraps the first frame plus the remaining stream in an SSE response.
///
/// By the time this runs, `first_chunk` is non-empty, so committing `200` here
/// is exactly the point at which the upstream has proven it will answer.
///
/// `inflight` moves into the body stream: the request is in flight until the
/// last frame is written or the client hangs up, not until this function
/// returns.
fn sse_response(request_id: &str, outcome: StreamDispatch, inflight: InflightGuard) -> Response {
    let StreamDispatch {
        first_chunk,
        stream,
        upstream,
        ..
    } = outcome;

    let head = futures_util::stream::once(async move { Ok::<Bytes, std::io::Error>(first_chunk) });
    let tail = stream.map(|item| item.map_err(|error| std::io::Error::other(error.to_string())));

    let body = GuardedStream {
        inner: head.chain(tail),
        _guard: inflight,
    };

    let mut response = Response::new(Body::from_stream(body));
    *response.status_mut() = StatusCode::OK;

    let headers = response.headers_mut();
    headers.insert(CONTENT_TYPE, HeaderValue::from_static("text/event-stream"));
    headers.insert(CACHE_CONTROL, HeaderValue::from_static("no-cache"));
    insert_if_valid(headers, "x-oxide-request-id", request_id);
    insert_if_valid(headers, "x-oxide-upstream", &upstream);

    response
}

pin_project_lite::pin_project! {
    /// A stream that owns an [`InflightGuard`] for its whole lifetime.
    ///
    /// Without this the gauge would decrement as soon as the handler returned,
    /// reporting zero streams during a perfectly healthy long-lived SSE
    /// response.
    struct GuardedStream<S> {
        #[pin]
        inner: S,
        _guard: InflightGuard,
    }
}

impl<S: Stream> Stream for GuardedStream<S> {
    type Item = S::Item;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.project().inner.poll_next(cx)
    }
}

fn json_response(request_id: &str, outcome: JsonDispatch) -> Response {
    let JsonDispatch {
        response,
        upstream,
        provider: _,
        model: _,
    } = outcome;

    let status = StatusCode::from_u16(response.status).unwrap_or(StatusCode::OK);
    let mut response = Response::new(Body::from(response.body));
    *response.status_mut() = status;

    let headers = response.headers_mut();
    headers.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
    insert_if_valid(headers, "x-oxide-request-id", request_id);
    insert_if_valid(headers, "x-oxide-upstream", &upstream);

    response
}

fn insert_if_valid(headers: &mut HeaderMap, name: &'static str, value: &str) {
    if let Ok(value) = HeaderValue::from_str(value) {
        headers.insert(name, value);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{ProviderConfig, ProviderKind, RoutingStrategy, VirtualModelConfig};

    fn state_with_model() -> Arc<AppState> {
        let mut config = RouterConfig::default();
        config.providers.insert(
            "local".to_string(),
            ProviderConfig {
                kind: ProviderKind::OpenaiCompatible,
                api_key: None,
                base_url: Some("http://127.0.0.1:9/v1".to_string()),
                timeout_ms: 200,
                ttft_timeout_ms: 100,
            },
        );
        config.models.insert(
            "m".to_string(),
            VirtualModelConfig {
                primary: Some("local/gpt-4o".to_string()),
                strategy: RoutingStrategy::Priority,
                targets: Vec::new(),
                fallbacks: Vec::new(),
                retry_on_status: Vec::new(),
            },
        );

        Arc::new(AppState::new(config).expect("state builds"))
    }

    #[tokio::test]
    async fn unknown_model_is_a_400() {
        let state = state_with_model();
        let response = handle_chat(
            state,
            HeaderMap::new(),
            Bytes::from(r#"{"model":"nope"}"#.to_string()),
            InflightGuard::new(),
        )
        .await
        .expect_err("unknown model");

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        assert!(response.to_string().contains("unknown model"));
    }

    #[tokio::test]
    async fn invalid_json_is_a_400() {
        let state = state_with_model();
        let response = handle_chat(
            state,
            HeaderMap::new(),
            Bytes::from("{".to_string()),
            InflightGuard::new(),
        )
        .await
        .expect_err("bad json");

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn unreachable_upstream_becomes_a_502_envelope() {
        let state = state_with_model();
        let response = handle_chat(
            state,
            HeaderMap::new(),
            Bytes::from(r#"{"model":"m","stream":true}"#.to_string()),
            InflightGuard::new(),
        )
        .await
        .expect_err("upstream is down");

        assert_eq!(response.status(), StatusCode::BAD_GATEWAY);

        let envelope = serde_json::to_value(response.envelope()).expect("serializes");
        assert_eq!(envelope["error"]["code"], "all_upstreams_failed");
        assert!(envelope["error"]["oxide_metadata"]["attempted_upstreams"]
            .as_array()
            .is_some_and(|attempts| !attempts.is_empty()));
    }

    #[tokio::test]
    async fn embeddings_unknown_model_is_a_400() {
        let state = state_with_model();
        let response = handle_embeddings(
            state,
            HeaderMap::new(),
            Bytes::from(r#"{"model":"nope","input":"hi"}"#.to_string()),
            InflightGuard::new(),
        )
        .await
        .expect_err("unknown model");

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    }

    #[test]
    fn invalid_header_values_are_skipped_rather_than_panicking() {
        let mut headers = HeaderMap::new();
        insert_if_valid(&mut headers, "x-oxide-upstream", "bad\nvalue");

        assert!(headers.get("x-oxide-upstream").is_none());
    }

    #[test]
    fn valid_header_values_are_inserted() {
        let mut headers = HeaderMap::new();
        insert_if_valid(&mut headers, "x-oxide-upstream", "openai/gpt-4o");

        assert_eq!(
            headers
                .get("x-oxide-upstream")
                .and_then(|value| value.to_str().ok()),
            Some("openai/gpt-4o")
        );
    }

    #[tokio::test]
    async fn the_public_router_does_not_expose_metrics() {
        use tower::ServiceExt;

        let state = state_with_model();
        let request = http::Request::builder()
            .uri("/metrics")
            .body(Body::empty())
            .expect("request builds");

        let response = build_router(state)
            .oneshot(request)
            .await
            .expect("router responds");

        assert_eq!(
            response.status(),
            StatusCode::NOT_FOUND,
            "the scrape endpoint must not be reachable on the public listener"
        );
    }

    #[tokio::test]
    async fn the_admin_router_serves_metrics_and_health() {
        use tower::ServiceExt;

        let state = state_with_model();
        let metrics = Metrics::install().expect("metrics install");

        for uri in ["/metrics", "/healthz"] {
            let request = http::Request::builder()
                .uri(uri)
                .body(Body::empty())
                .expect("request builds");

            let response = build_admin_router(Arc::clone(&state), metrics.clone())
                .oneshot(request)
                .await
                .expect("admin router responds");

            assert_eq!(response.status(), StatusCode::OK, "{uri} must be served");
        }
    }

    #[tokio::test]
    async fn the_scrape_endpoint_honours_the_configured_path() {
        use tower::ServiceExt;

        let mut config = RouterConfig::default();
        config.telemetry.prometheus_endpoint = "/internal/metrics".to_string();
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
        let state = Arc::new(AppState::new(config).expect("state builds"));
        let metrics = Metrics::install().expect("metrics install");

        let configured = build_admin_router(Arc::clone(&state), metrics.clone())
            .oneshot(
                http::Request::builder()
                    .uri("/internal/metrics")
                    .body(Body::empty())
                    .expect("request builds"),
            )
            .await
            .expect("router responds");
        assert_eq!(configured.status(), StatusCode::OK);

        let default = build_admin_router(state, metrics)
            .oneshot(
                http::Request::builder()
                    .uri("/metrics")
                    .body(Body::empty())
                    .expect("request builds"),
            )
            .await
            .expect("router responds");
        assert_eq!(
            default.status(),
            StatusCode::NOT_FOUND,
            "the hardcoded path must not shadow the configured one"
        );
    }

    #[tokio::test]
    async fn the_scrape_endpoint_exports_breaker_state() {
        use http_body_util::BodyExt;
        use std::time::Duration;
        use tower::ServiceExt;

        let state = state_with_model();
        let metrics = Metrics::install().expect("metrics install");

        // Trip a breaker the way the executor would.
        let health = state.health_for("local/gpt-4o");
        health.record_failure(1, Duration::from_secs(60));

        let request = http::Request::builder()
            .uri("/metrics")
            .body(Body::empty())
            .expect("request builds");

        let response = build_admin_router(Arc::clone(&state), metrics)
            .oneshot(request)
            .await
            .expect("admin router responds");

        let body = response
            .into_body()
            .collect()
            .await
            .expect("body")
            .to_bytes();
        let rendered = String::from_utf8_lossy(&body);

        assert!(
            rendered.contains("oxide_breaker_open{upstream=\"local/gpt-4o\"} 1"),
            "an ejected upstream must be visible to a scrape: {rendered}"
        );
    }
}

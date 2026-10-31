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

use std::sync::Arc;

use axum::body::Body;
use axum::extract::State;
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::Router;
use bytes::Bytes;
use futures_util::StreamExt;
use http::header::{CACHE_CONTROL, CONTENT_TYPE};
use http::{HeaderMap, HeaderValue, StatusCode};

use self::executor::{dispatch_json, dispatch_stream, JsonDispatch, StreamDispatch};
use self::inspector::inspect_request;
use self::ratelimit::resolve_tenant;
use self::router_engine::resolve_candidates;
use crate::error::RouterError;
use crate::state::AppState;
use crate::telemetry::metrics::{self, Metrics};

/// Builds the public ingress router.
pub fn build_router(state: Arc<AppState>) -> Router {
    Router::new()
        .route("/v1/chat/completions", post(chat_completions))
        .route("/healthz", get(healthz))
        .with_state(state)
}

/// Builds the ingress router with the Prometheus scrape endpoint attached.
///
/// Kept separate from [`build_router`] so tests and embeddings-only deployments
/// need not install a global recorder.
pub fn build_router_with_metrics(state: Arc<AppState>, metrics: Metrics) -> Router {
    build_router(state).route(
        "/metrics",
        get(move || {
            let metrics = metrics.clone();
            async move {
                (
                    [(CONTENT_TYPE, "text/plain; version=0.0.4; charset=utf-8")],
                    metrics.render(),
                )
            }
        }),
    )
}

async fn healthz() -> &'static str {
    "ok"
}

async fn chat_completions(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    match handle_chat(state, headers, body).await {
        Ok(response) => response,
        Err(error) => error.into_response(),
    }
}

async fn handle_chat(
    state: Arc<AppState>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Response, RouterError> {
    let request = inspect_request(headers, body)?;

    // One snapshot for the whole request: a reload cannot change the rules
    // underneath a dispatch that is already in flight.
    let config = state.config();

    let tenant = resolve_tenant(&config, &request.headers);
    if let Some((tenant_name, limits)) = &tenant {
        state
            .rate_limiter()
            .enforce(tenant_name, limits, request.estimated_prompt_tokens)?;
    }
    // Attribute tokens even when no limit block is configured.
    let tenant_name = tenant
        .map(|(name, _)| name)
        .unwrap_or_else(|| "anonymous".to_string());

    let resolution = resolve_candidates(&config, &request, state.health())?;

    if resolution.candidates.is_empty() {
        return Err(RouterError::InvalidRequest {
            message: format!(
                "no upstream candidates configured for model `{}`",
                request.requested_model
            ),
        });
    }

    if request.is_stream {
        let outcome = dispatch_stream(&state, &config, &request, &resolution).await?;
        // Streaming responses do not report usage, so the estimate is what we
        // have to bill against.
        metrics::record_tokens(
            &tenant_name,
            &outcome.provider,
            "prompt",
            request.estimated_prompt_tokens as u64,
        );
        Ok(sse_response(&request.request_id, outcome))
    } else {
        let outcome = dispatch_json(&state, &config, &request, &resolution).await?;
        metrics::record_tokens(
            &tenant_name,
            &outcome.provider,
            "prompt",
            outcome.response.prompt_tokens as u64,
        );
        metrics::record_tokens(
            &tenant_name,
            &outcome.provider,
            "completion",
            outcome.response.completion_tokens as u64,
        );
        Ok(json_response(&request.request_id, outcome))
    }
}

/// Wraps the first frame plus the remaining stream in an SSE response.
///
/// By the time this runs, `first_chunk` is non-empty, so committing `200` here
/// is exactly the point at which the upstream has proven it will answer.
fn sse_response(request_id: &str, outcome: StreamDispatch) -> Response {
    let StreamDispatch {
        first_chunk,
        stream,
        upstream,
        ..
    } = outcome;

    let head = futures_util::stream::once(async move { Ok::<Bytes, std::io::Error>(first_chunk) });
    let tail = stream.map(|item| item.map_err(|error| std::io::Error::other(error.to_string())));

    let mut response = Response::new(Body::from_stream(head.chain(tail)));
    *response.status_mut() = StatusCode::OK;

    let headers = response.headers_mut();
    headers.insert(CONTENT_TYPE, HeaderValue::from_static("text/event-stream"));
    headers.insert(CACHE_CONTROL, HeaderValue::from_static("no-cache"));
    insert_if_valid(headers, "x-oxide-request-id", request_id);
    insert_if_valid(headers, "x-oxide-upstream", &upstream);

    response
}

fn json_response(request_id: &str, outcome: JsonDispatch) -> Response {
    let JsonDispatch {
        response,
        upstream,
        provider: _,
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
    use crate::config::{
        ProviderConfig, ProviderKind, RouterConfig, RoutingStrategy, VirtualModelConfig,
    };

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
        )
        .await
        .expect_err("unknown model");

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        assert!(response.to_string().contains("unknown model"));
    }

    #[tokio::test]
    async fn invalid_json_is_a_400() {
        let state = state_with_model();
        let response = handle_chat(state, HeaderMap::new(), Bytes::from("{".to_string()))
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
}

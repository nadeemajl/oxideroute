//! End-to-end failover, circuit breaker, EWMA, and rate-limit behaviour.
//!
//! Every test drives the real axum router through `tower::ServiceExt::oneshot`
//! against the in-process mock upstream, so nothing leaves localhost and no API
//! keys are required.

mod mock_upstreams;

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use axum::body::Body;
use bytes::Bytes;
use http::{HeaderMap, Request, StatusCode};
use oxide_route::config::{
    CircuitBreakerConfig, PricingConfig, ProviderConfig, ProviderKind, RateLimitConfig,
    RouterConfig, RoutingStrategy, VirtualModelConfig,
};
use oxide_route::proxy::build_router;
use oxide_route::proxy::inspector::inspect_request;
use oxide_route::proxy::router_engine::{resolve_candidates, Candidate};
use oxide_route::state::AppState;
use tower::ServiceExt;

use mock_upstreams::MockServer;

// --------------------------------------------------------------------------- helpers

fn providers(mock: &MockServer) -> BTreeMap<String, ProviderConfig> {
    let mut providers = BTreeMap::new();

    providers.insert(
        "primary".to_string(),
        ProviderConfig {
            kind: ProviderKind::OpenaiCompatible,
            api_key: None,
            base_url: Some(mock.base_url()),
            timeout_ms: 30_000,
            ttft_timeout_ms: 2_500,
        },
    );

    providers.insert(
        "secondary".to_string(),
        ProviderConfig {
            kind: ProviderKind::Anthropic,
            api_key: Some("sk-ant-test".to_string()),
            base_url: Some(mock.root_url()),
            timeout_ms: 30_000,
            ttft_timeout_ms: 2_500,
        },
    );

    providers
}

fn base_config(mock: &MockServer) -> RouterConfig {
    RouterConfig {
        listen: "127.0.0.1:0".to_string(),
        providers: providers(mock),
        ..RouterConfig::default()
    }
}

fn priority_model(primary: &str, fallbacks: &[&str]) -> VirtualModelConfig {
    VirtualModelConfig {
        primary: Some(primary.to_string()),
        strategy: RoutingStrategy::Priority,
        targets: Vec::new(),
        fallbacks: fallbacks.iter().map(|item| item.to_string()).collect(),
        retry_on_status: Vec::new(),
    }
}

fn chat_body(model: &str) -> serde_json::Value {
    serde_json::json!({
        "model": model,
        "stream": true,
        "messages": [{"role": "user", "content": "hi"}],
    })
}

async fn post_chat(
    app: &axum::Router,
    body: serde_json::Value,
    tenant: Option<&str>,
) -> (StatusCode, Bytes) {
    let mut builder = Request::builder()
        .method("POST")
        .uri("/v1/chat/completions")
        .header("content-type", "application/json");

    if let Some(tenant) = tenant {
        builder = builder.header("x-tenant-id", tenant);
    }

    let request = builder.body(Body::from(body.to_string())).expect("request");
    let response = app.clone().oneshot(request).await.expect("router responds");
    let status = response.status();
    let bytes = axum::body::to_bytes(response.into_body(), 10 * 1024 * 1024)
        .await
        .expect("body");

    (status, bytes)
}

fn attempts_of(body: &Bytes) -> Vec<serde_json::Value> {
    let value: serde_json::Value = serde_json::from_slice(body).expect("error envelope");
    value["error"]["oxide_metadata"]["attempted_upstreams"]
        .as_array()
        .cloned()
        .unwrap_or_default()
}

// --------------------------------------------------------------------------- tests

#[tokio::test]
async fn test_transparent_429_fallback_openai_to_anthropic() {
    let mock = MockServer::start().await;
    let mut config = base_config(&mock);
    config.models.insert(
        "smart-router".to_string(),
        priority_model(
            "primary/gpt-4o-ratelimited",
            &["secondary/claude-3-7-sonnet-latest"],
        ),
    );

    let state = Arc::new(AppState::new(config).expect("state builds"));
    let app = build_router(Arc::clone(&state));

    let (status, body) = post_chat(&app, chat_body("smart-router"), None).await;

    assert_eq!(status, StatusCode::OK, "fallback must be transparent");

    let text = String::from_utf8_lossy(&body);
    assert!(
        text.contains("\"object\":\"chat.completion.chunk\""),
        "expected OpenAI chunks, got: {text}"
    );
    // Content proves the Anthropic upstream answered, not the 429 primary.
    assert!(text.contains("Hello"), "missing Anthropic content: {text}");
    assert!(
        text.contains("from Claude"),
        "missing Anthropic content: {text}"
    );
    assert!(text.ends_with("data: [DONE]\n\n"), "stream must terminate");

    // The failing primary was recorded as a failure; the fallback as a success.
    assert!(
        state
            .health_for("primary/gpt-4o-ratelimited")
            .consecutive_failures()
            >= 1
    );
    assert!(
        state
            .health_for("secondary/claude-3-7-sonnet-latest")
            .ewma_ttft_us()
            > 0
    );
}

#[tokio::test]
async fn test_post_header_ttft_stall_triggers_fallback() {
    let mock = MockServer::start().await;
    let mut config = base_config(&mock);

    // Short watchdog so the 5s upstream stall is abandoned quickly.
    if let Some(primary) = config.providers.get_mut("primary") {
        primary.ttft_timeout_ms = 150;
    }

    config.models.insert(
        "stalling".to_string(),
        priority_model(
            "primary/gpt-4o-stalled-gpu",
            &["secondary/claude-3-7-sonnet-latest"],
        ),
    );

    let state = Arc::new(AppState::new(config).expect("state builds"));
    let app = build_router(Arc::clone(&state));

    let started = Instant::now();
    let (status, body) = post_chat(&app, chat_body("stalling"), None).await;
    let elapsed = started.elapsed();

    assert_eq!(status, StatusCode::OK);

    let text = String::from_utf8_lossy(&body);
    assert!(
        text.contains("from Claude"),
        "must fail over to the fallback: {text}"
    );
    assert!(
        !text.contains("too late"),
        "the stalled upstream leaked content downstream: {text}"
    );

    // The primary sends `: ping` then sleeps 5s. Failover must happen on the
    // 150ms watchdog, so the whole exchange finishes far sooner than the stall.
    assert!(
        elapsed < Duration::from_secs(2),
        "failover took {elapsed:?}; the TTFT watchdog did not fire"
    );

    assert!(
        state
            .health_for("primary/gpt-4o-stalled-gpu")
            .consecutive_failures()
            >= 1,
        "the stalled upstream must be recorded as a failure"
    );
}

#[tokio::test]
async fn test_circuit_breaker_ejection_and_recovery() {
    let mock = MockServer::start().await;
    let mut config = base_config(&mock);

    config.resiliency.circuit_breaker = CircuitBreakerConfig {
        consecutive_failures_threshold: 3,
        cooldown_seconds: 1,
        ..CircuitBreakerConfig::default()
    };

    config.models.insert(
        "flaky".to_string(),
        priority_model("primary/gpt-4o-ratelimited", &[]),
    );

    let state = Arc::new(AppState::new(config).expect("state builds"));
    let app = build_router(Arc::clone(&state));

    // Three failures trip the breaker.
    for _ in 0..3 {
        let (status, _) = post_chat(&app, chat_body("flaky"), None).await;
        assert_eq!(status, StatusCode::BAD_GATEWAY);
    }

    assert!(!state
        .health_for("primary/gpt-4o-ratelimited")
        .is_healthy(oxide_route::state::now_ms()));

    // The fourth is rejected without touching the upstream.
    let (status, body) = post_chat(&app, chat_body("flaky"), None).await;
    assert_eq!(status, StatusCode::BAD_GATEWAY);

    let attempts = attempts_of(&body);
    assert_eq!(
        attempts.first().and_then(|a| a["reason"].as_str()),
        Some("circuit_breaker_open"),
        "expected a breaker skip, got {attempts:?}"
    );
    assert_eq!(
        attempts.first().and_then(|a| a["elapsed_ms"].as_u64()),
        Some(0),
        "a skipped upstream must not consume time"
    );

    // After the cooldown the breaker half-opens and probes the upstream again.
    tokio::time::sleep(Duration::from_millis(1_100)).await;

    let (status, body) = post_chat(&app, chat_body("flaky"), None).await;
    assert_eq!(status, StatusCode::BAD_GATEWAY);

    let attempts = attempts_of(&body);
    assert_eq!(
        attempts.first().and_then(|a| a["reason"].as_str()),
        Some("http_status_429"),
        "half-open probe should reach the upstream, got {attempts:?}"
    );
}

#[tokio::test]
async fn test_ewma_latency_routing_shifts_traffic() {
    let mock = MockServer::start().await;
    let mut config = base_config(&mock);

    config.models.insert(
        "auto".to_string(),
        VirtualModelConfig {
            primary: None,
            strategy: RoutingStrategy::LatencyEwma,
            targets: vec![
                oxide_route::config::WeightedTarget {
                    upstream: "primary/gpt-4o-slow".to_string(),
                    weight: 1,
                },
                oxide_route::config::WeightedTarget {
                    upstream: "primary/gpt-4o-fast".to_string(),
                    weight: 1,
                },
            ],
            fallbacks: Vec::new(),
            retry_on_status: Vec::new(),
        },
    );

    let state = Arc::new(AppState::new(config).expect("state builds"));
    let app = build_router(Arc::clone(&state));

    // Warm up both upstreams and record their observed TTFTs.
    let request = inspect_request(
        HeaderMap::new(),
        Bytes::from(r#"{"model":"auto"}"#.to_string()),
    )
    .expect("inspects");

    // A real request, so the executor populates the EWMA rather than the test.
    let (status, _) = post_chat(&app, chat_body("auto"), None).await;
    assert_eq!(status, StatusCode::OK);

    state
        .health_for("primary/gpt-4o-fast")
        .record_success(Duration::from_millis(10));
    state
        .health_for("primary/gpt-4o-slow")
        .record_success(Duration::from_millis(120));

    let resolution =
        resolve_candidates(&state.config(), &request, state.health()).expect("resolves");

    let order: Vec<Candidate> = resolution.candidates;
    assert_eq!(
        order.first().map(Candidate::reference).as_deref(),
        Some("primary/gpt-4o-fast"),
        "the lowest-EWMA target must lead, got {order:?}"
    );
    assert_eq!(
        order.get(1).map(Candidate::reference).as_deref(),
        Some("primary/gpt-4o-slow")
    );

    let fast_ewma = state.health_for("primary/gpt-4o-fast").ewma_ttft_us();
    assert!(fast_ewma > 0, "the executor must record TTFT");
    assert!(
        fast_ewma < state.health_for("primary/gpt-4o-slow").ewma_ttft_us(),
        "fast upstream should have the smaller EWMA"
    );
}

#[tokio::test]
async fn test_tenant_rpm_and_tpm_rate_limiting() {
    let mock = MockServer::start().await;
    let mut config = base_config(&mock);

    config.rate_limits.insert(
        "default_tenant".to_string(),
        RateLimitConfig {
            match_header: "X-Tenant-ID".to_string(),
            rpm: 5,
            tpm: 100,
            daily_budget_usd: 1_000.0,
            burst_multiplier: 1.0,
        },
    );
    config.models.insert(
        "auto".to_string(),
        priority_model("primary/gpt-4o-fast", &[]),
    );

    let state = Arc::new(AppState::new(config).expect("state builds"));
    let app = build_router(Arc::clone(&state));

    // Five small requests fit both the RPM and TPM budgets.
    for index in 0..5 {
        let (status, body) = post_chat(&app, chat_body("auto"), Some("acme")).await;
        assert_eq!(
            status,
            StatusCode::OK,
            "request {index} should be admitted: {}",
            String::from_utf8_lossy(&body)
        );
    }

    // The sixth exceeds rpm = 5.
    let (status, body) = post_chat(&app, chat_body("auto"), Some("acme")).await;
    assert_eq!(status, StatusCode::TOO_MANY_REQUESTS);

    let envelope: serde_json::Value = serde_json::from_slice(&body).expect("envelope");
    assert_eq!(envelope["error"]["type"], "rate_limit_error");
    assert_eq!(envelope["error"]["code"], "rate_limit_exceeded");

    // A different tenant is unaffected.
    let (status, _) = post_chat(&app, chat_body("auto"), Some("globex")).await;
    assert_eq!(status, StatusCode::OK);

    // A single oversized request is rejected by the TPM bucket, on fresh state
    // so RPM cannot be the reason.
    let mut config = base_config(&mock);
    config.rate_limits.insert(
        "default_tenant".to_string(),
        RateLimitConfig {
            match_header: "X-Tenant-ID".to_string(),
            rpm: 100,
            tpm: 100,
            daily_budget_usd: 1_000.0,
            burst_multiplier: 1.0,
        },
    );
    config.models.insert(
        "auto".to_string(),
        priority_model("primary/gpt-4o-fast", &[]),
    );

    let state = Arc::new(AppState::new(config).expect("state builds"));
    let app = build_router(Arc::clone(&state));

    let oversized = serde_json::json!({
        "model": "auto",
        "stream": true,
        "messages": [{"role": "user", "content": "x".repeat(4_000)}],
    });

    let (status, body) = post_chat(&app, oversized, Some("acme")).await;
    assert_eq!(status, StatusCode::TOO_MANY_REQUESTS);

    let envelope: serde_json::Value = serde_json::from_slice(&body).expect("envelope");
    assert!(
        envelope["error"]["message"]
            .as_str()
            .is_some_and(|message| message.contains("tokens per minute")),
        "expected a TPM rejection, got {}",
        envelope["error"]
    );
}

#[tokio::test]
async fn test_pricing_charges_the_tenant_daily_budget() {
    let mock = MockServer::start().await;
    let mut config = base_config(&mock);

    config.rate_limits.insert(
        "default_tenant".to_string(),
        RateLimitConfig {
            match_header: "X-Tenant-ID".to_string(),
            rpm: 1_000,
            tpm: 1_000_000,
            daily_budget_usd: 1_000.0,
            burst_multiplier: 1.0,
        },
    );

    // Deliberately round numbers: $1M per million prompt tokens means one token
    // costs exactly one dollar, so the assertion is unambiguous.
    config.pricing.insert(
        "primary/gpt-4o-json".to_string(),
        PricingConfig {
            prompt_per_million_usd: 1_000_000.0,
            completion_per_million_usd: 0.0,
        },
    );

    config.models.insert(
        "priced".to_string(),
        priority_model("primary/gpt-4o-json", &[]),
    );

    let state = Arc::new(AppState::new(config).expect("state builds"));
    let app = build_router(Arc::clone(&state));

    // Non-streaming, so the mock reports usage (11 prompt tokens).
    let body = serde_json::json!({
        "model": "priced",
        "stream": false,
        "messages": [{"role": "user", "content": "hi"}],
    });

    let (status, _) = post_chat(&app, body, Some("acme")).await;
    assert_eq!(status, StatusCode::OK);

    let spent = state.rate_limiter().spent_usd("acme");
    assert!(
        (spent - 11.0).abs() < 0.01,
        "expected $11 of spend for 11 prompt tokens, got {spent}"
    );
}

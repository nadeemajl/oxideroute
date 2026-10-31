//! `/v1/embeddings` behaviour: pass-through, model rewrite, and rejection.

mod mock_upstreams;

use std::collections::BTreeMap;
use std::sync::Arc;

use axum::body::Body;
use bytes::Bytes;
use http::{Request, StatusCode};
use oxide_route::config::{
    ProviderConfig, ProviderKind, RouterConfig, RoutingStrategy, VirtualModelConfig,
};
use oxide_route::proxy::build_router;
use oxide_route::state::AppState;
use tower::ServiceExt;

use mock_upstreams::MockServer;

fn providers(mock: &MockServer) -> BTreeMap<String, ProviderConfig> {
    let mut providers = BTreeMap::new();

    providers.insert(
        "primary".to_string(),
        ProviderConfig {
            kind: ProviderKind::OpenaiCompatible,
            api_key: None,
            base_url: Some(mock.base_url()),
            timeout_ms: 5_000,
            ttft_timeout_ms: 1_000,
        },
    );

    providers.insert(
        "secondary".to_string(),
        ProviderConfig {
            kind: ProviderKind::Anthropic,
            api_key: Some("sk-ant-test".to_string()),
            base_url: Some(mock.root_url()),
            timeout_ms: 5_000,
            ttft_timeout_ms: 1_000,
        },
    );

    providers
}

fn config_serving(mock: &MockServer, upstream: &str) -> RouterConfig {
    let mut config = RouterConfig {
        listen: "127.0.0.1:0".to_string(),
        providers: providers(mock),
        ..RouterConfig::default()
    };

    config.models.insert(
        "emb".to_string(),
        VirtualModelConfig {
            primary: Some(upstream.to_string()),
            strategy: RoutingStrategy::Priority,
            targets: Vec::new(),
            fallbacks: Vec::new(),
            retry_on_status: Vec::new(),
        },
    );

    config
}

async fn post_embeddings(app: &axum::Router, body: &str) -> (StatusCode, Bytes) {
    let request = Request::builder()
        .method("POST")
        .uri("/v1/embeddings")
        .header("content-type", "application/json")
        .body(Body::from(body.to_string()))
        .expect("request");

    let response = app.clone().oneshot(request).await.expect("responds");
    let status = response.status();
    let bytes = axum::body::to_bytes(response.into_body(), 1024 * 1024)
        .await
        .expect("body");

    (status, bytes)
}

#[tokio::test]
async fn embeddings_pass_through_byte_for_byte() {
    let mock = MockServer::start().await;
    // Upstream model id equals the requested model, so the body is forwarded as-is.
    let config = config_serving(&mock, "primary/emb");
    let state = Arc::new(AppState::new(config).expect("state builds"));
    let app = build_router(state);

    let body = r#"{"model":"emb","input":"hello world","encoding_format":"float"}"#;
    let (status, response) = post_embeddings(&app, body).await;

    assert_eq!(status, StatusCode::OK);

    let payload: serde_json::Value = serde_json::from_slice(&response).expect("json");
    assert_eq!(payload["object"], "list");
    assert!(payload["data"][0]["embedding"].is_array());

    let received = mock.requests();
    let forwarded = &received.first().expect("one request").body;
    assert_eq!(
        forwarded.as_ref(),
        body.as_bytes(),
        "embeddings must forward the original payload untouched"
    );
}

#[tokio::test]
async fn embeddings_rewrite_only_the_model() {
    let mock = MockServer::start().await;
    let config = config_serving(&mock, "primary/text-embedding-3-small");
    let state = Arc::new(AppState::new(config).expect("state builds"));
    let app = build_router(state);

    let body = r#"{"model":"emb","input":"hello","encoding_format":"float"}"#;
    let (status, _) = post_embeddings(&app, body).await;
    assert_eq!(status, StatusCode::OK);

    let received = mock.requests();
    let forwarded = &received.first().expect("one request").body;
    let expected = body.replace("\"emb\"", "\"text-embedding-3-small\"");

    assert_eq!(forwarded.as_ref(), expected.as_bytes());
}

#[tokio::test]
async fn embeddings_are_rejected_by_providers_without_support() {
    let mock = MockServer::start().await;
    let config = config_serving(&mock, "secondary/claude-3-7-sonnet-latest");
    let state = Arc::new(AppState::new(config).expect("state builds"));
    let app = build_router(state);

    let (status, body) = post_embeddings(&app, r#"{"model":"emb","input":"hi"}"#).await;

    assert_eq!(status, StatusCode::BAD_REQUEST);

    let payload: serde_json::Value = serde_json::from_slice(&body).expect("json");
    assert!(
        payload["error"]["message"]
            .as_str()
            .is_some_and(|message| message.contains("does not support")),
        "expected a clear rejection, got {payload}"
    );
}

#[tokio::test]
async fn embeddings_reject_unknown_models() {
    let mock = MockServer::start().await;
    let config = config_serving(&mock, "primary/emb");
    let state = Arc::new(AppState::new(config).expect("state builds"));
    let app = build_router(state);

    let (status, _) = post_embeddings(&app, r#"{"model":"nope","input":"hi"}"#).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
}

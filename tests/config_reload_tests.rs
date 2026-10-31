//! Hot-reload safety: in-flight streams survive, invalid configs are rejected.

mod mock_upstreams;

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use axum::body::Body;
use http::{Request, StatusCode};
use oxide_route::config::loader::{load_config_from_path, InterpolationMode};
use oxide_route::config::watcher::reload_from_path;
use oxide_route::proxy::build_router;
use oxide_route::state::AppState;
use tower::ServiceExt;

use mock_upstreams::MockServer;

// --------------------------------------------------------------------------- helpers

fn temp_path(label: &str) -> PathBuf {
    std::env::temp_dir().join(format!(
        "oxide-reload-{label}-{}.toml",
        uuid::Uuid::new_v4().simple()
    ))
}

/// Writes a config whose `base` model streams for ~500ms.
///
/// When `with_late` is set, an extra `late` model is included, representing a
/// route added by an operator mid-flight.
fn config_toml(mock: &MockServer, with_late: bool) -> String {
    let late = if with_late {
        "\n[models.late]\nprimary = \"primary/gpt-4o-fast\"\n"
    } else {
        ""
    };

    format!(
        r#"
listen = "127.0.0.1:0"

[providers.primary]
type = "openai_compatible"
base_url = "{base}"
timeout_ms = 30000
ttft_timeout_ms = 2500

[models.base]
primary = "primary/gpt-4o-slow-stream"
{late}"#,
        base = mock.base_url()
    )
}

fn invalid_reference_toml(mock: &MockServer) -> String {
    format!(
        r#"
listen = "127.0.0.1:0"

[providers.primary]
type = "openai_compatible"
base_url = "{base}"

[models.broken]
primary = "nowhere/gpt-4o"
"#,
        base = mock.base_url()
    )
}

fn chat_request(model: &str) -> Request<Body> {
    let body = serde_json::json!({
        "model": model,
        "stream": true,
        "messages": [{"role": "user", "content": "hi"}],
    });

    Request::builder()
        .method("POST")
        .uri("/v1/chat/completions")
        .header("content-type", "application/json")
        .body(Body::from(body.to_string()))
        .expect("request")
}

async fn post(app: &axum::Router, model: &str) -> (StatusCode, bytes::Bytes) {
    let response = app
        .clone()
        .oneshot(chat_request(model))
        .await
        .expect("router responds");
    let status = response.status();
    let body = axum::body::to_bytes(response.into_body(), 10 * 1024 * 1024)
        .await
        .expect("body");
    (status, body)
}

// --------------------------------------------------------------------------- tests

#[tokio::test]
async fn test_atomic_hot_reload_during_active_sse_stream() {
    let mock = MockServer::start().await;
    let path = temp_path("active");
    std::fs::write(&path, config_toml(&mock, false)).expect("write config");

    let config = load_config_from_path(&path, InterpolationMode::Strict).expect("config loads");
    let state = Arc::new(AppState::new(config).expect("state builds"));
    let app = build_router(Arc::clone(&state));

    // The `late` route does not exist yet.
    let (status, _) = post(&app, "late").await;
    assert_eq!(
        status,
        StatusCode::BAD_REQUEST,
        "unknown model should be rejected before the reload"
    );

    // Start a ~500ms SSE stream and let it get a few chunks out.
    let background = app.clone();
    let stream_task = tokio::spawn(async move {
        let response = background
            .oneshot(chat_request("base"))
            .await
            .expect("router responds");
        let status = response.status();
        let body = axum::body::to_bytes(response.into_body(), 10 * 1024 * 1024)
            .await
            .expect("body");
        (status, body)
    });

    tokio::time::sleep(Duration::from_millis(100)).await;

    // Swap in a config that adds a route, mid-stream.
    std::fs::write(&path, config_toml(&mock, true)).expect("rewrite config");
    reload_from_path(&state, &path).expect("valid reload succeeds");

    // The new route is visible to the very next request.
    let (status, _) = post(&app, "late").await;
    assert_eq!(
        status,
        StatusCode::OK,
        "the newly added route should be usable immediately"
    );

    // The in-flight stream is unaffected: every chunk still arrives.
    let (status, body) = stream_task.await.expect("stream task");
    assert_eq!(status, StatusCode::OK);

    let text = String::from_utf8_lossy(&body);
    for index in 0..10 {
        assert!(
            text.contains(&format!("chunk{index}")),
            "chunk{index} was dropped by the reload: {text}"
        );
    }
    assert!(
        text.ends_with("data: [DONE]\n\n"),
        "stream must terminate cleanly: {text}"
    );

    let _ = std::fs::remove_file(path);
}

#[tokio::test]
async fn test_invalid_config_reload_is_rejected_safely() {
    let mock = MockServer::start().await;
    let path = temp_path("invalid");
    std::fs::write(&path, config_toml(&mock, true)).expect("write config");

    let config = load_config_from_path(&path, InterpolationMode::Strict).expect("config loads");
    let state = Arc::new(AppState::new(config).expect("state builds"));
    let app = build_router(Arc::clone(&state));

    let active = state.config();
    assert!(active.models.contains_key("late"));

    // Syntactically broken TOML.
    std::fs::write(&path, "listen = [\n").expect("write broken config");
    assert!(
        reload_from_path(&state, &path).is_err(),
        "malformed TOML must be rejected"
    );

    // Semantically invalid: an undefined provider reference.
    std::fs::write(&path, invalid_reference_toml(&mock)).expect("write invalid config");
    assert!(
        reload_from_path(&state, &path).is_err(),
        "undefined provider references must be rejected"
    );

    // In both cases the previous configuration stayed live.
    let after = state.config();
    assert_eq!(after.listen, active.listen);
    assert!(after.models.contains_key("late"));
    assert!(!after.models.contains_key("broken"));

    let (status, _) = post(&app, "late").await;
    assert_eq!(
        status,
        StatusCode::OK,
        "the router must keep serving the previous config"
    );

    let _ = std::fs::remove_file(path);
}

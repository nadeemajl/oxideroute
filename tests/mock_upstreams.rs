//! Deterministic in-process mock upstream, with fault injection.
//!
//! Shared by the provider unit tests (included via `#[path]` into the library's
//! test build) and by the integration tests in `tests/`. It binds an ephemeral
//! localhost port, so the whole suite runs offline with no API keys.
//!
//! Fault modes are triggered by the `model` field of the request body:
//!
//! | model | behaviour |
//! |---|---|
//! | `gpt-4o-fast` (or any other) | 200 SSE, 3 chunks 5ms apart, then `[DONE]` |
//! | `gpt-4o-ping` | `: ping` comment, then the first data frame 20ms later |
//! | `gpt-4o-ratelimited` | immediate 429 |
//! | `gpt-4o-empty` | 200 with an empty body |
//! | `gpt-4o-json` | 200 non-streaming JSON with `usage` |
//! | `gpt-4o-dead` | sleeps 5s **before** headers (pre-header stall) |
//! | `gpt-4o-stalled-gpu` | 200 + `: ping`, then sleeps 5s before any data |
#![allow(dead_code)] // shared helper: individual test binaries use subsets

use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use axum::body::Body;
use axum::extract::State;
use axum::http::header::{HeaderValue, CONTENT_TYPE};
use axum::http::{HeaderMap, StatusCode, Uri};
use axum::response::Response;
use axum::routing::post;
use axum::Router;
use bytes::Bytes;

/// A request the mock observed, for assertions about what was forwarded.
#[derive(Debug, Clone)]
pub struct ReceivedRequest {
    /// Request path, e.g. `/v1/chat/completions`.
    pub path: String,
    /// Value of the `model` field, or empty when absent.
    pub model: String,
    /// Lower-cased header name to value pairs.
    pub headers: Vec<(String, String)>,
    /// Raw request body, byte-for-byte.
    pub body: Bytes,
}

#[derive(Debug, Default)]
struct MockState {
    received: Mutex<Vec<ReceivedRequest>>,
}

/// A running mock upstream.
#[derive(Debug, Clone)]
pub struct MockServer {
    addr: SocketAddr,
    state: Arc<MockState>,
}

impl MockServer {
    /// Binds an ephemeral port and starts serving in the background.
    pub async fn start() -> Self {
        let state = Arc::new(MockState::default());

        let app = Router::new()
            .route("/v1/chat/completions", post(handle))
            .route("/v1/messages", post(handle))
            .with_state(Arc::clone(&state));

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind ephemeral port");
        let addr = listener.local_addr().expect("read local addr");

        tokio::spawn(async move {
            // The server lives for the duration of the test process.
            let _ = axum::serve(listener, app).await;
        });

        Self { addr, state }
    }

    /// Base URL to hand to a provider config, including the `/v1` segment.
    pub fn base_url(&self) -> String {
        format!("http://{}/v1", self.addr)
    }

    /// Everything the mock has received so far, in arrival order.
    pub fn requests(&self) -> Vec<ReceivedRequest> {
        let guard = match self.state.received.lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        };
        guard.clone()
    }
}

async fn handle(
    uri: Uri,
    State(state): State<Arc<MockState>>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let model = serde_json::from_slice::<serde_json::Value>(&body)
        .ok()
        .and_then(|value| {
            value
                .get("model")
                .and_then(|m| m.as_str())
                .map(str::to_string)
        })
        .unwrap_or_default();

    let header_pairs = headers
        .iter()
        .filter_map(|(name, value)| {
            value
                .to_str()
                .ok()
                .map(|value| (name.as_str().to_string(), value.to_string()))
        })
        .collect();

    {
        let mut received = match state.received.lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        };
        received.push(ReceivedRequest {
            path: uri.path().to_string(),
            model: model.clone(),
            headers: header_pairs,
            body,
        });
    }

    match model.as_str() {
        "gpt-4o-ratelimited" => json_response(
            StatusCode::TOO_MANY_REQUESTS,
            serde_json::json!({
                "error": {"message": "slow down", "type": "rate_limit_error"}
            }),
        ),
        "gpt-4o-dead" => {
            // Pre-header stall: no status line for five seconds.
            tokio::time::sleep(Duration::from_secs(5)).await;
            sse_response(vec![(Duration::ZERO, data_frame("late"))])
        }
        "gpt-4o-stalled-gpu" => {
            // Post-header stall: headers plus a keep-alive, then silence.
            sse_response(vec![
                (Duration::ZERO, Bytes::from_static(b": ping\n\n")),
                (Duration::from_secs(5), data_frame("too late")),
            ])
        }
        "gpt-4o-empty" => sse_response(Vec::new()),
        "gpt-4o-json" => json_response(
            StatusCode::OK,
            serde_json::json!({
                "id": "chatcmpl-mock",
                "object": "chat.completion",
                "choices": [{"index": 0, "message": {"role": "assistant", "content": "ok"}}],
                "usage": {"prompt_tokens": 11, "completion_tokens": 7, "total_tokens": 18}
            }),
        ),
        "gpt-4o-ping" => sse_response(vec![
            (Duration::ZERO, Bytes::from_static(b": ping\n\n")),
            (Duration::from_millis(20), data_frame("Hello")),
            (Duration::ZERO, Bytes::from_static(b"data: [DONE]\n\n")),
        ]),
        _ => sse_response(vec![
            (Duration::ZERO, data_frame("Hello")),
            (Duration::from_millis(5), data_frame(" world")),
            (Duration::from_millis(5), data_frame("!")),
            (
                Duration::from_millis(5),
                Bytes::from_static(b"data: [DONE]\n\n"),
            ),
        ]),
    }
}

fn data_frame(content: &str) -> Bytes {
    let payload = serde_json::json!({
        "id": "chatcmpl-mock",
        "object": "chat.completion.chunk",
        "created": 0,
        "model": "mock",
        "choices": [{"index": 0, "delta": {"content": content}, "finish_reason": null}]
    });
    Bytes::from(format!("data: {payload}\n\n"))
}

fn sse_response(chunks: Vec<(Duration, Bytes)>) -> Response {
    let stream = futures_util::stream::unfold(chunks.into_iter(), |mut remaining| async move {
        match remaining.next() {
            Some((delay, chunk)) => {
                tokio::time::sleep(delay).await;
                Some((Ok::<Bytes, std::io::Error>(chunk), remaining))
            }
            None => None,
        }
    });

    let mut response = Response::new(Body::from_stream(stream));
    *response.status_mut() = StatusCode::OK;
    response
        .headers_mut()
        .insert(CONTENT_TYPE, HeaderValue::from_static("text/event-stream"));
    response
        .headers_mut()
        .insert("cache-control", HeaderValue::from_static("no-cache"));
    response
}

fn json_response(status: StatusCode, value: serde_json::Value) -> Response {
    let mut response = Response::new(Body::from(value.to_string()));
    *response.status_mut() = status;
    response
        .headers_mut()
        .insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
    response
}

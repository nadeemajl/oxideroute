//! Upstream provider adapters.
//!
//! Every provider speaks its own wire protocol but presents the same
//! [`LlmProvider`] interface to the executor. Adapters are responsible for
//! three things:
//!
//! 1. Translating the inbound OpenAI-shaped request into the upstream's format.
//! 2. Normalising the upstream's response back into OpenAI SSE frames.
//! 3. **Not** returning from [`LlmProvider::execute_stream`] until a first
//!    content frame is actually in hand — that is what lets the executor commit
//!    `HTTP 200` downstream only once the upstream has proven it will answer.

pub mod openai;

use std::pin::Pin;
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use futures_core::Stream;

use crate::error::RouterError;
use crate::proxy::inspector::ProxyRequest;

/// A boxed stream of SSE bytes, already normalised to OpenAI framing.
///
/// Erroring mid-stream is terminal: the executor forwards the error to the
/// client rather than attempting a fallback, because response headers have by
/// then already been committed.
pub type SseByteStream = Pin<Box<dyn Stream<Item = Result<Bytes, RouterError>> + Send + 'static>>;

/// A fully buffered non-streaming completion.
#[derive(Debug, Clone)]
pub struct NonStreamResponse {
    /// Upstream HTTP status.
    pub status: u16,
    /// Raw upstream JSON body.
    pub body: Bytes,
    /// Prompt tokens reported by the upstream, or `0` if absent.
    pub prompt_tokens: usize,
    /// Completion tokens reported by the upstream, or `0` if absent.
    pub completion_tokens: usize,
}

/// A single upstream that can answer chat completions.
#[async_trait]
pub trait LlmProvider: Send + Sync + 'static {
    /// Configuration key for this provider, e.g. `openai_us`.
    fn provider_id(&self) -> &str;

    /// Dispatches a streaming request.
    ///
    /// Must await the HTTP response headers **and** pull the first non-empty
    /// data frame before returning, so a stalled upstream can still be failed
    /// over before any bytes reach the client.
    async fn execute_stream(
        &self,
        upstream_model: &str,
        req: &ProxyRequest,
    ) -> Result<(Bytes, SseByteStream), RouterError>;

    /// Dispatches a non-streaming request.
    async fn execute_json(
        &self,
        upstream_model: &str,
        req: &ProxyRequest,
    ) -> Result<NonStreamResponse, RouterError>;
}

/// Builds the shared HTTP/2 client used by every upstream adapter.
///
/// Returns `Err` rather than panicking: `Client::build` fails when the TLS
/// backend cannot be initialised, and the request hot path must never panic.
///
/// # Note
///
/// `timeout_ms` is applied to the *whole* exchange, body included, so it also
/// bounds how long a single SSE stream may run. Providers that serve long
/// streams must raise their configured `timeout_ms` accordingly.
pub fn build_upstream_http_client(timeout_ms: u64) -> Result<reqwest::Client, RouterError> {
    reqwest::Client::builder()
        .use_rustls_tls()
        .http2_keep_alive_interval(Duration::from_secs(15))
        .http2_keep_alive_timeout(Duration::from_secs(5))
        .http2_keep_alive_while_idle(true)
        .http2_adaptive_window(true)
        .pool_max_idle_per_host(128)
        .tcp_nodelay(true)
        .timeout(Duration::from_millis(timeout_ms))
        .build()
        .map_err(|error| RouterError::Internal {
            message: format!("failed to build upstream HTTP client: {error}"),
        })
}

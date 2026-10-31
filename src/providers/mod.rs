//! Upstream provider adapters.
//!
//! Every provider speaks its own wire protocol but presents the same
//! [`LlmProvider`] interface to the executor. Adapters are responsible for
//! three things:
//!
//! 1. Translating the inbound OpenAI-shaped request into the upstream's format.
//! 2. Normalising the upstream's response back into OpenAI SSE frames.
//! 3. **Not** returning from [`LlmProvider::execute_stream`] until a first
//!    frame is actually in hand — that is what lets the executor commit
//!    `HTTP 200` downstream only once the upstream has proven it will answer.

pub mod anthropic;
pub mod gemini;
pub mod openai;
pub(crate) mod sse;

use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use futures_core::Stream;
use futures_util::StreamExt;

use crate::error::RouterError;
use crate::proxy::inspector::ProxyRequest;

/// Upper bound on connection establishment, regardless of `timeout_ms`.
pub const DEFAULT_CONNECT_TIMEOUT_MS: u64 = 10_000;

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
    /// frame before returning, so a stalled upstream can still be failed over
    /// before any bytes reach the client.
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
/// # Timeouts
///
/// Deliberately **no** total request timeout. `ClientBuilder::timeout` bounds
/// the entire exchange including the body, which would truncate a long SSE
/// stream mid-flight. Instead:
///
/// * `connect_timeout` bounds connection establishment (capped at 10s so a
///   misconfigured `timeout_ms` cannot stall a dial for a minute).
/// * `read_timeout` bounds the gap between successive reads — an idle timeout,
///   so a stalled stream fails fast without capping total stream duration.
/// * The pre-first-token watchdog in `proxy::executor` bounds time-to-first-byte.
pub fn build_upstream_http_client(timeout_ms: u64) -> Result<reqwest::Client, RouterError> {
    let connect_timeout = Duration::from_millis(timeout_ms.clamp(1, DEFAULT_CONNECT_TIMEOUT_MS));

    reqwest::Client::builder()
        .use_rustls_tls()
        .http2_keep_alive_interval(Duration::from_secs(15))
        .http2_keep_alive_timeout(Duration::from_secs(5))
        .http2_keep_alive_while_idle(true)
        .http2_adaptive_window(true)
        .pool_max_idle_per_host(128)
        .tcp_nodelay(true)
        .connect_timeout(connect_timeout)
        .read_timeout(Duration::from_millis(timeout_ms.max(1)))
        .build()
        .map_err(|error| RouterError::Internal {
            message: format!("failed to build upstream HTTP client: {error}"),
        })
}

/// Constructs the adapter for a configured provider.
///
/// # Errors
///
/// Returns an error for provider kinds without an adapter yet, and propagates
/// client-construction failures.
pub fn build_provider(
    id: &str,
    config: &crate::config::ProviderConfig,
) -> Result<Arc<dyn LlmProvider>, RouterError> {
    use crate::config::ProviderKind;

    let provider: Arc<dyn LlmProvider> = match config.kind {
        ProviderKind::Openai | ProviderKind::OpenaiCompatible => {
            Arc::new(openai::OpenAiProvider::new(id, config)?)
        }
        ProviderKind::Anthropic => Arc::new(anthropic::AnthropicProvider::new(id, config)?),
        ProviderKind::Gemini => Arc::new(gemini::GeminiProvider::new(id, config)?),
        ProviderKind::Bedrock => {
            return Err(RouterError::Config(
                crate::error::ConfigError::InvalidValue {
                    field: format!("providers.{id}.type"),
                    detail: "the Bedrock adapter is not implemented yet".to_string(),
                },
            ))
        }
    };

    Ok(provider)
}

/// Pulls from `stream` until a non-empty chunk arrives.
///
/// This is the pre-first-token gate: adapters call it before returning, so a
/// stream that has only produced keep-alives, or has already ended, fails over
/// to the next candidate instead of committing a downstream `200 OK`.
pub(crate) async fn await_first_frame(
    mut stream: SseByteStream,
    provider: &str,
) -> Result<(Bytes, SseByteStream), RouterError> {
    loop {
        match stream.next().await {
            Some(Ok(bytes)) if !bytes.is_empty() => return Ok((bytes, stream)),
            Some(Ok(_)) => continue,
            Some(Err(error)) => return Err(error),
            None => {
                return Err(RouterError::Transport {
                    provider: provider.to_string(),
                    message: "upstream closed the stream before producing any content frame"
                        .to_string(),
                })
            }
        }
    }
}

/// Flattens OpenAI message content into plain text.
///
/// Accepts both the plain-string form and the multimodal array-of-parts form,
/// where only `type = "text"` parts contribute.
pub(crate) fn openai_content_text(content: Option<&serde_json::Value>) -> String {
    let Some(content) = content else {
        return String::new();
    };

    if let Some(text) = content.as_str() {
        return text.to_string();
    }

    let Some(parts) = content.as_array() else {
        return String::new();
    };

    let mut out = String::new();
    for part in parts {
        if part.get("type").and_then(|kind| kind.as_str()) != Some("text") {
            continue;
        }
        if let Some(text) = part.get("text").and_then(|text| text.as_str()) {
            out.push_str(text);
        }
    }
    out
}

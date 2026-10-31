//! Anthropic `/v1/messages` adapter.
//!
//! Anthropic speaks a different request schema *and* a different SSE vocabulary
//! from OpenAI. The request side is a structural translation; the response side
//! is a state machine ([`AnthropicSseNormalizer`]) that rewrites each Anthropic
//! event into an OpenAI `chat.completion.chunk` and terminates with
//! `data: [DONE]`.
//!
//! `ping` events are swallowed, which matters for more than tidiness: they let
//! an upstream keep the socket warm while its GPU is saturated, and the TTFT
//! watchdog must not mistake them for progress.

use std::pin::Pin;
use std::task::{Context, Poll};

use async_trait::async_trait;
use bytes::Bytes;
use futures_core::Stream;
use futures_util::StreamExt;
use http::{HeaderMap, HeaderValue};
use reqwest::header::{ACCEPT, CONTENT_TYPE};
use serde_json::{json, Value};

use crate::config::{ProviderConfig, ProviderKind};
use crate::error::{ConfigError, RouterError};
use crate::providers::sse::{unix_now, ChunkBuilder, SseEvent, SseFrameParser, DONE_FRAME};
use crate::providers::{
    await_first_frame, build_upstream_http_client, openai_content_text, LlmProvider,
    NonStreamResponse, SseByteStream,
};
use crate::proxy::inspector::ProxyRequest;

/// Base URL used when an `anthropic` provider does not override it.
pub const DEFAULT_ANTHROPIC_BASE_URL: &str = "https://api.anthropic.com";

/// Anthropic requires an explicit API version header.
pub const ANTHROPIC_API_VERSION: &str = "2023-06-01";

/// `max_tokens` is mandatory for Anthropic; OpenAI treats it as optional.
pub const DEFAULT_MAX_TOKENS: u64 = 4096;

/// Adapter for Anthropic's Messages API.
pub struct AnthropicProvider {
    id: String,
    base_url: String,
    api_key: Option<String>,
    client: reqwest::Client,
}

impl AnthropicProvider {
    /// Builds an adapter from config.
    ///
    /// # Errors
    ///
    /// Fails if the provider kind is not `anthropic` or the HTTP client cannot
    /// be constructed.
    pub fn new(id: impl Into<String>, config: &ProviderConfig) -> Result<Self, RouterError> {
        let id = id.into();

        if config.kind != ProviderKind::Anthropic {
            return Err(RouterError::Config(ConfigError::InvalidValue {
                field: format!("providers.{id}.type"),
                detail: "the Anthropic adapter serves only `anthropic`".to_string(),
            }));
        }

        let base_url = config
            .base_url
            .as_deref()
            .unwrap_or(DEFAULT_ANTHROPIC_BASE_URL)
            .trim_end_matches('/')
            .to_string();

        Ok(Self {
            id,
            base_url,
            api_key: config.api_key().map(str::to_string),
            client: build_upstream_http_client(config.timeout_ms)?,
        })
    }

    /// Messages endpoint.
    pub fn endpoint(&self) -> String {
        format!("{}/v1/messages", self.base_url)
    }

    fn label(&self, upstream_model: &str) -> String {
        format!("{}/{}", self.id, upstream_model)
    }

    fn request_headers(&self) -> Result<HeaderMap, RouterError> {
        let mut headers = HeaderMap::new();
        headers.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
        headers.insert(ACCEPT, HeaderValue::from_static("text/event-stream"));
        headers.insert(
            "anthropic-version",
            HeaderValue::from_static(ANTHROPIC_API_VERSION),
        );

        let Some(api_key) = &self.api_key else {
            return Err(RouterError::Config(ConfigError::InvalidValue {
                field: format!("providers.{}.api_key", self.id),
                detail: "Anthropic requests require an api_key".to_string(),
            }));
        };

        let value = HeaderValue::from_str(api_key).map_err(|error| RouterError::Internal {
            message: format!("provider '{}' has an unusable api_key: {error}", self.id),
        })?;
        headers.insert("x-api-key", value);

        Ok(headers)
    }

    /// Converts an OpenAI chat request into an Anthropic Messages request.
    ///
    /// * `system`/`developer` messages are hoisted into the top-level `system`
    ///   string, which is where Anthropic expects them.
    /// * `stop` becomes `stop_sequences`.
    /// * `max_tokens` defaults to [`DEFAULT_MAX_TOKENS`], since Anthropic
    ///   requires it.
    pub fn translate_request(
        &self,
        upstream_model: &str,
        req: &ProxyRequest,
        stream: bool,
    ) -> Result<Bytes, RouterError> {
        let parsed: Value =
            serde_json::from_slice(&req.raw_body).map_err(|error| RouterError::InvalidRequest {
                message: format!("request body is not valid JSON: {error}"),
            })?;

        let messages = parsed
            .get("messages")
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default();

        let mut system_parts: Vec<String> = Vec::new();
        let mut converted: Vec<Value> = Vec::new();

        for message in &messages {
            let role = message
                .get("role")
                .and_then(Value::as_str)
                .unwrap_or("user");
            let text = openai_content_text(message.get("content"));

            match role {
                "system" | "developer" => {
                    if !text.is_empty() {
                        system_parts.push(text);
                    }
                }
                "assistant" => converted.push(json!({"role": "assistant", "content": text})),
                _ => converted.push(json!({"role": "user", "content": text})),
            }
        }

        if converted.is_empty() {
            return Err(RouterError::InvalidRequest {
                message: "Anthropic requests require at least one non-system message".to_string(),
            });
        }

        let mut body = serde_json::Map::new();
        body.insert("model".to_string(), json!(upstream_model));
        body.insert(
            "max_tokens".to_string(),
            parsed
                .get("max_tokens")
                .cloned()
                .unwrap_or_else(|| json!(DEFAULT_MAX_TOKENS)),
        );
        body.insert("messages".to_string(), Value::Array(converted));
        body.insert("stream".to_string(), json!(stream));

        if !system_parts.is_empty() {
            body.insert("system".to_string(), json!(system_parts.join("\n\n")));
        }

        for key in ["temperature", "top_p"] {
            if let Some(value) = parsed.get(key) {
                body.insert(key.to_string(), value.clone());
            }
        }

        if let Some(stop) = parsed.get("stop") {
            let sequences: Vec<Value> = match stop {
                Value::Array(items) => items.clone(),
                Value::String(single) => vec![json!(single)],
                _ => Vec::new(),
            };
            if !sequences.is_empty() {
                body.insert("stop_sequences".to_string(), Value::Array(sequences));
            }
        }

        serde_json::to_vec(&Value::Object(body))
            .map(Bytes::from)
            .map_err(|error| RouterError::Internal {
                message: format!("failed to encode Anthropic request: {error}"),
            })
    }
}

#[async_trait]
impl LlmProvider for AnthropicProvider {
    fn provider_id(&self) -> &str {
        &self.id
    }

    async fn execute_stream(
        &self,
        upstream_model: &str,
        req: &ProxyRequest,
    ) -> Result<(Bytes, SseByteStream), RouterError> {
        let body = self.translate_request(upstream_model, req, true)?;

        let response = self
            .client
            .post(self.endpoint())
            .headers(self.request_headers()?)
            .body(body)
            .send()
            .await
            .map_err(|error| RouterError::Transport {
                provider: self.label(upstream_model),
                message: error.to_string(),
            })?;

        let status = response.status();
        if !status.is_success() {
            return Err(RouterError::UpstreamHttp {
                provider: self.label(upstream_model),
                status: status.as_u16(),
            });
        }

        let label = self.label(upstream_model);
        let transport_label = label.clone();
        let raw: SseByteStream = Box::pin(response.bytes_stream().map(move |item| {
            item.map_err(|error| RouterError::Transport {
                provider: transport_label.clone(),
                message: error.to_string(),
            })
        }));

        let normalizer: SseByteStream = Box::pin(AnthropicSseNormalizer::new(
            raw,
            label.clone(),
            upstream_model.to_string(),
        ));

        await_first_frame(normalizer, &label).await
    }

    async fn execute_json(
        &self,
        upstream_model: &str,
        req: &ProxyRequest,
    ) -> Result<NonStreamResponse, RouterError> {
        let body = self.translate_request(upstream_model, req, false)?;

        let response = self
            .client
            .post(self.endpoint())
            .headers(self.request_headers()?)
            .body(body)
            .send()
            .await
            .map_err(|error| RouterError::Transport {
                provider: self.label(upstream_model),
                message: error.to_string(),
            })?;

        let status = response.status();
        if !status.is_success() {
            return Err(RouterError::UpstreamHttp {
                provider: self.label(upstream_model),
                status: status.as_u16(),
            });
        }

        let status_code = status.as_u16();
        let bytes = response
            .bytes()
            .await
            .map_err(|error| RouterError::Transport {
                provider: self.label(upstream_model),
                message: error.to_string(),
            })?;

        let parsed: Value =
            serde_json::from_slice(&bytes).map_err(|error| RouterError::Transport {
                provider: self.label(upstream_model),
                message: format!("malformed Anthropic completion: {error}"),
            })?;

        let text: String = parsed
            .get("content")
            .and_then(Value::as_array)
            .map(|blocks| {
                blocks
                    .iter()
                    .filter_map(|block| block.get("text").and_then(Value::as_str))
                    .collect::<Vec<_>>()
                    .join("")
            })
            .unwrap_or_default();

        let usage = parsed.get("usage");
        let prompt_tokens = usage
            .and_then(|usage| usage.get("input_tokens"))
            .and_then(Value::as_u64)
            .unwrap_or(0) as usize;
        let completion_tokens = usage
            .and_then(|usage| usage.get("output_tokens"))
            .and_then(Value::as_u64)
            .unwrap_or(0) as usize;

        let id = parsed
            .get("id")
            .and_then(Value::as_str)
            .unwrap_or("chatcmpl-anthropic");
        let model = parsed
            .get("model")
            .and_then(Value::as_str)
            .unwrap_or(upstream_model);
        let finish_reason = parsed
            .get("stop_reason")
            .and_then(Value::as_str)
            .unwrap_or("end_turn");

        let builder = ChunkBuilder::new(id, model, unix_now());
        let mut completion = builder.completion(&text, map_stop_reason(finish_reason));
        completion["usage"] = json!({
            "prompt_tokens": prompt_tokens,
            "completion_tokens": completion_tokens,
            "total_tokens": prompt_tokens + completion_tokens,
        });

        Ok(NonStreamResponse {
            status: status_code,
            body: Bytes::from(completion.to_string()),
            prompt_tokens,
            completion_tokens,
        })
    }
}

/// Maps Anthropic `stop_reason` values onto OpenAI `finish_reason` values.
fn map_stop_reason(reason: &str) -> &'static str {
    match reason {
        "max_tokens" => "length",
        "tool_use" => "tool_calls",
        "end_turn" | "stop_sequence" => "stop",
        _ => "stop",
    }
}

/// Rewrites Anthropic SSE events into OpenAI `chat.completion.chunk` frames.
///
/// The normalizer is `Unpin` (all fields are), so `poll_next` can operate on
/// `&mut self` without pin projection.
pub struct AnthropicSseNormalizer {
    inner: SseByteStream,
    parser: SseFrameParser,
    builder: ChunkBuilder,
    provider: String,
    prompt_tokens: usize,
    completion_tokens: usize,
    emitted: bool,
    done_sent: bool,
}

impl AnthropicSseNormalizer {
    /// Wraps an upstream byte stream, stamping frames with the current time.
    pub fn new(
        inner: SseByteStream,
        provider: impl Into<String>,
        model: impl Into<String>,
    ) -> Self {
        Self::new_at(inner, provider, model, unix_now())
    }

    /// Wraps an upstream byte stream with a fixed `created` timestamp.
    ///
    /// Tests use this to make frame output byte-for-byte deterministic.
    pub fn new_at(
        inner: SseByteStream,
        provider: impl Into<String>,
        model: impl Into<String>,
        created: u64,
    ) -> Self {
        Self {
            inner,
            parser: SseFrameParser::new(),
            builder: ChunkBuilder::new("chatcmpl-anthropic", model, created),
            provider: provider.into(),
            prompt_tokens: 0,
            completion_tokens: 0,
            emitted: false,
            done_sent: false,
        }
    }

    /// Converts one Anthropic event into zero or more OpenAI frames.
    fn translate(&mut self, event: SseEvent) -> Result<Bytes, RouterError> {
        let parsed: Value =
            serde_json::from_str(&event.data).map_err(|error| RouterError::Transport {
                provider: self.provider.clone(),
                message: format!("malformed Anthropic SSE payload: {error}"),
            })?;

        let event_type = parsed
            .get("type")
            .and_then(Value::as_str)
            .or(event.event.as_deref())
            .unwrap_or_default();

        match event_type {
            "message_start" => {
                if let Some(message) = parsed.get("message") {
                    if let Some(id) = message.get("id").and_then(Value::as_str) {
                        self.builder.set_id(id);
                    }
                    if let Some(model) = message.get("model").and_then(Value::as_str) {
                        self.builder.set_model(model);
                    }
                    self.prompt_tokens = message
                        .get("usage")
                        .and_then(|usage| usage.get("input_tokens"))
                        .and_then(Value::as_u64)
                        .unwrap_or(0) as usize;
                }
                self.emitted = true;
                Ok(self.builder.role_chunk())
            }
            "content_block_delta" => {
                let delta = parsed.get("delta");
                let is_text = delta
                    .and_then(|delta| delta.get("type"))
                    .and_then(Value::as_str)
                    == Some("text_delta");
                let text = delta
                    .and_then(|delta| delta.get("text"))
                    .and_then(Value::as_str)
                    .unwrap_or_default();

                if !is_text || text.is_empty() {
                    return Ok(Bytes::new());
                }

                self.emitted = true;
                Ok(self.builder.content_chunk(text))
            }
            "message_delta" => {
                let stop_reason = parsed
                    .get("delta")
                    .and_then(|delta| delta.get("stop_reason"))
                    .and_then(Value::as_str)
                    .unwrap_or("end_turn");

                if let Some(output) = parsed
                    .get("usage")
                    .and_then(|usage| usage.get("output_tokens"))
                    .and_then(Value::as_u64)
                {
                    self.completion_tokens = output as usize;
                }

                self.emitted = true;
                Ok(self.builder.finish_chunk(
                    map_stop_reason(stop_reason),
                    self.prompt_tokens,
                    self.completion_tokens,
                ))
            }
            "message_stop" => {
                self.emitted = true;
                self.done_sent = true;
                Ok(Bytes::from_static(DONE_FRAME))
            }
            "error" => {
                let message = parsed
                    .get("error")
                    .and_then(|error| error.get("message"))
                    .and_then(Value::as_str)
                    .unwrap_or("unknown Anthropic stream error");
                Err(RouterError::Transport {
                    provider: self.provider.clone(),
                    message: format!("upstream stream error: {message}"),
                })
            }
            // `ping`, `content_block_start`, `content_block_stop`, and anything
            // Anthropic adds later are intentionally ignored.
            _ => Ok(Bytes::new()),
        }
    }
}

impl Stream for AnthropicSseNormalizer {
    type Item = Result<Bytes, RouterError>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();

        loop {
            match this.inner.as_mut().poll_next(cx) {
                Poll::Pending => return Poll::Pending,

                Poll::Ready(Some(Err(error))) => return Poll::Ready(Some(Err(error))),

                Poll::Ready(Some(Ok(chunk))) => {
                    let events = this.parser.push(&chunk);
                    let mut output = Vec::new();

                    for event in events {
                        match this.translate(event) {
                            Ok(bytes) => output.extend_from_slice(&bytes),
                            Err(error) => return Poll::Ready(Some(Err(error))),
                        }
                    }

                    // Frames that produced nothing (pings) must not be surfaced,
                    // otherwise the TTFT gate would accept a keep-alive.
                    if !output.is_empty() {
                        return Poll::Ready(Some(Ok(Bytes::from(output))));
                    }
                }

                Poll::Ready(None) => {
                    let mut output = Vec::new();

                    if let Some(event) = this.parser.finish() {
                        match this.translate(event) {
                            Ok(bytes) => output.extend_from_slice(&bytes),
                            Err(error) => return Poll::Ready(Some(Err(error))),
                        }
                    }

                    // Only terminate a stream that actually produced frames;
                    // an empty upstream must surface as an error, not a bare
                    // `[DONE]`.
                    if !this.done_sent && this.emitted {
                        output.extend_from_slice(DONE_FRAME);
                        this.done_sent = true;
                    }

                    return if output.is_empty() {
                        Poll::Ready(None)
                    } else {
                        Poll::Ready(Some(Ok(Bytes::from(output))))
                    };
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mock_upstreams::MockServer;
    use crate::proxy::inspector::inspect_request;

    /// Raw Anthropic stream, including keep-alives and non-content blocks.
    const ANTHROPIC_FIXTURE: &[u8] = b"\
: ping\n\n\
event: ping\ndata: {\"type\":\"ping\"}\n\n\
event: message_start\ndata: {\"type\":\"message_start\",\"message\":{\"id\":\"msg_01ABC\",\"type\":\"message\",\"role\":\"assistant\",\"model\":\"claude-3-7-sonnet-latest\",\"usage\":{\"input_tokens\":17,\"output_tokens\":1}}}\n\n\
event: content_block_start\ndata: {\"type\":\"content_block_start\",\"index\":0,\"content_block\":{\"type\":\"text\",\"text\":\"\"}}\n\n\
event: ping\ndata: {\"type\":\"ping\"}\n\n\
event: content_block_delta\ndata: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"text_delta\",\"text\":\"Hello\"}}\n\n\
event: content_block_delta\ndata: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"text_delta\",\"text\":\" world\"}}\n\n\
event: content_block_stop\ndata: {\"type\":\"content_block_stop\",\"index\":0}\n\n\
event: message_delta\ndata: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"end_turn\",\"stop_sequence\":null},\"usage\":{\"output_tokens\":12}}\n\n\
event: message_stop\ndata: {\"type\":\"message_stop\"}\n\n";

    fn stream_from_chunks(chunks: Vec<Bytes>) -> SseByteStream {
        Box::pin(futures_util::stream::iter(
            chunks.into_iter().map(Ok::<Bytes, RouterError>),
        ))
    }

    fn normalizer_for(raw: &'static [u8]) -> AnthropicSseNormalizer {
        AnthropicSseNormalizer::new_at(
            stream_from_chunks(vec![Bytes::from_static(raw)]),
            "anthropic_prod/claude-3-7-sonnet-latest",
            "claude-fallback",
            0,
        )
    }

    async fn collect(mut stream: AnthropicSseNormalizer) -> Result<String, RouterError> {
        let mut out = Vec::new();
        while let Some(chunk) = stream.next().await {
            out.extend_from_slice(&chunk?);
        }
        Ok(String::from_utf8_lossy(&out).into_owned())
    }

    #[tokio::test]
    async fn translates_the_fixture_byte_for_byte() {
        let output = collect(normalizer_for(ANTHROPIC_FIXTURE))
            .await
            .expect("translation succeeds");

        let expected = concat!(
            "data: {\"id\":\"msg_01ABC\",\"object\":\"chat.completion.chunk\",\"created\":0,\"model\":\"claude-3-7-sonnet-latest\",\"choices\":[{\"index\":0,\"delta\":{\"role\":\"assistant\",\"content\":\"\"},\"finish_reason\":null}]}\n\n",
            "data: {\"id\":\"msg_01ABC\",\"object\":\"chat.completion.chunk\",\"created\":0,\"model\":\"claude-3-7-sonnet-latest\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"Hello\"},\"finish_reason\":null}]}\n\n",
            "data: {\"id\":\"msg_01ABC\",\"object\":\"chat.completion.chunk\",\"created\":0,\"model\":\"claude-3-7-sonnet-latest\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\" world\"},\"finish_reason\":null}]}\n\n",
            "data: {\"id\":\"msg_01ABC\",\"object\":\"chat.completion.chunk\",\"created\":0,\"model\":\"claude-3-7-sonnet-latest\",\"choices\":[{\"index\":0,\"delta\":{},\"finish_reason\":\"stop\"}],\"usage\":{\"prompt_tokens\":17,\"completion_tokens\":12,\"total_tokens\":29}}\n\n",
            "data: [DONE]\n\n",
        );

        assert_eq!(output, expected);
    }

    #[tokio::test]
    async fn survives_arbitrary_chunk_boundaries() {
        let whole = collect(normalizer_for(ANTHROPIC_FIXTURE))
            .await
            .expect("single chunk");

        // Split the fixture every 7 bytes, which lands inside frames and inside
        // multi-byte boundaries of the surrounding JSON.
        let chunks: Vec<Bytes> = ANTHROPIC_FIXTURE
            .chunks(7)
            .map(Bytes::copy_from_slice)
            .collect();
        let split = collect(AnthropicSseNormalizer::new_at(
            stream_from_chunks(chunks),
            "anthropic_prod/claude-3-7-sonnet-latest",
            "claude-fallback",
            0,
        ))
        .await
        .expect("split chunks");

        assert_eq!(whole, split);
    }

    #[tokio::test]
    async fn pings_alone_produce_no_frames() {
        let raw = b": ping\n\nevent: ping\ndata: {\"type\":\"ping\"}\n\n".to_vec();
        let stream = stream_from_chunks(vec![Bytes::from(raw)]);

        let output = collect(AnthropicSseNormalizer::new_at(stream, "p", "m", 0))
            .await
            .expect("no frames, no error");

        assert!(output.is_empty(), "pings must not emit frames: {output}");
    }

    #[tokio::test]
    async fn maps_max_tokens_to_length() {
        let raw = b"event: message_start\ndata: {\"type\":\"message_start\",\"message\":{\"id\":\"msg_x\",\"model\":\"m\",\"usage\":{\"input_tokens\":1}}}\n\nevent: message_delta\ndata: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"max_tokens\"},\"usage\":{\"output_tokens\":4096}}\n\n";
        let output = collect(normalizer_for(raw)).await.expect("translates");

        assert!(output.contains("\"finish_reason\":\"length\""));
        assert!(output.contains("\"completion_tokens\":4096"));
    }

    #[tokio::test]
    async fn maps_tool_use_to_tool_calls() {
        let raw = b"event: message_start\ndata: {\"type\":\"message_start\",\"message\":{\"id\":\"i\",\"model\":\"m\"}}\n\nevent: message_delta\ndata: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"tool_use\"}}\n\n";
        let output = collect(normalizer_for(raw)).await.expect("translates");

        assert!(output.contains("\"finish_reason\":\"tool_calls\""));
    }

    #[tokio::test]
    async fn ignores_non_text_deltas() {
        let raw = b"event: message_start\ndata: {\"type\":\"message_start\",\"message\":{\"id\":\"i\",\"model\":\"m\"}}\n\nevent: content_block_delta\ndata: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"input_json_delta\",\"partial_json\":\"{\\\"a\\\":\"}}\n\n";
        let output = collect(normalizer_for(raw)).await.expect("translates");

        assert!(!output.contains("partial_json"));
        assert_eq!(output.matches("data: ").count(), 2, "role + [DONE] only");
    }

    #[tokio::test]
    async fn surfaces_upstream_error_events() {
        let raw = b"event: error\ndata: {\"type\":\"error\",\"error\":{\"type\":\"overloaded_error\",\"message\":\"Overloaded\"}}\n\n";
        let result = collect(normalizer_for(raw)).await;

        let error = result.expect_err("error event must surface");
        assert!(error.to_string().contains("Overloaded"), "{error}");
    }

    #[tokio::test]
    async fn malformed_payload_is_an_error() {
        let raw = b"event: message_start\ndata: {not-json}\n\n";
        let result = collect(normalizer_for(raw)).await;

        assert!(result.is_err());
    }

    // ---------------------------------------------------------------- request translation

    fn provider() -> AnthropicProvider {
        let config = ProviderConfig {
            kind: ProviderKind::Anthropic,
            api_key: Some("sk-ant-test".to_string()),
            base_url: Some("https://api.anthropic.com".to_string()),
            timeout_ms: 5_000,
            ttft_timeout_ms: 1_000,
        };
        AnthropicProvider::new("anthropic_prod", &config).expect("provider builds")
    }

    fn request_for(json: serde_json::Value) -> ProxyRequest {
        inspect_request(HeaderMap::new(), Bytes::from(json.to_string())).expect("inspects")
    }

    #[test]
    fn hoists_system_messages_into_the_top_level_field() {
        let provider = provider();
        let request = request_for(json!({
            "model": "claude-3-7-sonnet-latest",
            "messages": [
                {"role": "system", "content": "Be brief."},
                {"role": "system", "content": "Prefer Rust."},
                {"role": "user", "content": "hi"},
                {"role": "assistant", "content": "hello"},
            ],
        }));

        let body = provider
            .translate_request("claude-3-7-sonnet-latest", &request, true)
            .expect("translates");
        let parsed: Value = serde_json::from_slice(&body).expect("valid JSON");

        assert_eq!(parsed["system"], "Be brief.\n\nPrefer Rust.");
        assert_eq!(parsed["messages"].as_array().expect("array").len(), 2);
        assert_eq!(parsed["messages"][0]["role"], "user");
        assert_eq!(parsed["messages"][1]["role"], "assistant");
        assert_eq!(parsed["stream"], true);
    }

    #[test]
    fn max_tokens_defaults_to_4096() {
        let provider = provider();
        let request = request_for(json!({
            "model": "claude-3-7-sonnet-latest",
            "messages": [{"role": "user", "content": "hi"}],
        }));

        let body = provider
            .translate_request("claude-3-7-sonnet-latest", &request, false)
            .expect("translates");
        let parsed: Value = serde_json::from_slice(&body).expect("valid JSON");

        assert_eq!(parsed["max_tokens"], DEFAULT_MAX_TOKENS);
        assert_eq!(parsed["stream"], false);
    }

    #[test]
    fn explicit_max_tokens_is_preserved() {
        let provider = provider();
        let request = request_for(json!({
            "model": "m",
            "max_tokens": 128,
            "messages": [{"role": "user", "content": "hi"}],
        }));

        let body = provider
            .translate_request("m", &request, false)
            .expect("translates");
        let parsed: Value = serde_json::from_slice(&body).expect("valid JSON");

        assert_eq!(parsed["max_tokens"], 128);
    }

    #[test]
    fn stop_becomes_stop_sequences() {
        let provider = provider();
        let request = request_for(json!({
            "model": "m",
            "messages": [{"role": "user", "content": "hi"}],
            "stop": ["END", "STOP"],
        }));

        let body = provider
            .translate_request("m", &request, false)
            .expect("translates");
        let parsed: Value = serde_json::from_slice(&body).expect("valid JSON");

        assert_eq!(parsed["stop_sequences"], json!(["END", "STOP"]));
    }

    #[test]
    fn single_string_stop_becomes_a_sequence() {
        let provider = provider();
        let request = request_for(json!({
            "model": "m",
            "messages": [{"role": "user", "content": "hi"}],
            "stop": "END",
        }));

        let body = provider
            .translate_request("m", &request, false)
            .expect("translates");
        let parsed: Value = serde_json::from_slice(&body).expect("valid JSON");

        assert_eq!(parsed["stop_sequences"], json!(["END"]));
    }

    #[test]
    fn multimodal_text_parts_are_concatenated() {
        let provider = provider();
        let request = request_for(json!({
            "model": "m",
            "messages": [{"role": "user", "content": [
                {"type": "text", "text": "one"},
                {"type": "image_url", "image_url": {"url": "http://x/y.png"}},
                {"type": "text", "text": "two"},
            ]}],
        }));

        let body = provider
            .translate_request("m", &request, false)
            .expect("translates");
        let parsed: Value = serde_json::from_slice(&body).expect("valid JSON");

        assert_eq!(parsed["messages"][0]["content"], "onetwo");
    }

    #[test]
    fn system_only_requests_are_rejected() {
        let provider = provider();
        let request = request_for(json!({
            "model": "m",
            "messages": [{"role": "system", "content": "only system"}],
        }));

        let error = provider
            .translate_request("m", &request, false)
            .expect_err("no user message");

        assert!(matches!(error, RouterError::InvalidRequest { .. }));
    }

    #[test]
    fn rejects_non_anthropic_provider_kinds() {
        let config = ProviderConfig {
            kind: ProviderKind::Openai,
            api_key: Some("sk".to_string()),
            base_url: None,
            timeout_ms: 1_000,
            ttft_timeout_ms: 500,
        };

        assert!(AnthropicProvider::new("openai", &config).is_err());
    }

    #[test]
    fn defaults_to_the_public_anthropic_endpoint() {
        let config = ProviderConfig {
            kind: ProviderKind::Anthropic,
            api_key: Some("sk".to_string()),
            base_url: None,
            timeout_ms: 1_000,
            ttft_timeout_ms: 500,
        };
        let provider = AnthropicProvider::new("anthropic", &config).expect("builds");

        assert_eq!(provider.endpoint(), "https://api.anthropic.com/v1/messages");
    }

    #[test]
    fn missing_api_key_is_rejected_at_request_time() {
        let config = ProviderConfig {
            kind: ProviderKind::Anthropic,
            api_key: None,
            base_url: None,
            timeout_ms: 1_000,
            ttft_timeout_ms: 500,
        };
        let provider = AnthropicProvider::new("anthropic", &config).expect("builds");

        assert!(provider.request_headers().is_err());
    }

    // ---------------------------------------------------------------- end to end

    #[tokio::test]
    async fn streams_from_an_anthropic_upstream_as_openai_frames() {
        let server = MockServer::start().await;
        let config = ProviderConfig {
            kind: ProviderKind::Anthropic,
            api_key: Some("sk-ant-test".to_string()),
            base_url: Some(server.root_url()),
            timeout_ms: 5_000,
            ttft_timeout_ms: 1_000,
        };
        let provider = AnthropicProvider::new("anthropic_prod", &config).expect("builds");
        let request = request_for(json!({
            "model": "claude-3-7-sonnet-latest",
            "stream": true,
            "messages": [{"role": "user", "content": "hi"}],
        }));

        let (first, rest) = provider
            .execute_stream("claude-3-7-sonnet-latest", &request)
            .await
            .expect("stream starts");

        let mut body = first.to_vec();
        body.extend(collect_bytes(rest).await);
        let rendered = String::from_utf8_lossy(&body).into_owned();

        assert!(rendered.starts_with("data: {"), "starts with a frame");
        assert!(rendered.contains("Hello"));
        assert!(rendered.contains(" from Claude"));
        assert!(rendered.ends_with("data: [DONE]\n\n"));

        let received = server.requests();
        let headers = &received.first().expect("one request").headers;
        assert!(
            headers
                .iter()
                .any(|(name, value)| name == "x-api-key" && value == "sk-ant-test"),
            "x-api-key missing: {headers:?}"
        );
        assert!(
            headers
                .iter()
                .any(|(name, value)| name == "anthropic-version" && value == ANTHROPIC_API_VERSION),
            "anthropic-version missing: {headers:?}"
        );
    }

    async fn collect_bytes(mut stream: SseByteStream) -> Vec<u8> {
        let mut out = Vec::new();
        while let Some(chunk) = stream.next().await {
            out.extend_from_slice(&chunk.expect("chunk"));
        }
        out
    }
}

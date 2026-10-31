//! Google Gemini adapter (`streamGenerateContent?alt=sse`).
//!
//! Gemini's SSE is simpler than Anthropic's: frames are `data:`-only with no
//! `event:` names and no explicit terminator, so `[DONE]` is synthesised when
//! the upstream stream closes.
//!
//! The API key travels in the query string rather than a header, and the model
//! is part of the URL path rather than the body.

use std::pin::Pin;
use std::task::{Context, Poll};

use async_trait::async_trait;
use bytes::Bytes;
use futures_core::Stream;
use futures_util::StreamExt;
use http::{HeaderMap, HeaderValue};
use reqwest::header::CONTENT_TYPE;
use serde_json::{json, Value};

use crate::config::{ProviderConfig, ProviderKind};
use crate::error::{ConfigError, RouterError};
use crate::providers::sse::{unix_now, ChunkBuilder, SseEvent, SseFrameParser, DONE_FRAME};
use crate::providers::{
    await_first_frame, build_upstream_http_client, openai_content_text, LlmProvider,
    NonStreamResponse, SseByteStream,
};
use crate::proxy::inspector::ProxyRequest;

/// Base URL used when a `gemini` provider does not override it.
pub const DEFAULT_GEMINI_BASE_URL: &str = "https://generativelanguage.googleapis.com";

/// Adapter for Google's Generative Language API.
pub struct GeminiProvider {
    id: String,
    base_url: String,
    api_key: Option<String>,
    client: reqwest::Client,
}

impl GeminiProvider {
    /// Builds an adapter from config.
    ///
    /// # Errors
    ///
    /// Fails if the provider kind is not `gemini` or the HTTP client cannot be
    /// constructed.
    pub fn new(id: impl Into<String>, config: &ProviderConfig) -> Result<Self, RouterError> {
        let id = id.into();

        if config.kind != ProviderKind::Gemini {
            return Err(RouterError::Config(ConfigError::InvalidValue {
                field: format!("providers.{id}.type"),
                detail: "the Gemini adapter serves only `gemini`".to_string(),
            }));
        }

        let base_url = config
            .base_url
            .as_deref()
            .unwrap_or(DEFAULT_GEMINI_BASE_URL)
            .trim_end_matches('/')
            .to_string();

        Ok(Self {
            id,
            base_url,
            api_key: config.api_key().map(str::to_string),
            client: build_upstream_http_client(config.timeout_ms)?,
        })
    }

    fn label(&self, upstream_model: &str) -> String {
        format!("{}/{}", self.id, upstream_model)
    }

    fn api_key(&self) -> Result<&str, RouterError> {
        self.api_key.as_deref().ok_or_else(|| {
            RouterError::Config(ConfigError::InvalidValue {
                field: format!("providers.{}.api_key", self.id),
                detail: "Gemini requests require an api_key".to_string(),
            })
        })
    }

    /// Streaming endpoint, with the key passed as a query parameter.
    pub fn stream_endpoint(&self, upstream_model: &str) -> Result<String, RouterError> {
        Ok(format!(
            "{}/v1beta/models/{}:streamGenerateContent?alt=sse&key={}",
            self.base_url,
            upstream_model,
            self.api_key()?
        ))
    }

    /// Non-streaming endpoint.
    pub fn generate_endpoint(&self, upstream_model: &str) -> Result<String, RouterError> {
        Ok(format!(
            "{}/v1beta/models/{}:generateContent?key={}",
            self.base_url,
            upstream_model,
            self.api_key()?
        ))
    }

    fn request_headers() -> HeaderMap {
        let mut headers = HeaderMap::new();
        headers.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
        headers
    }

    /// Converts an OpenAI chat request into a Gemini `generateContent` request.
    ///
    /// * `system`/`developer` messages become `systemInstruction`.
    /// * `assistant` becomes the `model` role.
    /// * `max_tokens` becomes `generationConfig.maxOutputTokens`, `stop`
    ///   becomes `stopSequences`.
    pub fn translate_request(
        &self,
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
        let mut contents: Vec<Value> = Vec::new();

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
                "assistant" => contents.push(json!({
                    "role": "model",
                    "parts": [{"text": text}],
                })),
                _ => contents.push(json!({
                    "role": "user",
                    "parts": [{"text": text}],
                })),
            }
        }

        if contents.is_empty() {
            return Err(RouterError::InvalidRequest {
                message: "Gemini requests require at least one non-system message".to_string(),
            });
        }

        let mut body = serde_json::Map::new();
        body.insert("contents".to_string(), Value::Array(contents));

        if !system_parts.is_empty() {
            body.insert(
                "systemInstruction".to_string(),
                json!({"parts": [{"text": system_parts.join("\n\n")}]}),
            );
        }

        let mut generation = serde_json::Map::new();
        if let Some(max_tokens) = parsed.get("max_tokens") {
            generation.insert("maxOutputTokens".to_string(), max_tokens.clone());
        }
        for (source, target) in [("temperature", "temperature"), ("top_p", "topP")] {
            if let Some(value) = parsed.get(source) {
                generation.insert(target.to_string(), value.clone());
            }
        }
        if let Some(stop) = parsed.get("stop") {
            let sequences: Vec<Value> = match stop {
                Value::Array(items) => items.clone(),
                Value::String(single) => vec![json!(single)],
                _ => Vec::new(),
            };
            if !sequences.is_empty() {
                generation.insert("stopSequences".to_string(), Value::Array(sequences));
            }
        }
        if !generation.is_empty() {
            body.insert("generationConfig".to_string(), Value::Object(generation));
        }

        // `stream` is implied by the URL path; kept for symmetry with the other
        // adapters' signatures.
        let _ = stream;

        serde_json::to_vec(&Value::Object(body))
            .map(Bytes::from)
            .map_err(|error| RouterError::Internal {
                message: format!("failed to encode Gemini request: {error}"),
            })
    }
}

#[async_trait]
impl LlmProvider for GeminiProvider {
    fn provider_id(&self) -> &str {
        &self.id
    }

    async fn execute_stream(
        &self,
        upstream_model: &str,
        req: &ProxyRequest,
    ) -> Result<(Bytes, SseByteStream), RouterError> {
        let body = self.translate_request(req, true)?;

        let response = self
            .client
            .post(self.stream_endpoint(upstream_model)?)
            .headers(Self::request_headers())
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

        let normalizer: SseByteStream = Box::pin(GeminiSseNormalizer::new(
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
        let body = self.translate_request(req, false)?;

        let response = self
            .client
            .post(self.generate_endpoint(upstream_model)?)
            .headers(Self::request_headers())
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
                message: format!("malformed Gemini completion: {error}"),
            })?;

        let text = candidate_text(&parsed);
        let finish_reason = candidate_finish_reason(&parsed).unwrap_or("STOP");
        let (prompt_tokens, completion_tokens) = usage_tokens(&parsed);

        let builder = ChunkBuilder::new(
            format!("chatcmpl-{}", uuid::Uuid::new_v4().simple()),
            upstream_model,
            unix_now(),
        );
        let mut completion = builder.completion(&text, map_finish_reason(finish_reason));
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

/// Maps Gemini `finishReason` values onto OpenAI `finish_reason` values.
fn map_finish_reason(reason: &str) -> &'static str {
    match reason {
        "MAX_TOKENS" => "length",
        "SAFETY" | "RECITATION" | "BLOCKLIST" | "PROHIBITED_CONTENT" => "content_filter",
        _ => "stop",
    }
}

/// Concatenates every text part of `candidates[0]`.
fn candidate_text(value: &Value) -> String {
    let Some(parts) = candidate_parts(value) else {
        return String::new();
    };

    let mut out = String::new();
    for part in parts {
        if let Some(text) = part.get("text").and_then(Value::as_str) {
            out.push_str(text);
        }
    }
    out
}

fn candidate_parts(value: &Value) -> Option<&Vec<Value>> {
    value
        .get("candidates")?
        .as_array()?
        .first()?
        .get("content")?
        .get("parts")?
        .as_array()
}

fn candidate_finish_reason(value: &Value) -> Option<&str> {
    value
        .get("candidates")?
        .as_array()?
        .first()?
        .get("finishReason")?
        .as_str()
}

fn usage_tokens(value: &Value) -> (usize, usize) {
    let usage = value.get("usageMetadata");
    let prompt = usage
        .and_then(|usage| usage.get("promptTokenCount"))
        .and_then(Value::as_u64)
        .unwrap_or(0) as usize;
    let completion = usage
        .and_then(|usage| usage.get("candidatesTokenCount"))
        .and_then(Value::as_u64)
        .unwrap_or(0) as usize;
    (prompt, completion)
}

/// Rewrites Gemini SSE frames into OpenAI `chat.completion.chunk` frames.
pub struct GeminiSseNormalizer {
    inner: SseByteStream,
    parser: SseFrameParser,
    builder: ChunkBuilder,
    provider: String,
    prompt_tokens: usize,
    completion_tokens: usize,
    finish_sent: bool,
    emitted: bool,
    done_sent: bool,
}

impl GeminiSseNormalizer {
    /// Wraps an upstream byte stream, stamping frames with the current time.
    pub fn new(
        inner: SseByteStream,
        provider: impl Into<String>,
        model: impl Into<String>,
    ) -> Self {
        Self::new_at(inner, provider, model, unix_now())
    }

    /// Wraps an upstream byte stream with a fixed `created` timestamp.
    pub fn new_at(
        inner: SseByteStream,
        provider: impl Into<String>,
        model: impl Into<String>,
        created: u64,
    ) -> Self {
        Self::with_id(
            inner,
            provider,
            model,
            created,
            format!("chatcmpl-{}", uuid::Uuid::new_v4().simple()),
        )
    }

    /// Wraps an upstream byte stream with a fixed id and timestamp.
    ///
    /// Gemini responses carry neither an id nor a model, so the adapter mints
    /// one; tests pin it to keep output byte-for-byte comparable.
    pub fn with_id(
        inner: SseByteStream,
        provider: impl Into<String>,
        model: impl Into<String>,
        created: u64,
        id: impl Into<String>,
    ) -> Self {
        Self {
            inner,
            parser: SseFrameParser::new(),
            builder: ChunkBuilder::new(id, model, created),
            provider: provider.into(),
            prompt_tokens: 0,
            completion_tokens: 0,
            finish_sent: false,
            emitted: false,
            done_sent: false,
        }
    }

    /// Converts one Gemini frame into zero or more OpenAI frames.
    fn translate(&mut self, event: SseEvent) -> Result<Bytes, RouterError> {
        let parsed: Value =
            serde_json::from_str(&event.data).map_err(|error| RouterError::Transport {
                provider: self.provider.clone(),
                message: format!("malformed Gemini SSE payload: {error}"),
            })?;

        let mut output = Vec::new();

        let (prompt_tokens, completion_tokens) = usage_tokens(&parsed);
        if prompt_tokens > 0 {
            self.prompt_tokens = prompt_tokens;
        }
        if completion_tokens > 0 {
            self.completion_tokens = completion_tokens;
        }

        let text = candidate_text(&parsed);
        if !text.is_empty() {
            self.emitted = true;
            output.extend_from_slice(&self.builder.content_chunk(&text));
        }

        if let Some(reason) = candidate_finish_reason(&parsed) {
            if !self.finish_sent && reason != "FINISH_REASON_UNSPECIFIED" {
                self.finish_sent = true;
                self.emitted = true;
                output.extend_from_slice(&self.builder.finish_chunk(
                    map_finish_reason(reason),
                    self.prompt_tokens,
                    self.completion_tokens,
                ));
            }
        }

        Ok(Bytes::from(output))
    }
}

impl Stream for GeminiSseNormalizer {
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

                    // Gemini has no terminator event, so `[DONE]` is emitted when
                    // the socket closes — but only for a stream that produced
                    // real frames.
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

    const GEMINI_FIXTURE: &[u8] = b"\
data: {\"candidates\":[{\"content\":{\"parts\":[{\"text\":\"Hello\"}],\"role\":\"model\"},\"index\":0}]}\n\n\
data: {\"candidates\":[{\"content\":{\"parts\":[{\"text\":\" from Gemini\"}],\"role\":\"model\"},\"finishReason\":\"STOP\",\"index\":0}],\"usageMetadata\":{\"promptTokenCount\":5,\"candidatesTokenCount\":7,\"totalTokenCount\":12}}\n\n";

    fn stream_from_chunks(chunks: Vec<Bytes>) -> SseByteStream {
        Box::pin(futures_util::stream::iter(
            chunks.into_iter().map(Ok::<Bytes, RouterError>),
        ))
    }

    fn normalizer_for(raw: &'static [u8]) -> GeminiSseNormalizer {
        GeminiSseNormalizer::with_id(
            stream_from_chunks(vec![Bytes::from_static(raw)]),
            "gemini_vertex/gemini-2.5-pro",
            "gemini-2.5-pro",
            0,
            "chatcmpl-test",
        )
    }

    async fn collect(mut stream: GeminiSseNormalizer) -> Result<String, RouterError> {
        let mut out = Vec::new();
        while let Some(chunk) = stream.next().await {
            out.extend_from_slice(&chunk?);
        }
        Ok(String::from_utf8_lossy(&out).into_owned())
    }

    #[tokio::test]
    async fn translates_the_fixture_into_openai_frames() {
        let output = collect(normalizer_for(GEMINI_FIXTURE))
            .await
            .expect("translates");

        assert!(output.contains("\"object\":\"chat.completion.chunk\""));
        assert!(output.contains("\"delta\":{\"content\":\"Hello\"}"));
        assert!(output.contains("\"delta\":{\"content\":\" from Gemini\"}"));
        assert!(output.contains("\"finish_reason\":\"stop\""));
        assert!(output.contains("\"completion_tokens\":7"));
        assert!(output.ends_with("data: [DONE]\n\n"));
    }

    #[tokio::test]
    async fn survives_arbitrary_chunk_boundaries() {
        let whole = collect(normalizer_for(GEMINI_FIXTURE))
            .await
            .expect("single chunk");

        let chunks: Vec<Bytes> = GEMINI_FIXTURE
            .chunks(5)
            .map(Bytes::copy_from_slice)
            .collect();
        let split = collect(GeminiSseNormalizer::with_id(
            stream_from_chunks(chunks),
            "gemini_vertex/gemini-2.5-pro",
            "gemini-2.5-pro",
            0,
            "chatcmpl-test",
        ))
        .await
        .expect("split chunks");

        assert_eq!(whole, split);
    }

    #[tokio::test]
    async fn emits_done_exactly_once() {
        let output = collect(normalizer_for(GEMINI_FIXTURE))
            .await
            .expect("translates");

        assert_eq!(output.matches("data: [DONE]").count(), 1);
    }

    #[tokio::test]
    async fn empty_candidates_produce_no_frames() {
        let raw = b"data: {\"candidates\":[]}\n\n".to_vec();
        let stream = stream_from_chunks(vec![Bytes::from(raw)]);

        let output = collect(GeminiSseNormalizer::new_at(stream, "g", "m", 0))
            .await
            .expect("no frames");

        assert!(output.is_empty(), "unexpected output: {output}");
    }

    #[tokio::test]
    async fn maps_max_tokens_finish_reason() {
        let raw = b"data: {\"candidates\":[{\"content\":{\"parts\":[{\"text\":\"x\"}]},\"finishReason\":\"MAX_TOKENS\"}]}\n\n";
        let output = collect(normalizer_for(raw)).await.expect("translates");

        assert!(output.contains("\"finish_reason\":\"length\""));
    }

    #[tokio::test]
    async fn maps_safety_finish_reason_to_content_filter() {
        let raw = b"data: {\"candidates\":[{\"content\":{\"parts\":[{\"text\":\"x\"}]},\"finishReason\":\"SAFETY\"}]}\n\n";
        let output = collect(normalizer_for(raw)).await.expect("translates");

        assert!(output.contains("\"finish_reason\":\"content_filter\""));
    }

    #[tokio::test]
    async fn malformed_payload_is_an_error() {
        let raw = b"data: {oops}\n\n";
        assert!(collect(normalizer_for(raw)).await.is_err());
    }

    // ---------------------------------------------------------------- request translation

    fn provider_for_base(base_url: &str) -> GeminiProvider {
        let config = ProviderConfig {
            kind: ProviderKind::Gemini,
            api_key: Some("gm-key".to_string()),
            base_url: Some(base_url.to_string()),
            timeout_ms: 5_000,
            ttft_timeout_ms: 1_000,
        };
        GeminiProvider::new("gemini_vertex", &config).expect("provider builds")
    }

    fn provider() -> GeminiProvider {
        provider_for_base(DEFAULT_GEMINI_BASE_URL)
    }

    fn request_for(json: serde_json::Value) -> ProxyRequest {
        inspect_request(HeaderMap::new(), Bytes::from(json.to_string())).expect("inspects")
    }

    #[test]
    fn system_messages_become_system_instruction() {
        let provider = provider();
        let request = request_for(json!({
            "model": "gemini-2.5-pro",
            "messages": [
                {"role": "system", "content": "Be terse."},
                {"role": "user", "content": "hi"},
            ],
        }));

        let body = provider
            .translate_request(&request, false)
            .expect("translates");
        let parsed: Value = serde_json::from_slice(&body).expect("valid JSON");

        assert_eq!(parsed["systemInstruction"]["parts"][0]["text"], "Be terse.");
        assert_eq!(parsed["contents"][0]["role"], "user");
    }

    #[test]
    fn assistant_maps_to_the_model_role() {
        let provider = provider();
        let request = request_for(json!({
            "model": "m",
            "messages": [
                {"role": "user", "content": "hi"},
                {"role": "assistant", "content": "hello"},
            ],
        }));

        let body = provider
            .translate_request(&request, false)
            .expect("translates");
        let parsed: Value = serde_json::from_slice(&body).expect("valid JSON");

        assert_eq!(parsed["contents"][0]["role"], "user");
        assert_eq!(parsed["contents"][1]["role"], "model");
        assert_eq!(parsed["contents"][1]["parts"][0]["text"], "hello");
    }

    #[test]
    fn generation_config_maps_max_tokens_and_stop() {
        let provider = provider();
        let request = request_for(json!({
            "model": "m",
            "max_tokens": 64,
            "temperature": 0.2,
            "top_p": 0.9,
            "stop": ["END"],
            "messages": [{"role": "user", "content": "hi"}],
        }));

        let body = provider
            .translate_request(&request, false)
            .expect("translates");
        let parsed: Value = serde_json::from_slice(&body).expect("valid JSON");

        assert_eq!(parsed["generationConfig"]["maxOutputTokens"], 64);
        assert_eq!(parsed["generationConfig"]["temperature"], 0.2);
        assert_eq!(parsed["generationConfig"]["topP"], 0.9);
        assert_eq!(parsed["generationConfig"]["stopSequences"], json!(["END"]));
    }

    #[test]
    fn system_only_requests_are_rejected() {
        let provider = provider();
        let request = request_for(json!({
            "model": "m",
            "messages": [{"role": "system", "content": "only"}],
        }));

        assert!(provider.translate_request(&request, false).is_err());
    }

    #[test]
    fn rejects_non_gemini_provider_kinds() {
        let config = ProviderConfig {
            kind: ProviderKind::Anthropic,
            api_key: Some("sk".to_string()),
            base_url: None,
            timeout_ms: 1_000,
            ttft_timeout_ms: 500,
        };

        assert!(GeminiProvider::new("anthropic", &config).is_err());
    }

    #[test]
    fn streaming_endpoint_embeds_model_and_key() {
        let provider = provider();

        assert_eq!(
            provider.stream_endpoint("gemini-2.5-pro").expect("endpoint"),
            "https://generativelanguage.googleapis.com/v1beta/models/gemini-2.5-pro:streamGenerateContent?alt=sse&key=gm-key"
        );
    }

    #[test]
    fn missing_api_key_is_rejected() {
        let config = ProviderConfig {
            kind: ProviderKind::Gemini,
            api_key: None,
            base_url: None,
            timeout_ms: 1_000,
            ttft_timeout_ms: 500,
        };
        let provider = GeminiProvider::new("gemini", &config).expect("builds");

        assert!(provider.stream_endpoint("m").is_err());
    }

    // ---------------------------------------------------------------- end to end

    #[tokio::test]
    async fn streams_from_a_gemini_upstream_as_openai_frames() {
        let server = MockServer::start().await;
        let provider = provider_for_base(&server.root_url());
        let request = request_for(json!({
            "model": "gemini-2.5-pro",
            "stream": true,
            "messages": [{"role": "user", "content": "hi"}],
        }));

        let (first, mut rest) = provider
            .execute_stream("gemini-2.5-pro", &request)
            .await
            .expect("stream starts");

        let mut body = first.to_vec();
        while let Some(chunk) = rest.next().await {
            body.extend_from_slice(&chunk.expect("chunk"));
        }
        let rendered = String::from_utf8_lossy(&body).into_owned();

        assert!(rendered.contains("Hello"));
        assert!(rendered.contains(" from Gemini"));
        assert!(rendered.ends_with("data: [DONE]\n\n"));
    }

    #[test]
    fn usage_tokens_reads_metadata() {
        let value: Value = serde_json::from_str(
            r#"{"usageMetadata":{"promptTokenCount":3,"candidatesTokenCount":4}}"#,
        )
        .expect("json");

        assert_eq!(usage_tokens(&value), (3, 4));
        assert_eq!(usage_tokens(&json!({})), (0, 0));
    }
}

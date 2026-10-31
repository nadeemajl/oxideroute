//! OpenAI and OpenAI-compatible (`vLLM`, `Ollama`, `LM Studio`) adapter.
//!
//! This adapter is a pass-through with two surgical exceptions:
//!
//! * The request body is forwarded as the original [`Bytes`] — a refcount bump,
//!   not a copy and not a re-serialization — whenever the resolved upstream
//!   model equals the model the client asked for.
//! * When routing rewrote the model, only the `model` string literal is spliced,
//!   leaving key order, whitespace, and unknown vendor fields byte-identical.
//!
//! Streaming never returns until a real `data:` frame has arrived, so `: ping`
//! keep-alives and empty frames cannot trick the executor into committing a
//! downstream `200 OK` for an upstream that is actually stalled.

use async_trait::async_trait;
use bytes::Bytes;
use futures_util::StreamExt;
use http::{HeaderMap, HeaderValue};
use reqwest::header::{ACCEPT, AUTHORIZATION, CONTENT_TYPE};

use crate::config::{ProviderConfig, ProviderKind};
use crate::error::{ConfigError, RouterError};
use crate::providers::{build_upstream_http_client, LlmProvider, NonStreamResponse, SseByteStream};
use crate::proxy::inspector::ProxyRequest;

/// Base URL used when an `openai` provider does not override it.
pub const DEFAULT_OPENAI_BASE_URL: &str = "https://api.openai.com/v1";

/// Adapter for OpenAI's API and any wire-compatible server.
pub struct OpenAiProvider {
    id: String,
    base_url: String,
    api_key: Option<String>,
    client: reqwest::Client,
}

impl OpenAiProvider {
    /// Builds an adapter from config.
    ///
    /// # Errors
    ///
    /// Fails if the provider kind is not OpenAI-compatible or the shared HTTP
    /// client cannot be constructed.
    pub fn new(id: impl Into<String>, config: &ProviderConfig) -> Result<Self, RouterError> {
        let id = id.into();

        if !matches!(
            config.kind,
            ProviderKind::Openai | ProviderKind::OpenaiCompatible
        ) {
            return Err(RouterError::Config(ConfigError::InvalidValue {
                field: format!("providers.{id}.type"),
                detail: "the OpenAI adapter serves only `openai` and `openai_compatible`"
                    .to_string(),
            }));
        }

        let base_url = config
            .base_url
            .as_deref()
            .unwrap_or(DEFAULT_OPENAI_BASE_URL)
            .trim_end_matches('/')
            .to_string();

        Ok(Self {
            id,
            base_url,
            api_key: config.api_key().map(str::to_string),
            client: build_upstream_http_client(config.timeout_ms)?,
        })
    }

    /// Chat-completions endpoint on the configured base URL.
    pub fn endpoint(&self) -> String {
        format!("{}/chat/completions", self.base_url)
    }

    /// Embeddings endpoint on the configured base URL.
    pub fn embeddings_endpoint(&self) -> String {
        format!("{}/embeddings", self.base_url)
    }

    /// `provider/model` label used in errors and attempt records.
    fn label(&self, upstream_model: &str) -> String {
        format!("{}/{}", self.id, upstream_model)
    }

    fn request_headers(&self, streaming: bool) -> Result<HeaderMap, RouterError> {
        let mut headers = HeaderMap::new();
        headers.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
        if streaming {
            headers.insert(ACCEPT, HeaderValue::from_static("text/event-stream"));
        }

        if let Some(api_key) = &self.api_key {
            let value = HeaderValue::from_str(&format!("Bearer {api_key}")).map_err(|error| {
                RouterError::Internal {
                    message: format!("provider '{}' has an unusable api_key: {error}", self.id),
                }
            })?;
            headers.insert(AUTHORIZATION, value);
        }

        Ok(headers)
    }

    fn transport_error(&self, upstream_model: &str, error: &reqwest::Error) -> RouterError {
        RouterError::Transport {
            provider: self.label(upstream_model),
            message: error.to_string(),
        }
    }

    /// Chooses between the zero-copy path and a targeted model rewrite.
    fn build_body(&self, upstream_model: &str, req: &ProxyRequest) -> Result<Bytes, RouterError> {
        if upstream_model == req.requested_model {
            // `Bytes::clone` is a refcount bump: no allocation, no copy.
            return Ok(req.raw_body.clone());
        }
        rewrite_model(&req.raw_body, upstream_model)
    }
}

#[async_trait]
impl LlmProvider for OpenAiProvider {
    fn provider_id(&self) -> &str {
        &self.id
    }

    async fn execute_stream(
        &self,
        upstream_model: &str,
        req: &ProxyRequest,
    ) -> Result<(Bytes, SseByteStream), RouterError> {
        let body = self.build_body(upstream_model, req)?;

        let response = self
            .client
            .post(self.endpoint())
            .headers(self.request_headers(true)?)
            .body(body)
            .send()
            .await
            .map_err(|error| self.transport_error(upstream_model, &error))?;

        let status = response.status();
        if !status.is_success() {
            return Err(RouterError::UpstreamHttp {
                provider: self.label(upstream_model),
                status: status.as_u16(),
            });
        }

        let mut stream = response.bytes_stream();
        let mut buffered: Vec<u8> = Vec::with_capacity(512);

        while let Some(chunk) = stream.next().await {
            let chunk = chunk.map_err(|error| self.transport_error(upstream_model, &error))?;
            buffered.extend_from_slice(&chunk);

            // Keep pulling until a complete `data:` line exists; comments and
            // blank lines are dropped, which is what makes the TTFT watchdog
            // immune to `: ping` keep-alives.
            if let Some(offset) = first_data_offset(&buffered) {
                let first = Bytes::copy_from_slice(&buffered[offset..]);
                let provider = self.label(upstream_model);
                let rest = stream.map(move |item| {
                    item.map_err(|error| RouterError::Transport {
                        provider: provider.clone(),
                        message: error.to_string(),
                    })
                });
                return Ok((first, Box::pin(rest)));
            }
        }

        Err(RouterError::Transport {
            provider: self.label(upstream_model),
            message: "upstream closed the stream before sending any data frame".to_string(),
        })
    }

    async fn execute_json(
        &self,
        upstream_model: &str,
        req: &ProxyRequest,
    ) -> Result<NonStreamResponse, RouterError> {
        let body = self.build_body(upstream_model, req)?;

        let response = self
            .client
            .post(self.endpoint())
            .headers(self.request_headers(false)?)
            .body(body)
            .send()
            .await
            .map_err(|error| self.transport_error(upstream_model, &error))?;

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
            .map_err(|error| self.transport_error(upstream_model, &error))?;
        let (prompt_tokens, completion_tokens) = usage_from(&bytes);

        Ok(NonStreamResponse {
            status: status_code,
            body: bytes,
            prompt_tokens,
            completion_tokens,
        })
    }

    async fn execute_embeddings(
        &self,
        upstream_model: &str,
        req: &ProxyRequest,
    ) -> Result<NonStreamResponse, RouterError> {
        // Same pass-through rules as chat: identical model means the original
        // bytes are forwarded, otherwise only the model literal is spliced.
        let body = self.build_body(upstream_model, req)?;

        let response = self
            .client
            .post(self.embeddings_endpoint())
            .headers(self.request_headers(false)?)
            .body(body)
            .send()
            .await
            .map_err(|error| self.transport_error(upstream_model, &error))?;

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
            .map_err(|error| self.transport_error(upstream_model, &error))?;
        let (prompt_tokens, completion_tokens) = usage_from(&bytes);

        Ok(NonStreamResponse {
            status: status_code,
            body: bytes,
            prompt_tokens,
            completion_tokens,
        })
    }
}

/// Returns the byte offset where the first complete `data:` line begins.
///
/// Comment lines (`: ping`), blank lines, and `event:` lines are skipped. An
/// unterminated trailing line yields `None` so the caller keeps reading rather
/// than committing a partial frame.
fn first_data_offset(buffer: &[u8]) -> Option<usize> {
    let mut line_start = 0usize;

    while line_start < buffer.len() {
        let remaining = buffer.get(line_start..)?;
        let relative_end = remaining.iter().position(|byte| *byte == b'\n')?;
        let line_end = line_start + relative_end;
        let line = buffer.get(line_start..line_end)?;

        // Strip the CR of a CRLF terminator before matching.
        let line = match line.last() {
            Some(b'\r') => line.get(..line.len().saturating_sub(1)).unwrap_or(line),
            _ => line,
        };

        if line.starts_with(b"data:") {
            return Some(line_start);
        }

        line_start = line_end + 1;
    }

    None
}

/// Replaces only the value of the top-level `model` key.
///
/// Byte-splicing keeps key order and vendor extensions intact; re-serializing
/// through a DOM would reorder the payload and defeat the pass-through design.
fn rewrite_model(raw: &Bytes, upstream_model: &str) -> Result<Bytes, RouterError> {
    let span = find_top_level_model_value(raw).ok_or_else(|| RouterError::InvalidRequest {
        message: "cannot rewrite `model`: request body has no top-level `model` string".to_string(),
    })?;

    let escaped = escape_json_string(upstream_model);
    let mut out = Vec::with_capacity(raw.len() + escaped.len());
    out.extend_from_slice(&raw[..span.start]);
    out.extend_from_slice(escaped.as_bytes());
    out.extend_from_slice(&raw[span.end..]);
    Ok(Bytes::from(out))
}

/// Locates the character range *inside* the quotes of the top-level `model` value.
fn find_top_level_model_value(raw: &[u8]) -> Option<std::ops::Range<usize>> {
    let mut depth: i32 = 0;
    let mut index = 0usize;

    while let Some(byte) = raw.get(index) {
        match byte {
            b'{' | b'[' => {
                depth += 1;
                index += 1;
            }
            b'}' | b']' => {
                depth -= 1;
                index += 1;
            }
            b'"' => {
                let (key_start, key_end, after_key) = scan_string(raw, index)?;

                let mut lookahead = after_key;
                while raw.get(lookahead).is_some_and(u8::is_ascii_whitespace) {
                    lookahead += 1;
                }

                let is_key = raw.get(lookahead) == Some(&b':');
                if is_key && depth == 1 && raw.get(key_start..key_end)? == b"model" {
                    let mut value_start = lookahead + 1;
                    while raw.get(value_start).is_some_and(u8::is_ascii_whitespace) {
                        value_start += 1;
                    }
                    if raw.get(value_start) != Some(&b'"') {
                        return None;
                    }
                    let (value_start, value_end, _) = scan_string(raw, value_start)?;
                    return Some(value_start..value_end);
                }

                index = after_key;
            }
            _ => index += 1,
        }
    }

    None
}

/// Scans one JSON string literal starting at `open_quote`.
///
/// Returns `(content_start, content_end, index_after_closing_quote)`.
fn scan_string(raw: &[u8], open_quote: usize) -> Option<(usize, usize, usize)> {
    if raw.get(open_quote) != Some(&b'"') {
        return None;
    }

    let mut index = open_quote + 1;
    while let Some(byte) = raw.get(index) {
        match byte {
            b'\\' => index += 2,
            b'"' => return Some((open_quote + 1, index, index + 1)),
            _ => index += 1,
        }
    }

    None
}

fn escape_json_string(input: &str) -> String {
    let mut out = String::with_capacity(input.len());
    for character in input.chars() {
        match character {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            control if (control as u32) < 0x20 => {
                out.push_str(&format!("\\u{:04x}", control as u32));
            }
            other => out.push(other),
        }
    }
    out
}

/// Extracts `usage.{prompt,completion}_tokens`, defaulting to zero.
fn usage_from(body: &[u8]) -> (usize, usize) {
    let Ok(parsed) = serde_json::from_slice::<serde_json::Value>(body) else {
        return (0, 0);
    };
    let Some(usage) = parsed.get("usage") else {
        return (0, 0);
    };

    let prompt = usage
        .get("prompt_tokens")
        .and_then(serde_json::Value::as_u64)
        .unwrap_or(0) as usize;
    let completion = usage
        .get("completion_tokens")
        .and_then(serde_json::Value::as_u64)
        .unwrap_or(0) as usize;

    (prompt, completion)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mock_upstreams::MockServer;
    use crate::proxy::inspector::inspect_request;

    fn provider_for(server: &MockServer, timeout_ms: u64) -> OpenAiProvider {
        let config = ProviderConfig {
            kind: ProviderKind::OpenaiCompatible,
            api_key: None,
            base_url: Some(server.base_url()),
            timeout_ms,
            ttft_timeout_ms: 1_000,
        };
        OpenAiProvider::new("mock", &config).expect("provider builds")
    }

    fn proxy_request(model: &str, stream: bool) -> ProxyRequest {
        let body = serde_json::json!({
            "model": model,
            "stream": stream,
            "messages": [{"role": "user", "content": "hi"}],
        })
        .to_string();
        inspect_request(HeaderMap::new(), Bytes::from(body)).expect("request inspects")
    }

    async fn collect(mut stream: SseByteStream) -> Vec<u8> {
        let mut out = Vec::new();
        while let Some(chunk) = stream.next().await {
            out.extend_from_slice(&chunk.expect("stream chunk"));
        }
        out
    }

    // ---------------------------------------------------------------- streaming

    #[tokio::test]
    async fn returns_first_data_frame_as_the_first_chunk() {
        let server = MockServer::start().await;
        let provider = provider_for(&server, 5_000);
        let request = proxy_request("gpt-4o-fast", true);

        let (first, rest) = provider
            .execute_stream("gpt-4o-fast", &request)
            .await
            .expect("stream starts");

        assert!(
            first.starts_with(b"data:"),
            "first chunk must be a data frame, got {:?}",
            String::from_utf8_lossy(&first)
        );
        assert!(first.windows(5).any(|w| w == b"Hello"));

        let remainder = collect(rest).await;
        let full = [first.as_ref(), remainder.as_slice()].concat();

        assert!(full.windows(6).any(|w| w == b" world"));
        assert!(full.windows(7).any(|w| w == b" [DONE]"));
    }

    #[tokio::test]
    async fn skips_ping_comments_before_the_first_data_frame() {
        let server = MockServer::start().await;
        let provider = provider_for(&server, 5_000);
        let request = proxy_request("gpt-4o-ping", true);

        let (first, _rest) = provider
            .execute_stream("gpt-4o-ping", &request)
            .await
            .expect("stream starts");

        let rendered = String::from_utf8_lossy(&first);
        assert!(first.starts_with(b"data:"), "got {rendered}");
        assert!(
            !rendered.contains(": ping"),
            "ping leaked downstream: {rendered}"
        );
    }

    #[tokio::test]
    async fn empty_upstream_body_is_a_transport_error() {
        let server = MockServer::start().await;
        let provider = provider_for(&server, 5_000);
        let request = proxy_request("gpt-4o-empty", true);

        let error = provider
            .execute_stream("gpt-4o-empty", &request)
            .await
            .err()
            .expect("no data frame means no usable stream");

        assert!(matches!(error, RouterError::Transport { .. }), "{error:?}");
    }

    #[tokio::test]
    async fn non_success_status_maps_to_upstream_http() {
        let server = MockServer::start().await;
        let provider = provider_for(&server, 5_000);
        let request = proxy_request("gpt-4o-ratelimited", true);

        let error = provider
            .execute_stream("gpt-4o-ratelimited", &request)
            .await
            .err()
            .expect("429 must not be treated as a stream");

        match error {
            RouterError::UpstreamHttp { provider, status } => {
                assert_eq!(status, 429);
                assert_eq!(provider, "mock/gpt-4o-ratelimited");
            }
            other => panic!("unexpected error: {other:?}"),
        }
    }

    #[tokio::test]
    async fn unreachable_upstream_maps_to_transport_error() {
        // Bind then drop to obtain a port that is closed but syntactically valid.
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind");
        let addr = listener.local_addr().expect("addr");
        drop(listener);

        let config = ProviderConfig {
            kind: ProviderKind::OpenaiCompatible,
            api_key: None,
            base_url: Some(format!("http://{addr}/v1")),
            timeout_ms: 2_000,
            ttft_timeout_ms: 500,
        };
        let provider = OpenAiProvider::new("down", &config).expect("provider builds");
        let request = proxy_request("gpt-4o", true);

        let error = provider
            .execute_stream("gpt-4o", &request)
            .await
            .err()
            .expect("connection refused");

        assert!(matches!(error, RouterError::Transport { .. }), "{error:?}");
    }

    #[tokio::test]
    async fn api_key_is_sent_as_a_bearer_token() {
        let server = MockServer::start().await;
        let config = ProviderConfig {
            kind: ProviderKind::Openai,
            api_key: Some("sk-secret".to_string()),
            base_url: Some(server.base_url()),
            timeout_ms: 5_000,
            ttft_timeout_ms: 1_000,
        };
        let provider = OpenAiProvider::new("openai_us", &config).expect("provider builds");
        let request = proxy_request("gpt-4o-fast", true);

        let (_first, _stream) = provider
            .execute_stream("gpt-4o-fast", &request)
            .await
            .expect("stream starts");

        let received = server.requests();
        let request_headers = &received.first().expect("one request").headers;

        assert!(
            request_headers
                .iter()
                .any(|(name, value)| name.eq_ignore_ascii_case("authorization")
                    && value == "Bearer sk-secret"),
            "authorization header missing: {request_headers:?}"
        );
    }

    // ---------------------------------------------------------------- pass-through

    #[tokio::test]
    async fn passes_the_body_through_untouched_when_the_model_matches() {
        let server = MockServer::start().await;
        let provider = provider_for(&server, 5_000);

        // Deliberately unusual formatting and an unknown vendor field.
        let original = r#"{ "model" : "gpt-4o-fast",
            "messages":[{"role":"user","content":"hi"}],
            "vendor_extension": {"trace": true},
            "stream": true }"#;
        let request =
            inspect_request(HeaderMap::new(), Bytes::from(original.to_string())).expect("inspects");

        let (_first, _stream) = provider
            .execute_stream("gpt-4o-fast", &request)
            .await
            .expect("stream starts");

        let received = server.requests();
        let forwarded = &received.first().expect("one request").body;

        assert_eq!(
            forwarded.as_ref(),
            original.as_bytes(),
            "pass-through must be byte-identical"
        );
    }

    #[tokio::test]
    async fn rewrites_only_the_model_field_when_routing_changed_it() {
        let server = MockServer::start().await;
        let provider = provider_for(&server, 5_000);

        let original = r#"{"model":"auto","messages":[{"role":"user","content":"hi"}],"vendor_extension":{"keep":true},"stream":true}"#;
        let request =
            inspect_request(HeaderMap::new(), Bytes::from(original.to_string())).expect("inspects");

        let (_first, _stream) = provider
            .execute_stream("gpt-4o", &request)
            .await
            .expect("stream starts");

        let received = server.requests();
        let forwarded = &received.first().expect("one request").body;
        let expected = original.replace("\"auto\"", "\"gpt-4o\"");

        assert_eq!(
            forwarded.as_ref(),
            expected.as_bytes(),
            "only the model literal may change"
        );
    }

    // ---------------------------------------------------------------- non-streaming

    #[tokio::test]
    async fn execute_json_reports_usage_tokens() {
        let server = MockServer::start().await;
        let provider = provider_for(&server, 5_000);
        let request = proxy_request("gpt-4o-json", false);

        let response = provider
            .execute_json("gpt-4o-json", &request)
            .await
            .expect("completion returns");

        assert_eq!(response.status, 200);
        assert_eq!(response.prompt_tokens, 11);
        assert_eq!(response.completion_tokens, 7);
        assert!(!response.body.is_empty());
    }

    #[tokio::test]
    async fn execute_json_maps_error_statuses() {
        let server = MockServer::start().await;
        let provider = provider_for(&server, 5_000);
        let request = proxy_request("gpt-4o-ratelimited", false);

        let error = provider
            .execute_json("gpt-4o-ratelimited", &request)
            .await
            .expect_err("429 surfaces");

        assert!(matches!(
            error,
            RouterError::UpstreamHttp { status: 429, .. }
        ));
    }

    // ---------------------------------------------------------------- construction

    #[test]
    fn rejects_non_openai_provider_kinds() {
        let config = ProviderConfig {
            kind: ProviderKind::Anthropic,
            api_key: Some("sk".to_string()),
            base_url: None,
            timeout_ms: 1_000,
            ttft_timeout_ms: 500,
        };

        assert!(OpenAiProvider::new("anthropic", &config).is_err());
    }

    #[test]
    fn defaults_to_the_public_openai_endpoint() {
        let config = ProviderConfig {
            kind: ProviderKind::Openai,
            api_key: Some("sk".to_string()),
            base_url: None,
            timeout_ms: 1_000,
            ttft_timeout_ms: 500,
        };
        let provider = OpenAiProvider::new("openai", &config).expect("provider builds");

        assert_eq!(
            provider.endpoint(),
            "https://api.openai.com/v1/chat/completions"
        );
        assert_eq!(provider.provider_id(), "openai");
    }

    #[test]
    fn trims_trailing_slashes_from_the_base_url() {
        let config = ProviderConfig {
            kind: ProviderKind::OpenaiCompatible,
            api_key: None,
            base_url: Some("http://127.0.0.1:11434/v1/".to_string()),
            timeout_ms: 1_000,
            ttft_timeout_ms: 500,
        };
        let provider = OpenAiProvider::new("ollama", &config).expect("provider builds");

        assert_eq!(
            provider.endpoint(),
            "http://127.0.0.1:11434/v1/chat/completions"
        );
    }

    // ---------------------------------------------------------------- first_data_offset

    #[test]
    fn first_data_offset_skips_comments_and_blank_lines() {
        let buffer = b": ping\n\n: keep-alive\n\ndata: {\"a\":1}\n\n";

        let offset = first_data_offset(buffer).expect("present");
        assert_eq!(&buffer[offset..], b"data: {\"a\":1}\n\n");
    }

    #[test]
    fn first_data_offset_waits_for_a_terminated_line() {
        assert_eq!(first_data_offset(b"data: {\"partial\""), None);
        assert_eq!(first_data_offset(b""), None);
        assert_eq!(first_data_offset(b"\n\n\n"), None);
    }

    #[test]
    fn first_data_offset_handles_crlf() {
        let buffer = b": ping\r\ndata: {}\r\n";
        let offset = first_data_offset(buffer).expect("present");
        assert!(buffer[offset..].starts_with(b"data:"));
    }

    // ---------------------------------------------------------------- rewrite_model

    #[test]
    fn rewrite_preserves_surrounding_bytes() {
        let raw = Bytes::from(r#"{ "model" : "auto" , "x" : 1 }"#);
        let rewritten = rewrite_model(&raw, "gpt-4o").expect("rewrites");

        assert_eq!(rewritten.as_ref(), br#"{ "model" : "gpt-4o" , "x" : 1 }"#);
    }

    #[test]
    fn rewrite_ignores_nested_model_keys() {
        let raw = Bytes::from(r#"{"vendor":{"model":"nested"},"model":"auto"}"#);
        let rewritten = rewrite_model(&raw, "gpt-4o").expect("rewrites");

        assert_eq!(
            rewritten.as_ref(),
            br#"{"vendor":{"model":"nested"},"model":"gpt-4o"}"#
        );
    }

    #[test]
    fn rewrite_does_not_confuse_suffixed_keys() {
        let raw = Bytes::from(r#"{"model_extra":"keep","model":"auto"}"#);
        let rewritten = rewrite_model(&raw, "x").expect("rewrites");

        assert_eq!(rewritten.as_ref(), br#"{"model_extra":"keep","model":"x"}"#);
    }

    #[test]
    fn rewrite_escapes_the_new_model_name() {
        let raw = Bytes::from(r#"{"model":"auto"}"#);
        let rewritten = rewrite_model(&raw, "weird\"name").expect("rewrites");

        assert_eq!(rewritten.as_ref(), br#"{"model":"weird\"name"}"#);
    }

    #[test]
    fn rewrite_handles_escaped_quotes_inside_earlier_strings() {
        let raw = Bytes::from(r#"{"prompt":"a \"model\": b","model":"auto"}"#);
        let rewritten = rewrite_model(&raw, "gpt-4o").expect("rewrites");

        assert_eq!(
            rewritten.as_ref(),
            br#"{"prompt":"a \"model\": b","model":"gpt-4o"}"#
        );
    }

    #[test]
    fn rewrite_without_a_model_key_is_rejected() {
        let raw = Bytes::from(r#"{"prompt":"hi"}"#);

        assert!(matches!(
            rewrite_model(&raw, "gpt-4o"),
            Err(RouterError::InvalidRequest { .. })
        ));
    }

    #[test]
    fn rewrite_with_non_string_model_is_rejected() {
        let raw = Bytes::from(r#"{"model":42}"#);

        assert!(rewrite_model(&raw, "gpt-4o").is_err());
    }

    #[test]
    fn escape_json_string_covers_control_characters() {
        assert_eq!(escape_json_string("a\"b\\c\nd\te"), "a\\\"b\\\\c\\nd\\te");
        assert_eq!(escape_json_string("\u{1}"), "\\u0001");
    }

    // ---------------------------------------------------------------- usage_from

    #[test]
    fn usage_is_zero_when_absent_or_malformed() {
        assert_eq!(usage_from(b"not json"), (0, 0));
        assert_eq!(usage_from(b"{}"), (0, 0));
        assert_eq!(usage_from(br#"{"usage":{"prompt_tokens":3}}"#), (3, 0));
    }
}

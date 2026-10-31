//! AWS Bedrock `Converse` / `ConverseStream` adapter.
//!
//! Bedrock differs from the other upstreams in three ways, each handled here:
//!
//! 1. **Authentication is SigV4**, not a bearer token. Credentials come from the
//!    standard `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` (plus optional
//!    `AWS_SESSION_TOKEN`) environment variables, matching AWS convention; the
//!    region comes from `AWS_REGION` / `AWS_DEFAULT_REGION`, or from an explicit
//!    `base_url` override.
//! 2. **The model id is in the URL path**, not the body.
//! 3. **Streaming responses are `application/vnd.amazon.eventstream`**, a binary
//!    length-prefixed framing — not SSE. [`EventStreamDecoder`] reassembles
//!    frames across arbitrary TCP chunk boundaries before they are translated
//!    into OpenAI `chat.completion.chunk` frames.
//!
//! # Scope
//!
//! Credentials are environment-only (no profile, IMDS, or SSO resolution). The
//! decoder does not verify the per-message CRC32 trailers: TLS and framing
//! lengths already detect corruption, and a mismatch would surface as a JSON
//! parse error. Both are documented gaps rather than silent behaviour.

use std::pin::Pin;
use std::task::{Context, Poll};

use async_trait::async_trait;
use bytes::Bytes;
use futures_core::Stream;
use futures_util::StreamExt;
use hmac::{Hmac, Mac};
use http::{HeaderMap, HeaderValue};
use reqwest::header::CONTENT_TYPE;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};

use crate::config::{ProviderConfig, ProviderKind};
use crate::error::{ConfigError, RouterError};
use crate::providers::sse::{unix_now, ChunkBuilder, DONE_FRAME};
use crate::providers::{
    await_first_frame, build_upstream_http_client, openai_content_text, LlmProvider,
    NonStreamResponse, SseByteStream,
};
use crate::proxy::inspector::ProxyRequest;

/// Default region when neither config nor environment supplies one.
pub const DEFAULT_REGION: &str = "us-east-1";

/// SigV4 service name for the runtime API.
const SIGNING_SERVICE: &str = "bedrock";

/// `max_tokens` is optional for OpenAI but required by Converse.
pub const DEFAULT_MAX_TOKENS: u64 = 4096;

/// Largest event-stream frame we will buffer, as a sanity bound.
const MAX_FRAME_BYTES: usize = 16 * 1024 * 1024;

type HmacSha256 = Hmac<Sha256>;

/// Adapter for AWS Bedrock's Converse APIs.
pub struct BedrockProvider {
    id: String,
    region: String,
    endpoint: String,
    access_key_id: String,
    secret_access_key: String,
    session_token: Option<String>,
    client: reqwest::Client,
}

impl BedrockProvider {
    /// Builds an adapter, reading AWS credentials from the environment.
    ///
    /// # Errors
    ///
    /// Fails when the provider kind is wrong or credentials are absent.
    pub fn new(id: impl Into<String>, config: &ProviderConfig) -> Result<Self, RouterError> {
        let id = id.into();

        if config.kind != ProviderKind::Bedrock {
            return Err(RouterError::Config(ConfigError::InvalidValue {
                field: format!("providers.{id}.type"),
                detail: "the Bedrock adapter serves only `bedrock`".to_string(),
            }));
        }

        // An explicit base_url wins and also supplies the region.
        let (endpoint, region) = match config.base_url.as_deref() {
            Some(base) => {
                let base = base.trim_end_matches('/').to_string();
                let region = region_from_host(&base).unwrap_or_else(env_region);
                (base, region)
            }
            None => {
                let region = env_region();
                (
                    format!("https://bedrock-runtime.{region}.amazonaws.com"),
                    region,
                )
            }
        };

        let access_key_id = env_var("AWS_ACCESS_KEY_ID").ok_or_else(|| missing_credentials(&id))?;
        let secret_access_key =
            env_var("AWS_SECRET_ACCESS_KEY").ok_or_else(|| missing_credentials(&id))?;

        Ok(Self {
            id,
            region,
            endpoint,
            access_key_id,
            secret_access_key,
            session_token: env_var("AWS_SESSION_TOKEN"),
            client: build_upstream_http_client(config.timeout_ms)?,
        })
    }

    /// Region this adapter signs for.
    pub fn region(&self) -> &str {
        &self.region
    }

    fn label(&self, upstream_model: &str) -> String {
        format!("{}/{}", self.id, upstream_model)
    }

    fn streaming_url(&self, upstream_model: &str) -> String {
        format!(
            "{}/model/{}/converse-stream",
            self.endpoint,
            percent_encode(upstream_model)
        )
    }

    fn url(&self, upstream_model: &str) -> String {
        format!(
            "{}/model/{}/converse",
            self.endpoint,
            percent_encode(upstream_model)
        )
    }

    /// Signs and sends a Converse request.
    async fn send(
        &self,
        url: &str,
        body: Bytes,
        upstream_model: &str,
    ) -> Result<reqwest::Response, RouterError> {
        let amz_date = format_amz_date(unix_now());
        let mut headers = HeaderMap::new();

        self.sign_request("POST", url, &body, &amz_date, &mut headers)?;

        self.client
            .post(url)
            .headers(headers)
            .body(body)
            .send()
            .await
            .map_err(|error| RouterError::Transport {
                provider: self.label(upstream_model),
                message: error.to_string(),
            })
    }

    /// Adds the SigV4 `Authorization`/`x-amz-date`/host headers for `url`.
    fn sign_request(
        &self,
        method: &str,
        url: &str,
        payload: &[u8],
        amz_date: &str,
        headers: &mut HeaderMap,
    ) -> Result<(), RouterError> {
        let parsed = reqwest::Url::parse(url).map_err(|error| RouterError::Internal {
            message: format!("invalid Bedrock URL `{url}`: {error}"),
        })?;

        let host = match parsed.port() {
            Some(port) => format!("{}:{port}", parsed.host_str().unwrap_or_default()),
            None => parsed.host_str().unwrap_or_default().to_string(),
        };
        let path = if parsed.path().is_empty() {
            "/".to_string()
        } else {
            parsed.path().to_string()
        };

        let mut canonical = vec![
            ("content-type".to_string(), "application/json".to_string()),
            ("host".to_string(), host.clone()),
            ("x-amz-date".to_string(), amz_date.to_string()),
        ];
        if let Some(token) = &self.session_token {
            canonical.push(("x-amz-security-token".to_string(), token.clone()));
        }
        canonical.sort_by(|a, b| a.0.cmp(&b.0));

        let signed_headers: Vec<&str> = canonical.iter().map(|(name, _)| name.as_str()).collect();
        let signed_headers = signed_headers.join(";");

        let canonical_headers: String = canonical
            .iter()
            .map(|(name, value)| format!("{name}:{value}\n"))
            .collect();

        let payload_hash = hex(&Sha256::digest(payload));
        let canonical_request =
            format!("{method}\n{path}\n\n{canonical_headers}\n{signed_headers}\n{payload_hash}");

        let date = &amz_date[..8.min(amz_date.len())];
        let scope = format!("{date}/{}/{SIGNING_SERVICE}/aws4_request", self.region);
        let string_to_sign = format!(
            "AWS4-HMAC-SHA256\n{amz_date}\n{scope}\n{}",
            hex(&Sha256::digest(canonical_request.as_bytes()))
        );

        let signing_key =
            signing_key(&self.secret_access_key, date, &self.region, SIGNING_SERVICE)?;
        let signature = hex(&hmac(&signing_key, string_to_sign.as_bytes()));

        let authorization = format!(
            "AWS4-HMAC-SHA256 Credential={}/{scope}, SignedHeaders={signed_headers}, Signature={signature}",
            self.access_key_id
        );

        headers.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
        headers.insert("host", header_value(&host)?);
        headers.insert("x-amz-date", header_value(amz_date)?);
        headers.insert("authorization", header_value(&authorization)?);
        if let Some(token) = &self.session_token {
            headers.insert("x-amz-security-token", header_value(token)?);
        }

        Ok(())
    }

    /// Converts an OpenAI chat request into a Converse request body.
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

        let mut system_parts: Vec<Value> = Vec::new();
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
                        system_parts.push(json!({ "text": text }));
                    }
                }
                "assistant" => converted.push(json!({
                    "role": "assistant",
                    "content": [{ "text": text }],
                })),
                _ => converted.push(json!({
                    "role": "user",
                    "content": [{ "text": text }],
                })),
            }
        }

        if converted.is_empty() {
            return Err(RouterError::InvalidRequest {
                message: "Bedrock requests require at least one non-system message".to_string(),
            });
        }

        let mut body = serde_json::Map::new();
        body.insert("messages".to_string(), Value::Array(converted));

        if !system_parts.is_empty() {
            body.insert("system".to_string(), Value::Array(system_parts));
        }

        let mut inference = serde_json::Map::new();
        inference.insert(
            "maxTokens".to_string(),
            parsed
                .get("max_tokens")
                .cloned()
                .unwrap_or_else(|| json!(DEFAULT_MAX_TOKENS)),
        );
        for (source, target) in [("temperature", "temperature"), ("top_p", "topP")] {
            if let Some(value) = parsed.get(source) {
                inference.insert(target.to_string(), value.clone());
            }
        }
        if let Some(stop) = parsed.get("stop") {
            let sequences: Vec<Value> = match stop {
                Value::Array(items) => items.clone(),
                Value::String(single) => vec![json!(single)],
                _ => Vec::new(),
            };
            if !sequences.is_empty() {
                inference.insert("stopSequences".to_string(), Value::Array(sequences));
            }
        }
        body.insert("inferenceConfig".to_string(), Value::Object(inference));

        // `stream` is implied by the endpoint path.
        let _ = stream;

        serde_json::to_vec(&Value::Object(body))
            .map(Bytes::from)
            .map_err(|error| RouterError::Internal {
                message: format!("failed to encode Bedrock request: {error}"),
            })
    }
}

#[async_trait]
impl LlmProvider for BedrockProvider {
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
            .send(&self.streaming_url(upstream_model), body, upstream_model)
            .await?;

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

        let normalizer: SseByteStream = Box::pin(BedrockStreamNormalizer::new(
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
        let response = self.url(upstream_model);
        let response = self.send(&response, body, upstream_model).await?;

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
                message: format!("malformed Bedrock completion: {error}"),
            })?;

        let text = completion_text(&parsed);
        let finish_reason = parsed
            .get("stopReason")
            .and_then(Value::as_str)
            .unwrap_or("end_turn");
        let usage = parsed.get("usage");
        let prompt_tokens = usage
            .and_then(|usage| usage.get("inputTokens"))
            .and_then(Value::as_u64)
            .unwrap_or(0) as usize;
        let completion_tokens = usage
            .and_then(|usage| usage.get("outputTokens"))
            .and_then(Value::as_u64)
            .unwrap_or(0) as usize;

        let builder = ChunkBuilder::new(
            format!("chatcmpl-{}", uuid::Uuid::new_v4().simple()),
            upstream_model,
            unix_now(),
        );
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

fn completion_text(value: &Value) -> String {
    value
        .get("output")
        .and_then(|output| output.get("message"))
        .and_then(|message| message.get("content"))
        .and_then(Value::as_array)
        .map(|blocks| {
            blocks
                .iter()
                .filter_map(|block| block.get("text").and_then(Value::as_str))
                .collect::<Vec<_>>()
                .join("")
        })
        .unwrap_or_default()
}

fn map_stop_reason(reason: &str) -> &'static str {
    match reason {
        "max_tokens" => "length",
        "tool_use" => "tool_calls",
        "content_filtered" | "guardrail_intervened" => "content_filter",
        _ => "stop",
    }
}

// --------------------------------------------------------------------------- event stream

/// One decoded `vnd.amazon.eventstream` message.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EventStreamMessage {
    /// Header name/value pairs. Only string-typed headers are retained.
    pub headers: Vec<(String, String)>,
    /// Raw JSON payload.
    pub payload: Vec<u8>,
}

impl EventStreamMessage {
    /// Value of a header, if present.
    pub fn header(&self, name: &str) -> Option<&str> {
        self.headers
            .iter()
            .find(|(key, _)| key == name)
            .map(|(_, value)| value.as_str())
    }

    /// The `:event-type` header (message kind such as `contentBlockDelta`).
    pub fn event_type(&self) -> Option<&str> {
        self.header(":event-type")
    }

    /// The `:message-type` header (`event` or `exception`).
    pub fn message_type(&self) -> Option<&str> {
        self.header(":message-type")
    }
}

/// Reassembles length-prefixed event-stream frames from a byte stream.
#[derive(Debug, Default)]
pub struct EventStreamDecoder {
    buffer: Vec<u8>,
}

impl EventStreamDecoder {
    /// Creates an empty decoder.
    pub fn new() -> Self {
        Self::default()
    }

    /// Appends bytes and returns every frame that is now complete.
    ///
    /// # Errors
    ///
    /// Returns an error for a frame whose declared length is impossible, which
    /// indicates a protocol violation rather than a partial read.
    pub fn push(&mut self, bytes: &[u8]) -> Result<Vec<EventStreamMessage>, RouterError> {
        self.buffer.extend_from_slice(bytes);

        let mut messages = Vec::new();
        loop {
            if self.buffer.len() < 4 {
                break;
            }

            let total_len = u32::from_be_bytes([
                self.buffer[0],
                self.buffer[1],
                self.buffer[2],
                self.buffer[3],
            ]) as usize;

            // prelude (12) + message CRC (4) is the minimum possible frame.
            if !(16..=MAX_FRAME_BYTES).contains(&total_len) {
                return Err(RouterError::Transport {
                    provider: "bedrock".to_string(),
                    message: format!("invalid event-stream frame length {total_len}"),
                });
            }

            if self.buffer.len() < total_len {
                break;
            }

            let frame: Vec<u8> = self.buffer.drain(..total_len).collect();
            messages.push(parse_frame(&frame)?);
        }

        Ok(messages)
    }
}

fn parse_frame(frame: &[u8]) -> Result<EventStreamMessage, RouterError> {
    let malformed = || RouterError::Transport {
        provider: "bedrock".to_string(),
        message: "malformed event-stream frame".to_string(),
    };

    let headers_len = u32::from_be_bytes(
        frame
            .get(4..8)
            .ok_or_else(malformed)?
            .try_into()
            .map_err(|_| malformed())?,
    ) as usize;

    let headers_start: usize = 12;
    let headers_end = headers_start
        .checked_add(headers_len)
        .ok_or_else(malformed)?;
    if headers_end + 4 > frame.len() {
        return Err(malformed());
    }

    let headers = parse_headers(&frame[headers_start..headers_end])?;
    // The trailing 4 bytes are the message CRC, deliberately not validated.
    let payload = frame[headers_end..frame.len() - 4].to_vec();

    Ok(EventStreamMessage { headers, payload })
}

fn parse_headers(bytes: &[u8]) -> Result<Vec<(String, String)>, RouterError> {
    let malformed = || RouterError::Transport {
        provider: "bedrock".to_string(),
        message: "malformed event-stream headers".to_string(),
    };

    let mut headers = Vec::new();
    let mut index = 0usize;

    while index < bytes.len() {
        let name_len = *bytes.get(index).ok_or_else(malformed)? as usize;
        index += 1;

        let name_bytes = bytes.get(index..index + name_len).ok_or_else(malformed)?;
        let name = String::from_utf8_lossy(name_bytes).into_owned();
        index += name_len;

        let value_type = *bytes.get(index).ok_or_else(malformed)?;
        index += 1;

        let value_len = u16::from_be_bytes(
            bytes
                .get(index..index + 2)
                .ok_or_else(malformed)?
                .try_into()
                .map_err(|_| malformed())?,
        ) as usize;
        index += 2;

        let value_bytes = bytes.get(index..index + value_len).ok_or_else(malformed)?;
        index += value_len;

        // Type 7 is a string; every header we care about is a string.
        if value_type == 7 {
            headers.push((name, String::from_utf8_lossy(value_bytes).into_owned()));
        }
    }

    Ok(headers)
}

// --------------------------------------------------------------------------- normaliser

/// Translates Bedrock event-stream messages into OpenAI chunks.
///
/// Bedrock reports `stopReason` in `messageStop` but token usage in a trailing
/// `metadata` event, so the finish frame is deferred until `metadata` arrives
/// (or the stream ends) to keep the usage numbers accurate.
pub struct BedrockStreamNormalizer {
    inner: SseByteStream,
    decoder: EventStreamDecoder,
    builder: ChunkBuilder,
    provider: String,
    prompt_tokens: usize,
    completion_tokens: usize,
    pending_stop: Option<String>,
    emitted: bool,
    done_sent: bool,
}

impl BedrockStreamNormalizer {
    /// Wraps a raw byte stream, stamping frames with the current time.
    pub fn new(
        inner: SseByteStream,
        provider: impl Into<String>,
        model: impl Into<String>,
    ) -> Self {
        Self::new_at(inner, provider, model, unix_now())
    }

    /// Wraps a raw byte stream with a fixed `created` timestamp.
    pub fn new_at(
        inner: SseByteStream,
        provider: impl Into<String>,
        model: impl Into<String>,
        created: u64,
    ) -> Self {
        Self {
            inner,
            decoder: EventStreamDecoder::new(),
            builder: ChunkBuilder::new("chatcmpl-bedrock", model, created),
            provider: provider.into(),
            prompt_tokens: 0,
            completion_tokens: 0,
            pending_stop: None,
            emitted: false,
            done_sent: false,
        }
    }

    fn translate(&mut self, message: &EventStreamMessage) -> Result<Bytes, RouterError> {
        if message.message_type() == Some("exception") {
            let detail = message
                .header(":exception-type")
                .unwrap_or("unknown Bedrock exception");
            return Err(RouterError::Transport {
                provider: self.provider.clone(),
                message: format!("upstream stream exception: {detail}"),
            });
        }

        let Ok(payload) = serde_json::from_slice::<Value>(&message.payload) else {
            return Ok(Bytes::new());
        };

        match message.event_type().unwrap_or_default() {
            "messageStart" => {
                self.emitted = true;
                Ok(self.builder.role_chunk())
            }
            "contentBlockDelta" => {
                let text = payload
                    .get("delta")
                    .and_then(|delta| delta.get("text"))
                    .and_then(Value::as_str)
                    .unwrap_or_default();

                if text.is_empty() {
                    return Ok(Bytes::new());
                }

                self.emitted = true;
                Ok(self.builder.content_chunk(text))
            }
            "messageStop" => {
                let reason = payload
                    .get("stopReason")
                    .and_then(Value::as_str)
                    .unwrap_or("end_turn");
                self.pending_stop = Some(map_stop_reason(reason).to_string());
                Ok(Bytes::new())
            }
            "metadata" => {
                if let Some(usage) = payload.get("usage") {
                    if let Some(input) = usage.get("inputTokens").and_then(Value::as_u64) {
                        self.prompt_tokens = input as usize;
                    }
                    if let Some(output) = usage.get("outputTokens").and_then(Value::as_u64) {
                        self.completion_tokens = output as usize;
                    }
                }
                Ok(self.take_finish())
            }
            _ => Ok(Bytes::new()),
        }
    }

    fn take_finish(&mut self) -> Bytes {
        let Some(reason) = self.pending_stop.take() else {
            return Bytes::new();
        };

        self.emitted = true;
        self.builder
            .finish_chunk(&reason, self.prompt_tokens, self.completion_tokens)
    }
}

impl Stream for BedrockStreamNormalizer {
    type Item = Result<Bytes, RouterError>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();

        loop {
            match this.inner.as_mut().poll_next(cx) {
                Poll::Pending => return Poll::Pending,

                Poll::Ready(Some(Err(error))) => return Poll::Ready(Some(Err(error))),

                Poll::Ready(Some(Ok(chunk))) => {
                    let messages = match this.decoder.push(&chunk) {
                        Ok(messages) => messages,
                        Err(error) => return Poll::Ready(Some(Err(error))),
                    };

                    let mut output = Vec::new();
                    for message in messages {
                        match this.translate(&message) {
                            Ok(bytes) => output.extend_from_slice(&bytes),
                            Err(error) => return Poll::Ready(Some(Err(error))),
                        }
                    }

                    if !output.is_empty() {
                        return Poll::Ready(Some(Ok(Bytes::from(output))));
                    }
                }

                Poll::Ready(None) => {
                    // Flush a stop reason that never received metadata.
                    let mut output = this.take_finish().to_vec();

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

// --------------------------------------------------------------------------- signing helpers

/// Derives the SigV4 signing key for a date/region/service triple.
fn signing_key(
    secret: &str,
    date: &str,
    region: &str,
    service: &str,
) -> Result<Vec<u8>, RouterError> {
    let mut key = hmac(format!("AWS4{secret}").as_bytes(), date.as_bytes());
    key = hmac(&key, region.as_bytes());
    key = hmac(&key, service.as_bytes());
    Ok(hmac(&key, b"aws4_request"))
}

/// HMAC-SHA256.
///
/// HMAC accepts keys of any length, so `new_from_slice` cannot fail here; the
/// empty fallback keeps the function panic-free regardless.
fn hmac(key: &[u8], data: &[u8]) -> Vec<u8> {
    match HmacSha256::new_from_slice(key) {
        Ok(mut mac) => {
            mac.update(data);
            mac.finalize().into_bytes().to_vec()
        }
        Err(_) => Vec::new(),
    }
}

fn hex(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        out.push(HEX[(byte >> 4) as usize] as char);
        out.push(HEX[(byte & 0x0f) as usize] as char);
    }
    out
}

/// Percent-encodes everything outside the SigV4 unreserved set.
///
/// Bedrock model ids contain `:` and `.`, which must be escaped in the path.
fn percent_encode(input: &str) -> String {
    const HEX: &[u8; 16] = b"0123456789ABCDEF";
    let mut out = String::with_capacity(input.len());

    for byte in input.bytes() {
        match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(byte as char)
            }
            _ => {
                out.push('%');
                out.push(HEX[(byte >> 4) as usize] as char);
                out.push(HEX[(byte & 0x0f) as usize] as char);
            }
        }
    }

    out
}

/// Formats epoch seconds as SigV4's `YYYYMMDDTHHMMSSZ`.
fn format_amz_date(epoch_secs: u64) -> String {
    let (year, month, day, hour, minute, second) = civil_from_unix(epoch_secs);
    format!("{year:04}{month:02}{day:02}T{hour:02}{minute:02}{second:02}Z")
}

/// Converts Unix seconds to a UTC civil timestamp (Howard Hinnant's algorithm).
fn civil_from_unix(epoch_secs: u64) -> (i64, u32, u32, u32, u32, u32) {
    let days = (epoch_secs / 86_400) as i64;
    let seconds_of_day = (epoch_secs % 86_400) as u32;

    let z = days + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1_460 + doe / 36_524 - doe / 146_096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let day = (doy - (153 * mp + 2) / 5 + 1) as u32;
    let month = if mp < 10 { mp + 3 } else { mp - 9 } as u32;
    let year = if month <= 2 { y + 1 } else { y };

    (
        year,
        month,
        day,
        seconds_of_day / 3_600,
        (seconds_of_day % 3_600) / 60,
        seconds_of_day % 60,
    )
}

fn env_var(name: &str) -> Option<String> {
    std::env::var(name)
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
}

fn env_region() -> String {
    env_var("AWS_REGION")
        .or_else(|| env_var("AWS_DEFAULT_REGION"))
        .unwrap_or_else(|| DEFAULT_REGION.to_string())
}

fn region_from_host(base_url: &str) -> Option<String> {
    let host = reqwest::Url::parse(base_url).ok()?.host_str()?.to_string();
    // bedrock-runtime.<region>.amazonaws.com
    let rest = host.strip_prefix("bedrock-runtime.")?;
    let region = rest.strip_suffix(".amazonaws.com")?;
    if region.is_empty() {
        None
    } else {
        Some(region.to_string())
    }
}

fn missing_credentials(id: &str) -> RouterError {
    RouterError::Config(ConfigError::InvalidValue {
        field: format!("providers.{id}"),
        detail: "the Bedrock adapter requires AWS_ACCESS_KEY_ID and \
                 AWS_SECRET_ACCESS_KEY in the environment"
            .to_string(),
    })
}

fn header_value(value: &str) -> Result<HeaderValue, RouterError> {
    HeaderValue::from_str(value).map_err(|error| RouterError::Internal {
        message: format!("invalid Bedrock header value: {error}"),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::proxy::inspector::inspect_request;

    // ------------------------------------------------------------------ time

    #[test]
    fn formats_the_sigv4_timestamp() {
        // 1440938160 = 2015-08-30T12:36:00Z, the AWS documentation example.
        assert_eq!(format_amz_date(1_440_938_160), "20150830T123600Z");
    }

    #[test]
    fn formats_epoch_zero() {
        assert_eq!(format_amz_date(0), "19700101T000000Z");
    }

    #[test]
    fn handles_a_leap_day() {
        // 2024-02-29T00:00:00Z
        assert_eq!(format_amz_date(1_709_164_800), "20240229T000000Z");
    }

    // ------------------------------------------------------------------ encoding

    #[test]
    fn percent_encodes_model_ids() {
        assert_eq!(
            percent_encode("anthropic.claude-3-sonnet-v1:0"),
            "anthropic.claude-3-sonnet-v1%3A0"
        );
        assert_eq!(percent_encode("plain-model_1.0"), "plain-model_1.0");
    }

    #[test]
    fn hex_encodes_lowercase() {
        assert_eq!(hex(&[0x00, 0x0f, 0xa5, 0xff]), "000fa5ff");
    }

    // ------------------------------------------------------------------ sigv4

    #[test]
    fn matches_the_documented_sigv4_test_vector() {
        // AWS "get-vanilla" from the SigV4 test suite.
        let canonical_request = "GET\n/\n\nhost:example.amazonaws.com\nx-amz-date:20150830T123600Z\n\nhost;x-amz-date\ne3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";

        assert_eq!(
            hex(&Sha256::digest(canonical_request.as_bytes())),
            "bb579772317eb040ac9ed261061d46c1f17a8133879d6129b6e1c25292927e63"
        );

        let scope = "20150830/us-east-1/service/aws4_request";
        let string_to_sign = format!(
            "AWS4-HMAC-SHA256\n20150830T123600Z\n{scope}\n{}",
            hex(&Sha256::digest(canonical_request.as_bytes()))
        );

        let key = signing_key(
            "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY",
            "20150830",
            "us-east-1",
            "service",
        )
        .expect("key derives");

        assert_eq!(
            hex(&hmac(&key, string_to_sign.as_bytes())),
            "5fa00fa31553b73ebf1942676e86291e8372ff2a2260956d9b8aae1d763fbf31"
        );
    }

    #[test]
    fn sha256_of_empty_payload_is_the_well_known_digest() {
        assert_eq!(
            hex(&Sha256::digest(b"")),
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
        );
    }

    #[test]
    fn region_can_be_derived_from_the_endpoint() {
        assert_eq!(
            region_from_host("https://bedrock-runtime.eu-west-1.amazonaws.com"),
            Some("eu-west-1".to_string())
        );
        assert_eq!(region_from_host("http://127.0.0.1:8080"), None);
    }

    // ------------------------------------------------------------------ event stream

    /// Builds an event-stream frame; CRC trailers are zero because the decoder
    /// does not validate them.
    fn encode_message(headers: &[(&str, &str)], payload: &[u8]) -> Vec<u8> {
        let mut encoded_headers = Vec::new();
        for (name, value) in headers {
            encoded_headers.push(name.len() as u8);
            encoded_headers.extend_from_slice(name.as_bytes());
            encoded_headers.push(7); // string
            encoded_headers.extend_from_slice(&(value.len() as u16).to_be_bytes());
            encoded_headers.extend_from_slice(value.as_bytes());
        }

        let total_len = 12 + encoded_headers.len() + payload.len() + 4;
        let mut frame = Vec::with_capacity(total_len);
        frame.extend_from_slice(&(total_len as u32).to_be_bytes());
        frame.extend_from_slice(&(encoded_headers.len() as u32).to_be_bytes());
        frame.extend_from_slice(&0u32.to_be_bytes()); // prelude CRC
        frame.extend_from_slice(&encoded_headers);
        frame.extend_from_slice(payload);
        frame.extend_from_slice(&0u32.to_be_bytes()); // message CRC
        frame
    }

    fn stream_from_chunks(chunks: Vec<Vec<u8>>) -> SseByteStream {
        Box::pin(futures_util::stream::iter(
            chunks
                .into_iter()
                .map(|chunk| Ok::<Bytes, RouterError>(Bytes::from(chunk))),
        ))
    }

    fn normalizer_from(frames: Vec<Vec<u8>>) -> BedrockStreamNormalizer {
        BedrockStreamNormalizer::new_at(
            stream_from_chunks(frames),
            "bedrock/aws",
            "anthropic.claude-3-sonnet-v1:0",
            0,
        )
    }

    async fn collect(mut stream: BedrockStreamNormalizer) -> Result<String, RouterError> {
        let mut out = Vec::new();
        while let Some(chunk) = stream.next().await {
            out.extend_from_slice(&chunk?);
        }
        Ok(String::from_utf8_lossy(&out).into_owned())
    }

    fn happy_frames() -> Vec<Vec<u8>> {
        vec![
            encode_message(
                &[(":event-type", "messageStart")],
                br#"{"role":"assistant"}"#,
            ),
            encode_message(
                &[(":event-type", "contentBlockDelta")],
                br#"{"contentBlockIndex":0,"delta":{"text":"Hello"}}"#,
            ),
            encode_message(
                &[(":event-type", "contentBlockDelta")],
                br#"{"contentBlockIndex":0,"delta":{"text":" from Bedrock"}}"#,
            ),
            encode_message(
                &[(":event-type", "messageStop")],
                br#"{"stopReason":"end_turn"}"#,
            ),
            encode_message(
                &[(":event-type", "metadata")],
                br#"{"usage":{"inputTokens":11,"outputTokens":7},"metrics":{"latencyMs":42}}"#,
            ),
        ]
    }

    #[test]
    fn decodes_a_complete_frame() {
        let mut decoder = EventStreamDecoder::new();
        let frames = decoder
            .push(&encode_message(&[(":event-type", "messageStart")], b"{}"))
            .expect("decodes");

        assert_eq!(frames.len(), 1);
        assert_eq!(frames[0].event_type(), Some("messageStart"));
        assert_eq!(frames[0].payload, b"{}");
    }

    #[test]
    fn decoder_buffers_partial_frames() {
        let frame = encode_message(&[(":event-type", "messageStart")], b"{}");
        let mut decoder = EventStreamDecoder::new();

        assert!(decoder.push(&frame[..5]).expect("partial").is_empty());
        assert!(decoder
            .push(&frame[5..frame.len() - 2])
            .expect("partial")
            .is_empty());

        let frames = decoder.push(&frame[frame.len() - 2..]).expect("completes");
        assert_eq!(frames.len(), 1);
    }

    #[test]
    fn decoder_handles_several_frames_in_one_push() {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&encode_message(&[(":event-type", "a")], b"1"));
        bytes.extend_from_slice(&encode_message(&[(":event-type", "b")], b"2"));

        let frames = EventStreamDecoder::new().push(&bytes).expect("decodes");
        assert_eq!(frames.len(), 2);
        assert_eq!(frames[0].event_type(), Some("a"));
        assert_eq!(frames[1].event_type(), Some("b"));
    }

    #[test]
    fn decoder_rejects_impossible_lengths() {
        let mut decoder = EventStreamDecoder::new();
        let mut bytes = 8u32.to_be_bytes().to_vec();
        bytes.extend_from_slice(&[0, 0, 0, 0]);

        assert!(decoder.push(&bytes).is_err());
    }

    #[tokio::test]
    async fn translates_the_whole_stream() {
        let output = collect(normalizer_from(happy_frames()))
            .await
            .expect("translates");

        assert!(output.contains("\"role\":\"assistant\""), "{output}");
        assert!(output.contains("\"content\":\"Hello\""), "{output}");
        assert!(output.contains("\"content\":\" from Bedrock\""), "{output}");
        assert!(output.contains("\"finish_reason\":\"stop\""), "{output}");
        assert!(output.contains("\"prompt_tokens\":11"), "{output}");
        assert!(output.contains("\"completion_tokens\":7"), "{output}");
        assert!(output.ends_with("data: [DONE]\n\n"), "{output}");
    }

    #[tokio::test]
    async fn survives_frames_split_across_chunks() {
        let whole = collect(normalizer_from(happy_frames()))
            .await
            .expect("single chunk");

        // Re-split every frame into 3-byte fragments.
        let mut fragments = Vec::new();
        for frame in happy_frames() {
            for piece in frame.chunks(3) {
                fragments.push(piece.to_vec());
            }
        }

        let split = collect(normalizer_from(fragments)).await.expect("split");
        assert_eq!(whole, split);
    }

    #[tokio::test]
    async fn surfaces_exception_events() {
        let frames = vec![encode_message(
            &[
                (":message-type", "exception"),
                (":exception-type", "throttlingException"),
            ],
            b"{}",
        )];

        let error = collect(normalizer_from(frames))
            .await
            .expect_err("exception must surface");
        assert!(error.to_string().contains("throttlingException"), "{error}");
    }

    #[tokio::test]
    async fn flushes_the_finish_frame_when_metadata_is_missing() {
        let frames = vec![
            encode_message(
                &[(":event-type", "messageStart")],
                br#"{"role":"assistant"}"#,
            ),
            encode_message(
                &[(":event-type", "contentBlockDelta")],
                br#"{"delta":{"text":"hi"}}"#,
            ),
            encode_message(
                &[(":event-type", "messageStop")],
                br#"{"stopReason":"max_tokens"}"#,
            ),
        ];

        let output = collect(normalizer_from(frames)).await.expect("translates");

        assert!(output.contains("\"finish_reason\":\"length\""), "{output}");
        assert!(output.ends_with("data: [DONE]\n\n"));
    }

    #[tokio::test]
    async fn empty_stream_produces_no_frames() {
        let output = collect(normalizer_from(Vec::new()))
            .await
            .expect("no frames");
        assert!(output.is_empty(), "unexpected output: {output}");
    }

    // ------------------------------------------------------------------ request translation

    fn provider() -> BedrockProvider {
        BedrockProvider {
            id: "bedrock".to_string(),
            region: "us-east-1".to_string(),
            endpoint: "https://bedrock-runtime.us-east-1.amazonaws.com".to_string(),
            access_key_id: "AKIDEXAMPLE".to_string(),
            secret_access_key: "secret".to_string(),
            session_token: None,
            client: build_upstream_http_client(1_000).expect("client"),
        }
    }

    fn request_for(json: serde_json::Value) -> ProxyRequest {
        inspect_request(HeaderMap::new(), Bytes::from(json.to_string())).expect("inspects")
    }

    #[test]
    fn translates_roles_and_system_prompt() {
        let provider = provider();
        let request = request_for(json!({
            "model": "anthropic.claude-3-sonnet-v1:0",
            "messages": [
                {"role": "system", "content": "Be brief."},
                {"role": "user", "content": "hi"},
                {"role": "assistant", "content": "hello"},
            ],
            "max_tokens": 128,
            "temperature": 0.2,
            "stop": ["END"],
        }));

        let body = provider
            .translate_request(&request, true)
            .expect("translates");
        let parsed: Value = serde_json::from_slice(&body).expect("json");

        assert_eq!(parsed["system"][0]["text"], "Be brief.");
        assert_eq!(parsed["messages"][0]["role"], "user");
        assert_eq!(parsed["messages"][1]["role"], "assistant");
        assert_eq!(parsed["messages"][0]["content"][0]["text"], "hi");
        assert_eq!(parsed["inferenceConfig"]["maxTokens"], 128);
        assert_eq!(parsed["inferenceConfig"]["temperature"], 0.2);
        assert_eq!(parsed["inferenceConfig"]["stopSequences"], json!(["END"]));
    }

    #[test]
    fn max_tokens_defaults_for_bedrock() {
        let provider = provider();
        let request = request_for(json!({
            "model": "m",
            "messages": [{"role": "user", "content": "hi"}],
        }));

        let body = provider
            .translate_request(&request, false)
            .expect("translates");
        let parsed: Value = serde_json::from_slice(&body).expect("json");

        assert_eq!(parsed["inferenceConfig"]["maxTokens"], DEFAULT_MAX_TOKENS);
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
    fn streaming_url_encodes_the_model() {
        let provider = provider();
        assert_eq!(
            provider.streaming_url("anthropic.claude-3-sonnet-v1:0"),
            "https://bedrock-runtime.us-east-1.amazonaws.com/model/\
             anthropic.claude-3-sonnet-v1%3A0/converse-stream"
        );
    }

    #[test]
    fn signs_a_request_with_the_expected_headers() {
        let provider = provider();
        let mut headers = HeaderMap::new();

        provider
            .sign_request(
                "POST",
                "https://bedrock-runtime.us-east-1.amazonaws.com/model/m/converse",
                b"{}",
                "20150830T123600Z",
                &mut headers,
            )
            .expect("signs");

        let authorization = headers
            .get("authorization")
            .and_then(|value| value.to_str().ok())
            .expect("authorization present");

        assert!(authorization.starts_with("AWS4-HMAC-SHA256 Credential=AKIDEXAMPLE/"));
        assert!(authorization.contains("/us-east-1/bedrock/aws4_request"));
        assert!(authorization.contains("SignedHeaders=content-type;host;x-amz-date"));
        assert!(authorization.contains("Signature="));
        assert_eq!(
            headers.get("x-amz-date").and_then(|v| v.to_str().ok()),
            Some("20150830T123600Z")
        );
    }

    #[test]
    fn session_tokens_are_signed_and_sent() {
        let mut provider = provider();
        provider.session_token = Some("token-value".to_string());
        let mut headers = HeaderMap::new();

        provider
            .sign_request(
                "POST",
                "https://bedrock-runtime.us-east-1.amazonaws.com/model/m/converse",
                b"{}",
                "20150830T123600Z",
                &mut headers,
            )
            .expect("signs");

        let authorization = headers
            .get("authorization")
            .and_then(|value| value.to_str().ok())
            .expect("authorization");

        assert!(authorization.contains("x-amz-security-token"));
        assert_eq!(
            headers
                .get("x-amz-security-token")
                .and_then(|v| v.to_str().ok()),
            Some("token-value")
        );
    }

    #[test]
    fn rejects_non_bedrock_provider_kinds() {
        let config = ProviderConfig {
            kind: ProviderKind::Openai,
            api_key: Some("sk".to_string()),
            base_url: None,
            timeout_ms: 1_000,
            ttft_timeout_ms: 500,
        };

        assert!(BedrockProvider::new("openai", &config).is_err());
    }
}

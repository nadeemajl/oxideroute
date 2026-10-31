//! Zero-copy request inspection.
//!
//! The inspector is the first thing a request touches. It must answer four
//! questions cheaply — which model, streaming or not, how big, how complex —
//! without disturbing the payload, because the very same [`Bytes`] buffer is
//! forwarded verbatim to OpenAI-compatible upstreams afterwards.
//!
//! `simd_json` copies the input into its own padded, aligned buffer, so the
//! scratch copy here is only needed to satisfy the `&mut [u8]` signature; the
//! forwarding buffer itself is never modified.

use bytes::Bytes;
use http::HeaderMap;
use simd_json::prelude::*;
use simd_json::BorrowedValue;

use crate::error::RouterError;

/// Prompt bytes are assumed to tokenise at roughly four bytes per token.
///
/// Matches the documented heuristic in `docs/ARCHITECTURE.md`; a real BPE
/// tokeniser is a deliberate later optimisation.
const BYTES_PER_TOKEN: usize = 4;

/// Character count at which prompt length alone maxes out the complexity score.
const LENGTH_SATURATION_CHARS: f32 = 6_000.0;

/// Fenced code blocks at which the code component maxes out.
const CODE_FENCE_SATURATION: f32 = 3.0;

/// Reasoning-keyword hits at which the reasoning component maxes out.
const REASONING_SATURATION: f32 = 3.0;

/// Phrases that correlate with prompts needing a stronger (more expensive) model.
///
/// Matched case-insensitively and only as substrings: this is a routing hint,
/// not a semantic classifier.
const REASONING_KEYWORDS: &[&str] = &[
    "step by step",
    "step-by-step",
    "think step",
    "chain of thought",
    "reason",
    "prove",
    "derive",
    "analyze",
    "compare",
    "evaluate",
    "trade-off",
    "architecture",
];

/// A request that has been parsed and is ready to be routed.
#[derive(Debug, Clone)]
pub struct ProxyRequest {
    /// Correlation id, `req_<32 hex>`, echoed in errors and traces.
    pub request_id: String,
    /// The model the client asked for, before any routing rewrite.
    pub requested_model: String,
    /// Whether the client asked for an SSE stream.
    pub is_stream: bool,
    /// Rough prompt size, used for TPM accounting and routing rules.
    pub estimated_prompt_tokens: usize,
    /// Heuristic difficulty in `0.0..=1.0`, used by `complexity_score_*` rules.
    pub complexity_score: f32,
    /// Number of entries in `messages`; `0` when the payload omits it.
    ///
    /// Additive to the struct sketched in `docs/ARCHITECTURE.md`: the milestone
    /// requires extracting it, and it is useful for telemetry and routing.
    pub message_count: usize,
    /// Inbound headers, forwarded for rule matching.
    pub headers: HeaderMap,
    /// The original JSON payload, byte-for-byte, for zero-copy forwarding.
    pub raw_body: Bytes,
}

/// Parses an inbound request body into a [`ProxyRequest`].
///
/// # Errors
///
/// Returns [`RouterError::InvalidRequest`] when the body is not JSON, is not a
/// JSON object, or lacks a non-empty string `model`. The body is never mutated:
/// a failure simply drops the scratch copy.
pub fn inspect_request(headers: HeaderMap, raw_body: Bytes) -> Result<ProxyRequest, RouterError> {
    // simd-json needs a mutable buffer; it copies into its own padded storage,
    // so this scratch copy does not scale with the padding requirement.
    let mut scratch = raw_body.to_vec();

    let document: BorrowedValue<'_> =
        simd_json::from_slice(&mut scratch).map_err(|error| RouterError::InvalidRequest {
            message: format!("request body is not valid JSON: {error}"),
        })?;

    if document.as_object().is_none() {
        return Err(RouterError::InvalidRequest {
            message: "request body must be a JSON object".to_string(),
        });
    }

    let requested_model = document
        .get("model")
        .and_then(|model| model.as_str())
        .map(str::trim)
        .filter(|model| !model.is_empty())
        .ok_or_else(|| RouterError::InvalidRequest {
            message: "request body must include a non-empty string `model`".to_string(),
        })?
        .to_string();

    let is_stream = document
        .get("stream")
        .and_then(|stream| stream.as_bool())
        .unwrap_or(false);

    let messages = document.get("messages").and_then(|m| m.as_array());
    let message_count = messages.map_or(0, Vec::len);

    let mut prompt_text = String::new();
    for message in messages.into_iter().flatten() {
        append_message_text(message, &mut prompt_text);
    }

    let complexity_score = score_complexity(&prompt_text);

    Ok(ProxyRequest {
        request_id: new_request_id(),
        requested_model,
        is_stream,
        estimated_prompt_tokens: raw_body.len() / BYTES_PER_TOKEN,
        complexity_score,
        message_count,
        headers,
        raw_body,
    })
}

fn new_request_id() -> String {
    format!("req_{}", uuid::Uuid::new_v4().simple())
}

/// Appends the text content of one chat message.
///
/// Handles both the plain-string form and the multimodal array-of-parts form,
/// where only `type = "text"` parts contribute to the heuristic.
fn append_message_text(message: &BorrowedValue<'_>, out: &mut String) {
    let Some(content) = message.get("content") else {
        return;
    };

    if let Some(text) = content.as_str() {
        out.push_str(text);
        return;
    }

    let Some(parts) = content.as_array() else {
        return;
    };

    for part in parts {
        let is_text = part.get("type").and_then(|kind| kind.as_str()) == Some("text");
        if !is_text {
            continue;
        }
        if let Some(text) = part.get("text").and_then(|text| text.as_str()) {
            out.push_str(text);
        }
    }
}

/// Blends length, code density, and reasoning language into `0.0..=1.0`.
fn score_complexity(text: &str) -> f32 {
    let chars = text.chars().count();
    let length_component = (chars as f32 / LENGTH_SATURATION_CHARS).min(1.0) * 0.45;

    let code_fences = text.matches("```").count() as f32;
    let code_component = (code_fences / CODE_FENCE_SATURATION).min(1.0) * 0.25;

    let reasoning_hits = REASONING_KEYWORDS
        .iter()
        .map(|keyword| count_ignore_ascii_case(text, keyword))
        .sum::<usize>() as f32;
    let reasoning_component = (reasoning_hits / REASONING_SATURATION).min(1.0) * 0.30;

    (length_component + code_component + reasoning_component).clamp(0.0, 1.0)
}

fn count_ignore_ascii_case(haystack: &str, needle: &str) -> usize {
    let needle = needle.as_bytes();
    if needle.is_empty() || haystack.len() < needle.len() {
        return 0;
    }
    haystack
        .as_bytes()
        .windows(needle.len())
        .filter(|window| window.eq_ignore_ascii_case(needle))
        .count()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn body(json: &str) -> Bytes {
        Bytes::from(json.to_string())
    }

    fn inspect(json: &str) -> ProxyRequest {
        inspect_request(HeaderMap::new(), body(json)).expect("request inspects")
    }

    #[test]
    fn extracts_model_and_stream_flag() {
        let request = inspect(
            r#"{"model":"gpt-4o","stream":true,"messages":[{"role":"user","content":"hi"}]}"#,
        );

        assert_eq!(request.requested_model, "gpt-4o");
        assert!(request.is_stream);
        assert_eq!(request.message_count, 1);
        assert!(request.request_id.starts_with("req_"));
        assert_eq!(request.request_id.len(), 4 + 32);
    }

    #[test]
    fn stream_defaults_to_false() {
        let request = inspect(r#"{"model":"gpt-4o","messages":[]}"#);
        assert!(!request.is_stream);
    }

    #[test]
    fn raw_body_is_preserved_byte_for_byte() {
        let original = r#"{ "model" : "gpt-4o", "z": 1, "a": [1,2,3] }"#;
        let bytes = body(original);
        let request = inspect_request(HeaderMap::new(), bytes.clone()).expect("inspects");

        assert_eq!(request.raw_body, bytes);
        assert_eq!(request.raw_body.as_ref(), original.as_bytes());
    }

    #[test]
    fn missing_model_is_rejected() {
        let error = inspect_request(HeaderMap::new(), body(r#"{"messages":[]}"#))
            .expect_err("model is required");

        assert!(matches!(error, RouterError::InvalidRequest { .. }));
        assert!(error.to_string().contains("`model`"));
    }

    #[test]
    fn blank_model_is_rejected() {
        let error = inspect_request(HeaderMap::new(), body(r#"{"model":"   "}"#))
            .expect_err("blank model is not usable");

        assert!(matches!(error, RouterError::InvalidRequest { .. }));
    }

    #[test]
    fn non_string_model_is_rejected() {
        let error = inspect_request(HeaderMap::new(), body(r#"{"model":42}"#))
            .expect_err("model must be a string");

        assert!(matches!(error, RouterError::InvalidRequest { .. }));
    }

    #[test]
    fn malformed_json_is_rejected() {
        let error = inspect_request(HeaderMap::new(), body(r#"{"model":"#))
            .expect_err("truncated JSON must fail");

        assert!(matches!(error, RouterError::InvalidRequest { .. }));
    }

    #[test]
    fn non_object_body_is_rejected() {
        let error = inspect_request(HeaderMap::new(), body(r#"["model"]"#))
            .expect_err("arrays are not chat requests");

        assert!(matches!(error, RouterError::InvalidRequest { .. }));
    }

    #[test]
    fn empty_body_is_rejected() {
        assert!(inspect_request(HeaderMap::new(), Bytes::new()).is_err());
    }

    #[test]
    fn token_estimate_uses_four_bytes_per_token() {
        let raw = r#"{"model":"gpt-4o","messages":[]}"#;
        let request = inspect(raw);

        assert_eq!(request.estimated_prompt_tokens, raw.len() / 4);
    }

    #[test]
    fn complexity_is_strictly_within_unit_interval() {
        for prompt in [
            "",
            "hi",
            "Write a function.",
            &"a".repeat(50_000),
            &"```rust\nfn main() {}\n```".repeat(200),
            "Think step by step and reason about the trade-offs, then prove it.",
        ] {
            let json = serde_json::json!({
                "model": "gpt-4o",
                "messages": [{"role": "user", "content": prompt}],
            })
            .to_string();
            let score = inspect(&json).complexity_score;

            assert!(
                (0.0..=1.0).contains(&score),
                "score {score} out of range for prompt of len {}",
                prompt.len()
            );
        }
    }

    #[test]
    fn trivial_prompt_scores_below_cost_aware_threshold() {
        // `examples/production.toml` routes `complexity_score_below = 0.35`
        // to the cheap model; a greeting must land there.
        let score =
            inspect(r#"{"model":"auto","messages":[{"role":"user","content":"hello there"}]}"#)
                .complexity_score;

        assert!(score < 0.35, "trivial prompt scored {score}");
    }

    #[test]
    fn code_heavy_prompt_scores_higher_than_trivial_prompt() {
        let trivial =
            inspect(r#"{"model":"auto","messages":[{"role":"user","content":"hello there"}]}"#)
                .complexity_score;

        let fenced = "```rust\nfn a() {}\n```\n```rust\nfn b() {}\n```\n```rust\nfn c() {}\n```";
        let json = serde_json::json!({
            "model": "auto",
            "messages": [{"role": "user", "content": fenced}],
        })
        .to_string();
        let heavy = inspect(&json).complexity_score;

        assert!(
            heavy > trivial,
            "heavy {heavy} should exceed trivial {trivial}"
        );
    }

    #[test]
    fn long_prompt_scores_higher_than_short_prompt() {
        let short = inspect(r#"{"model":"auto","messages":[{"role":"user","content":"hi"}]}"#)
            .complexity_score;
        let long_text = "word ".repeat(3_000);
        let json = serde_json::json!({
            "model": "auto",
            "messages": [{"role": "user", "content": long_text}],
        })
        .to_string();
        let long = inspect(&json).complexity_score;

        assert!(long > short, "long {long} should exceed short {short}");
    }

    #[test]
    fn multimodal_text_parts_contribute_to_complexity() {
        let with_text = serde_json::json!({
            "model": "auto",
            "messages": [{"role": "user", "content": [
                {"type": "text", "text": "Think step by step and prove the claim with ```code```"},
                {"type": "image_url", "image_url": {"url": "data:image/png;base64,AAAA"}}
            ]}]
        })
        .to_string();

        let image_only = serde_json::json!({
            "model": "auto",
            "messages": [{"role": "user", "content": [
                {"type": "image_url", "image_url": {"url": "data:image/png;base64,AAAA"}}
            ]}]
        })
        .to_string();

        let with_text_score = inspect(&with_text).complexity_score;
        let image_only_score = inspect(&image_only).complexity_score;

        assert!(
            with_text_score > image_only_score,
            "text part should raise the score: {with_text_score} vs {image_only_score}"
        );
    }

    #[test]
    fn messages_may_be_absent() {
        let request = inspect(r#"{"model":"gpt-4o"}"#);
        assert_eq!(request.message_count, 0);
    }

    #[test]
    fn headers_are_carried_through() {
        let mut headers = HeaderMap::new();
        headers.insert("x-tenant-id", "acme".parse().expect("valid header"));

        let request = inspect_request(headers, body(r#"{"model":"gpt-4o"}"#)).expect("inspects");

        assert_eq!(
            request
                .headers
                .get("x-tenant-id")
                .and_then(|value| value.to_str().ok()),
            Some("acme")
        );
    }

    #[test]
    fn ascii_case_insensitive_matching_counts_hits() {
        assert_eq!(
            count_ignore_ascii_case("Step By Step reasoning", "step by step"),
            1
        );
        assert_eq!(count_ignore_ascii_case("reason reason", "reason"), 2);
        assert_eq!(count_ignore_ascii_case("short", "a much longer needle"), 0);
    }
}

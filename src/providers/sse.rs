//! Shared SSE framing and OpenAI chunk rendering.
//!
//! Both the Anthropic and Gemini adapters face the same problem: transport
//! chunks can split an event at *any* byte, including inside a multi-byte
//! character or between `event:` and `data:`. [`SseFrameParser`] owns that
//! buffering so each adapter only deals in whole events.
//!
//! Rendering goes through the structs below rather than `serde_json::json!`
//! because `Value`'s object type sorts keys, while these structs serialize in
//! declaration order — matching the frame layout in `docs/SPECS_AND_SCHEMAS.md`
//! and making fixture comparisons byte-for-byte stable.

use bytes::Bytes;
use serde::Serialize;

/// The OpenAI stream terminator, emitted verbatim.
pub const DONE_FRAME: &[u8] = b"data: [DONE]\n\n";

/// One complete SSE event.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SseEvent {
    /// Value of the `event:` field, when the upstream sends one.
    pub event: Option<String>,
    /// Joined value of all `data:` lines in the event.
    pub data: String,
}

/// Line-buffered SSE parser that survives arbitrary chunk boundaries.
#[derive(Debug, Default)]
pub struct SseFrameParser {
    buffer: Vec<u8>,
    event: Option<String>,
    data: Vec<String>,
}

impl SseFrameParser {
    /// Creates an empty parser.
    pub fn new() -> Self {
        Self::default()
    }

    /// Appends bytes and returns every event that is now complete.
    ///
    /// A trailing partial line stays buffered until the rest arrives.
    pub fn push(&mut self, bytes: &[u8]) -> Vec<SseEvent> {
        self.buffer.extend_from_slice(bytes);

        let mut events = Vec::new();
        while let Some(position) = self.buffer.iter().position(|byte| *byte == b'\n') {
            let line: Vec<u8> = self.buffer.drain(..=position).collect();
            self.consume_line(strip_terminator(&line), &mut events);
        }
        events
    }

    /// Flushes any unterminated trailing line and returns a final pending event.
    ///
    /// Upstreams that close without a trailing blank line would otherwise lose
    /// their last event.
    pub fn finish(&mut self) -> Option<SseEvent> {
        let mut events = Vec::new();

        if !self.buffer.is_empty() {
            let line = std::mem::take(&mut self.buffer);
            self.consume_line(&line, &mut events);
        }

        if let Some(event) = self.take_pending() {
            events.push(event);
        }

        events.into_iter().next()
    }

    fn consume_line(&mut self, line: &[u8], events: &mut Vec<SseEvent>) {
        if line.is_empty() {
            // A blank line dispatches the accumulated event.
            if let Some(event) = self.take_pending() {
                events.push(event);
            }
            return;
        }

        // `:` starts a comment (used for keep-alives).
        if line.starts_with(b":") {
            return;
        }

        if let Some(rest) = line.strip_prefix(b"event:") {
            self.event = Some(String::from_utf8_lossy(trim_leading_space(rest)).into_owned());
        } else if let Some(rest) = line.strip_prefix(b"data:") {
            self.data
                .push(String::from_utf8_lossy(trim_leading_space(rest)).into_owned());
        }
        // Other fields (`id:`, `retry:`) are irrelevant to translation.
    }

    fn take_pending(&mut self) -> Option<SseEvent> {
        let data = std::mem::take(&mut self.data);
        let event = self.event.take();

        if data.is_empty() {
            return None;
        }

        Some(SseEvent {
            event,
            data: data.join("\n"),
        })
    }
}

fn strip_terminator(line: &[u8]) -> &[u8] {
    let line = line.strip_suffix(b"\n").unwrap_or(line);
    line.strip_suffix(b"\r").unwrap_or(line)
}

fn trim_leading_space(bytes: &[u8]) -> &[u8] {
    bytes.strip_prefix(b" ").unwrap_or(bytes)
}

/// Builds `chat.completion.chunk` frames for a single logical completion.
#[derive(Debug, Clone)]
pub struct ChunkBuilder {
    id: String,
    model: String,
    created: u64,
}

impl ChunkBuilder {
    /// Creates a builder for one response stream.
    pub fn new(id: impl Into<String>, model: impl Into<String>, created: u64) -> Self {
        Self {
            id: id.into(),
            model: model.into(),
            created,
        }
    }

    /// Replaces the completion id, e.g. once Anthropic reports it.
    pub fn set_id(&mut self, id: impl Into<String>) {
        self.id = id.into();
    }

    /// Replaces the model name, e.g. once Anthropic reports it.
    pub fn set_model(&mut self, model: impl Into<String>) {
        self.model = model.into();
    }

    /// Frame carrying `role: assistant` and an empty first delta.
    pub fn role_chunk(&self) -> Bytes {
        self.render(
            DeltaFrame {
                role: Some("assistant"),
                content: Some(""),
            },
            None,
            None,
        )
    }

    /// Frame carrying one text delta.
    pub fn content_chunk(&self, content: &str) -> Bytes {
        self.render(
            DeltaFrame {
                role: None,
                content: Some(content),
            },
            None,
            None,
        )
    }

    /// Terminal frame carrying the finish reason and token accounting.
    pub fn finish_chunk(
        &self,
        finish_reason: &str,
        prompt_tokens: usize,
        completion_tokens: usize,
    ) -> Bytes {
        self.render(
            DeltaFrame {
                role: None,
                content: None,
            },
            Some(finish_reason),
            Some(UsageFrame {
                prompt_tokens,
                completion_tokens,
                total_tokens: prompt_tokens + completion_tokens,
            }),
        )
    }

    /// Renders a non-streaming `chat.completion` document.
    pub fn completion(&self, content: &str, finish_reason: &str) -> serde_json::Value {
        serde_json::json!({
            "id": self.id,
            "object": "chat.completion",
            "created": self.created,
            "model": self.model,
            "choices": [{
                "index": 0,
                "message": {"role": "assistant", "content": content},
                "finish_reason": finish_reason,
            }],
        })
    }

    fn render(
        &self,
        delta: DeltaFrame<'_>,
        finish_reason: Option<&str>,
        usage: Option<UsageFrame>,
    ) -> Bytes {
        let frame = ChunkFrame {
            id: &self.id,
            object: "chat.completion.chunk",
            created: self.created,
            model: &self.model,
            choices: vec![ChoiceFrame {
                index: 0,
                delta,
                finish_reason,
            }],
            usage,
        };

        match serde_json::to_string(&frame) {
            Ok(json) => Bytes::from(format!("data: {json}\n\n")),
            // The frame contains only strings and integers, so serialization
            // cannot fail; returning empty bytes keeps the path panic-free.
            Err(_) => Bytes::new(),
        }
    }
}

/// Current wall-clock time in whole seconds, used for `created`.
pub fn unix_now() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|elapsed| elapsed.as_secs())
        .unwrap_or(0)
}

#[derive(Debug, Serialize)]
struct ChunkFrame<'a> {
    id: &'a str,
    object: &'a str,
    created: u64,
    model: &'a str,
    choices: Vec<ChoiceFrame<'a>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    usage: Option<UsageFrame>,
}

#[derive(Debug, Serialize)]
struct ChoiceFrame<'a> {
    index: u32,
    delta: DeltaFrame<'a>,
    finish_reason: Option<&'a str>,
}

#[derive(Debug, Serialize)]
struct DeltaFrame<'a> {
    #[serde(skip_serializing_if = "Option::is_none")]
    role: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    content: Option<&'a str>,
}

#[derive(Debug, Serialize)]
struct UsageFrame {
    prompt_tokens: usize,
    completion_tokens: usize,
    total_tokens: usize,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_a_single_event() {
        let mut parser = SseFrameParser::new();
        let events = parser.push(b"event: message_start\ndata: {\"a\":1}\n\n");

        assert_eq!(
            events,
            vec![SseEvent {
                event: Some("message_start".to_string()),
                data: "{\"a\":1}".to_string(),
            }]
        );
    }

    #[test]
    fn holds_partial_lines_until_terminated() {
        let mut parser = SseFrameParser::new();

        assert!(parser
            .push(b"event: message_start\ndata: {\"a\"")
            .is_empty());
        assert!(parser.push(b":1}").is_empty());
        let events = parser.push(b"\n\n");

        assert_eq!(events.len(), 1);
        assert_eq!(events[0].data, "{\"a\":1}");
    }

    #[test]
    fn survives_being_fed_one_byte_at_a_time() {
        let raw = b"event: content_block_delta\ndata: {\"text\":\"hi\"}\n\nevent: message_stop\ndata: {}\n\n";
        let mut parser = SseFrameParser::new();
        let mut events = Vec::new();

        for byte in raw {
            events.extend(parser.push(&[*byte]));
        }

        assert_eq!(events.len(), 2);
        assert_eq!(events[0].data, "{\"text\":\"hi\"}");
        assert_eq!(events[1].event.as_deref(), Some("message_stop"));
    }

    #[test]
    fn ignores_comment_lines() {
        let mut parser = SseFrameParser::new();
        let events = parser.push(b": ping\n\n: ping\n\ndata: x\n\n");

        assert_eq!(events.len(), 1);
        assert_eq!(events[0].data, "x");
        assert!(events[0].event.is_none());
    }

    #[test]
    fn joins_multiple_data_lines() {
        let mut parser = SseFrameParser::new();
        let events = parser.push(b"data: line1\ndata: line2\n\n");

        assert_eq!(events[0].data, "line1\nline2");
    }

    #[test]
    fn handles_crlf_terminators() {
        let mut parser = SseFrameParser::new();
        let events = parser.push(b"event: ping\r\ndata: {}\r\n\r\n");

        assert_eq!(events.len(), 1);
        assert_eq!(events[0].event.as_deref(), Some("ping"));
        assert_eq!(events[0].data, "{}");
    }

    #[test]
    fn finish_flushes_an_unterminated_event() {
        let mut parser = SseFrameParser::new();
        assert!(parser.push(b"data: {\"a\":1}").is_empty());

        let event = parser.finish().expect("flushes the trailing event");
        assert_eq!(event.data, "{\"a\":1}");
    }

    #[test]
    fn renders_frames_in_documented_order() {
        let builder = ChunkBuilder::new("msg_1", "claude-x", 0);
        let frame = builder.content_chunk("hi");
        let rendered = String::from_utf8_lossy(&frame);

        assert!(rendered.starts_with("data: {"));
        assert!(rendered.ends_with("}\n\n"));
        assert!(
            rendered.find("\"id\"").unwrap() < rendered.find("\"object\"").unwrap(),
            "id must precede object: {rendered}"
        );
        assert!(rendered.contains("\"delta\":{\"content\":\"hi\"}"));
        assert!(rendered.contains("\"finish_reason\":null"));
    }

    #[test]
    fn finish_chunk_carries_totals() {
        let builder = ChunkBuilder::new("id", "m", 0);
        let rendered = String::from_utf8_lossy(&builder.finish_chunk("stop", 3, 4)).into_owned();

        assert!(rendered.contains("\"finish_reason\":\"stop\""));
        assert!(rendered.contains("\"prompt_tokens\":3"));
        assert!(rendered.contains("\"completion_tokens\":4"));
        assert!(rendered.contains("\"total_tokens\":7"));
        assert!(rendered.contains("\"delta\":{}"));
    }
}

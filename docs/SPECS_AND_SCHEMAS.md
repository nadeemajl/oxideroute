# OxideRoute — Configuration Specs & Protocol Normalization Schemas

## 1. Configuration Specification (`oxide.toml`)

### 1.1 Environment Variable Interpolation Rules
Before deserializing `oxide.toml` into `RouterConfig`, `src/config/loader.rs` must run a regex interpolation pass matching `\$\{([A-Z0-9_]+)(?::-([^}]*))?\}`:
1. **`${VAR_NAME}`**: Replaced with `std::env::var("VAR_NAME")`. If unset in `serve` mode, return a `ConfigError::MissingEnvVar("VAR_NAME")`.
2. **`${VAR_NAME:-default_val}`**: Replaced with `std::env::var("VAR_NAME").unwrap_or_else(|_| "default_val".to_string())`.

### 1.2 Minimal Quickstart Configuration (`examples/minimal.toml`)
```toml
#:schema https://schema.oxideroute.dev/v1/oxide.schema.json
listen = "0.0.0.0:8080"

[providers]
openai = { type = "openai", api_key = "${OPENAI_API_KEY}" }
anthropic = { type = "anthropic", api_key = "${ANTHROPIC_API_KEY}" }

[models."gpt-4o"]
primary = "openai/gpt-4o"
fallbacks = ["anthropic/claude-3-7-sonnet-latest"]
```

### 1.3 Full Production Configuration (`examples/production.toml`)
```toml
#:schema https://schema.oxideroute.dev/v1/oxide.schema.json
listen = "0.0.0.0:8080"
admin_listen = "127.0.0.1:9090"   # serves /metrics and /healthz (admin API pending); must differ from `listen`
graceful_shutdown_timeout_ms = 30000  # hard deadline for draining after SIGTERM

[telemetry]
prometheus_endpoint = "/metrics"
otlp_endpoint = "${OTEL_EXPORTER_OTLP_ENDPOINT:-http://localhost:4317}"
redact_pii = true
log_payloads = false

[cache]
enabled = true
backend = "memory"
max_capacity_mb = 512
ttl_seconds = 3600

[resiliency.circuit_breaker]
consecutive_failures_threshold = 5
error_rate_threshold_pct = 50.0
window_seconds = 30
cooldown_seconds = 15
active_probe_interval_seconds = 10

[rate_limits.default_tenant]
match_header = "X-Tenant-ID"
rpm = 600
tpm = 250000
daily_budget_usd = 50.00
burst_multiplier = 1.5

[providers.openai_us]
type = "openai"
api_key = "${OPENAI_API_KEY}"
timeout_ms = 60000
ttft_timeout_ms = 2500

[providers.anthropic_prod]
type = "anthropic"
api_key = "${ANTHROPIC_API_KEY}"
ttft_timeout_ms = 3000

[providers.gemini_vertex]
type = "gemini"
api_key = "${GEMINI_API_KEY}"
ttft_timeout_ms = 4000

[providers.local_ollama]
type = "openai_compatible"
base_url = "http://127.0.0.1:11434/v1"
ttft_timeout_ms = 1500

[models."smart-router"]
strategy = "latency_ewma"
targets = [
  { upstream = "openai_us/gpt-4o", weight = 70 },
  { upstream = "anthropic_prod/claude-3-7-sonnet-latest", weight = 30 }
]
fallbacks = ["gemini_vertex/gemini-2.5-pro", "local_ollama/qwen2.5-coder:32b"]
retry_on_status = [429, 500, 502, 503, 504, 529]

[[rules]]
name = "long-context-to-gemini"
condition = { min_estimated_tokens = 32000 }
route_to = "gemini_vertex/gemini-2.5-pro"

[[rules]]
name = "privacy-sensitive-local"
condition = { header_matches = { "X-Data-Classification" = "confidential" } }
route_to = "local_ollama/qwen2.5-coder:32b"

[[rules]]
name = "cost-aware-complexity-tiering"
condition = { model = "auto", complexity_score_below = 0.35 }
route_to = "openai_us/gpt-4o-mini"
```

---

## 2. Protocol Translation & SSE Normalization (`src/providers/`)

All downstream responses emitted by OxideRoute must conform to the OpenAI `/v1/chat/completions` specification.

### 2.1 OpenAI -> Anthropic Request Translation (`src/providers/anthropic.rs`)
1. **Endpoint:** `POST {base_url}/v1/messages` (default `https://api.anthropic.com/v1/messages`).
2. **Required Headers:**
   - `x-api-key: {api_key}`
   - `anthropic-version: 2023-06-01`
   - `content-type: application/json`
3. **Body Transformation:**
   - Extract all messages with `role == "system"` and concatenate their text into the top-level `"system"` string field.
   - Pass remaining `user` and `assistant` messages inside `"messages"`.
   - If `max_tokens` is omitted in the incoming OpenAI request, set `"max_tokens": 4096` (required by Anthropic).

### 2.2 Anthropic -> OpenAI SSE Stream State Machine
When `"stream": true`, Anthropic emits `event: <type>
data: <json>

` frames. Translate each frame into an OpenAI `data: <json>

` chunk as follows:

| Anthropic SSE Event | Action in OxideRoute Stream Adapter | Emitted OpenAI SSE Frame |
| :--- | :--- | :--- |
| `message_start` | Extract `message.id` and `message.usage.input_tokens`. | `data: {"id":"<id>","object":"chat.completion.chunk","created":<ts>,"model":"<model>","choices":[{"index":0,"delta":{"role":"assistant","content":""},"finish_reason":null}]}

` |
| `content_block_delta` (`text_delta`) | Extract `delta.text`. | `data: {"id":"<id>","object":"chat.completion.chunk","created":<ts>,"model":"<model>","choices":[{"index":0,"delta":{"content":"<delta.text>"},"finish_reason":null}]}

` |
| `message_delta` | Map `delta.stop_reason` (`end_turn` -> `"stop"`, `max_tokens` -> `"length"`). Record `usage.output_tokens`. | `data: {"id":"<id>","object":"chat.completion.chunk","created":<ts>,"model":"<model>","choices":[{"index":0,"delta":{},"finish_reason":"stop"}],"usage":{"prompt_tokens":<in>,"completion_tokens":<out>,"total_tokens":<sum>}}

` |
| `message_stop` | Flush stream termination marker. | `data: [DONE]

` |
| `ping` | Ignore / do not count as first content token for TTFT watchdog. | *(None)* |

### 2.3 OpenAI -> Google Gemini Translation (`src/providers/gemini.rs`)
1. **Endpoint:** `POST https://generativelanguage.googleapis.com/v1beta/models/{model}:streamGenerateContent?alt=sse&key={api_key}`
2. **Body Transformation:**
   - Map `role == "system"` into `"systemInstruction": { "parts": [{ "text": "..." }] }`.
   - Map `user` -> `"role": "user"` and `assistant` -> `"role": "model"` inside `"contents"`.
3. **SSE Stream Translation:**
   - Parse each `data: <GenerateContentResponse>` JSON line, extract `candidates[0].content.parts[0].text`, and wrap it in an OpenAI `chat.completion.chunk` frame. Emit `data: [DONE]

` when stream closes.

---

## 3. Unified OpenAI Error Envelope (`src/error.rs`)
Whenever a request is rejected (e.g., 429 Tenant Rate Limit, 400 Invalid Request) or all upstream fallbacks fail (`502 Bad Gateway` / `504 Gateway Timeout`), return:

```json
{
  "error": {
    "message": "All configured upstreams failed for model 'smart-router'.",
    "type": "upstream_router_error",
    "param": null,
    "code": "all_upstreams_failed",
    "oxide_metadata": {
      "request_id": "req_01j9z8x7w6v5u4t3s2r1",
      "attempted_upstreams": [
        {
          "provider": "openai_us/gpt-4o",
          "reason": "ttft_timeout_exceeded (2500ms)",
          "elapsed_ms": 2501
        },
        {
          "provider": "anthropic_prod/claude-3-7-sonnet-latest",
          "reason": "http_status_429",
          "elapsed_ms": 84
        }
      ]
    }
  }
}
```

---

## 4. Admin Response Schemas (`src/admin/mod.rs`)

The admin surface has its own document because its schemas are a client contract
rather than a wire-protocol translation:
[`ADMIN_API.md`](ADMIN_API.md). In brief, every response is built from these
types:

| Type | Path | Carries |
|---|---|---|
| `AdminSnapshot` | `/admin/snapshot` | config generation, redacted config, secret posture, health, tenants, last reload |
| `RedactedConfig` | `/admin/config` | the live document with keys masked and URL userinfo stripped |
| `HealthSnapshot` | `/admin/health` | EWMA vs watchdog, breaker phase/countdown, orphans |
| `RouteExplain` | `/admin/explain` | attempt order from the real resolver, labelled by source |
| `ValidationReport` | `/admin/config/validate` | `miette` code, message, help, field path, 1-based source span |
| `ReloadStatus` | `/admin/config/apply` | generation, source, `applied`/`rejected`, diagnostic |

Two schema decisions are load-bearing. `ewma_ttft_us` is nullable because `0` is
the "never observed" sentinel and a UI must not render it as instant. Rejection is
a `200`-shaped `ReloadStatus` with `outcome: "rejected"` (HTTP `422` from the
handler), not a transport error: the diagnostic *is* the payload the editor
renders next to the draft.

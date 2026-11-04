# OxideRoute — Phased Implementation Plan for Coding Agent

Execute these six milestones sequentially. Do not proceed to the next milestone until all verification commands for the current milestone pass with zero warnings.

---

## Milestone 1: Workspace Scaffolding, Config Loader & JSON Schema
### Deliverables
1. **`Cargo.toml`**: Initialize project with the exact dependencies from `docs/ARCHITECTURE.md`.
2. **`src/error.rs`**: Implement `RouterError`, `AttemptRecord`, and `axum::response::IntoResponse` producing the unified JSON error envelope.
3. **`src/config/mod.rs`**: Define `RouterConfig`, `ProviderConfig`, `VirtualModelConfig`, `RoutingRule`, `RateLimitConfig`, and `CircuitBreakerConfig` with `#[derive(Deserialize, Serialize, JsonSchema)]`.
4. **`src/config/loader.rs`**:
   - Implement `${ENV_VAR}` and `${ENV_VAR:-default}` regex interpolation.
   - Implement semantic validation: verify every `provider/model` string in `models` and `rules` references a defined key in `[providers]`, and detect circular routing rules.
5. **`examples/minimal.toml` & `examples/production.toml`**: Save the two reference configurations from `docs/SPECS_AND_SCHEMAS.md`.

### Verification
- Run `cargo test config::` — verify unit tests for env interpolation, valid parsing of `examples/minimal.toml` and `examples/production.toml`, and rejection of undefined provider references.

---

## Milestone 2: Zero-Copy Request Inspector, Provider Trait & OpenAI Pass-Through
### Deliverables
1. **`src/proxy/inspector.rs`**:
   - Implement `inspect_request(headers: HeaderMap, raw_body: Bytes) -> Result<ProxyRequest, RouterError>`.
   - Use `simd_json` on a short-lived mutable slice copy (or header inspection) to extract `model`, `stream`, and `messages` count, calculate `estimated_prompt_tokens`, and compute a fast heuristic complexity score (`0.0..=1.0` based on prompt length, code fences, and reasoning keywords).
   - Keep `raw_body: Bytes` untouched for zero-copy forwarding.
2. **`src/providers/mod.rs`**: Define `LlmProvider`, `SseByteStream`, and `build_upstream_http_client`.
3. **`src/providers/openai.rs`**:
   - Implement `OpenAiProvider` (supporting both `type = "openai"` and `type = "openai_compatible"` for vLLM/Ollama).
   - If the incoming `requested_model` matches `upstream_model`, forward `req.raw_body.clone()` with zero re-serialization. If the model name was rewritten by a virtual model or routing rule, rewrite only the `"model"` field.
   - In `execute_stream`, await the HTTP 200 status, pull chunks from `response.bytes_stream()`, ignore empty/keep-alive `: ping` lines until the first `data:` chunk arrives, and return `Ok((first_chunk, remaining_stream))`.

### Verification
- Run `cargo test providers::openai::` using an in-process mock server (`tests/mock_upstreams.rs`) yielding chunked SSE frames.

---

## Milestone 3: Anthropic & Gemini Stream Normalizers
### Deliverables
1. **`src/providers/anthropic.rs`**:
   - Implement request payload conversion from OpenAI chat format to Anthropic `/v1/messages`.
   - Implement a `Stream` state machine (`AnthropicSseNormalizer`) that buffers partial lines up to `

`, parses `event:` and `data:`, ignores `ping` events during the TTFT wait phase, and yields OpenAI-formatted `data: {...}

` `Bytes` chunks.
2. **`src/providers/gemini.rs`**:
   - Implement Google Gemini `streamGenerateContent?alt=sse` request/response translation into OpenAI SSE chunks.

### Verification
- Run `cargo test providers::anthropic::` and `cargo test providers::gemini::` to verify that raw Anthropic and Gemini SSE fixtures are transformed byte-for-byte into valid OpenAI `chat.completion.chunk` streams terminating with `data: [DONE]

`.

---

## Milestone 4: Routing Engine, TTFT Watchdog, Circuit Breakers & Rate Limiting
### Deliverables
1. **`src/state.rs`**: Implement `AppState` with `ArcSwap<RouterConfig>`, `DashMap<String, Arc<dyn LlmProvider>>`, and `DashMap<String, Arc<UpstreamHealthMetrics>>`.
2. **`src/proxy/router_engine.rs`**:
   - Evaluate `[[rules]]` in priority order (`min_estimated_tokens`, `header_matches`, `complexity_score_below`).
   - Resolve virtual models using `Priority`, `WeightedRandom`, or `LatencyEwma` (sorting healthy targets by lowest `ewma_ttft_us`).
3. **`src/proxy/executor.rs`**:
   - Implement `dispatch_with_fallbacks` wrapping `provider.execute_stream` in `tokio::time::timeout(ttft_timeout_ms)`.
   - Record TTFT in `UpstreamHealthMetrics::record_success` or increment circuit breaker failures in `record_failure`.
4. **`src/proxy/ratelimit.rs`**:
   - Implement per-tenant GCRA rate limiters (`governor`) for RPM and TPM, plus atomic USD budget enforcement.
5. **`src/config/watcher.rs`**:
   - Implement `notify` file watcher and `SIGHUP` signal listener that validates the updated TOML file and atomically swaps `AppState.config` via `ArcSwap::store`.

### Verification
- Run `cargo test --test failover_tests` and `cargo test --test config_reload_tests`.

---

## Milestone 5: Telemetry, CLI Subcommands & Final Polish
### Deliverables
1. **`src/telemetry/metrics.rs`**:
   - Expose `/metrics` via `metrics-exporter-prometheus` tracking:
     - `oxide_ttft_seconds` (histogram by provider, model)
     - `oxide_requests_total` (counter by provider, model, status)
     - `oxide_fallback_triggered_total` (counter by from_provider, to_provider, reason)
     - `oxide_tokens_total` (counter by tenant, provider, token_type)
2. **`src/main.rs`**: Implement the `clap` CLI:
   - `oxide serve [--config oxide.toml]`
   - `oxide validate [--config oxide.toml] [--check-upstreams]`
   - `oxide dry-run [--config oxide.toml] --model <MODEL> [--header KEY=VAL] --prompt <TEXT>`
   - `oxide init [--output oxide.toml]`
   - `oxide schema`

### Verification
- Execute `cargo clippy --all-targets -- -D warnings` and `cargo test --all-targets`.
- Verify `cargo run -- dry-run --config examples/production.toml --model auto --prompt "Hello"` outputs a formatted routing trace table.

---

## Milestone 6: Admin Surface (TUI and Embedded SPA)
The admin surface is not a Grafana replacement: `/metrics` already covers rates,
p99 TTFT, the rejection split, and inflight requests. It carries only what a
scrape cannot — configuration, breaker phase and countdown, tenant budgets,
reload diagnostics, and a routing X-ray. The client contract, including the JSON
shapes and the gaps still open, is [`ADMIN_API.md`](ADMIN_API.md).

### Deliverables
1. **`src/admin/mod.rs`** *(implemented)*:
   - `AdminState` over `Arc<AppState>`: generation counter, `ArcSwap<ReloadStatus>`,
     `WritePolicy::ReadOnly` default. No field added to `AppState`.
   - `RedactedConfig` masking keys and URL userinfo by construction;
     `SecretStatus` rows so an operator can see a key is present without it.
   - `HealthSnapshot` joining the health table against the config: nullable
     `ewma_ttft_us`, `ttft_headroom_pct` against each provider's watchdog,
     breaker phase/countdown, and `configured: false` orphans.
   - `ValidationReport` with `miette` codes and 1-based source spans;
     lenient parse for validation, strict interpolation for apply.
   - `RouteExplain` running `inspect_request` + `resolve_candidates`, no upstream
     call, candidates labelled `primary`/`target`/`fallback`/`rule_target`.
2. **`src/admin/http.rs`** *(pending)*: axum handlers for the six paths, with
   `Extension<AdminState>` on `build_admin_router` so the handler state can stay
   `Arc<AppState>`. `apply` returns `403` when read-only, `422` on rejection.
3. **`src/main.rs`** *(pending)*: `serve` constructs `AdminState` and passes it to
   `build_admin_router`; writes stay disabled until a `ui.allow_writes` key
   (schema-validated) opts in.
4. **`src/config/watcher.rs`** *(pending)*: call `note_reload` on file and
   `SIGHUP` reloads so `last_reload` matches the serving generation.
5. **`oxide top`** *(pending)*: ratatui client polling `/admin/snapshot` at 1 Hz,
   with `POST /admin/explain` on demand. Proves the contract before any asset
   pipeline exists.
6. **Embedded SPA** *(later)*: static assets behind a feature flag, served on
   `admin_listen` from the same JSON; no server change required.

### Verification
- `cargo test --lib admin::` — redaction never leaks a key, breaker phase and
  countdown derive correctly, orphan rows are flagged, a rejected apply keeps the
  previous config live, and the X-ray renders attempt order with sources.
- Once wired: `curl -s localhost:9090/admin/snapshot | jq .config_generation`
  returns the serving generation, and `POST /admin/config/apply` with an invalid
  body returns `422` while the old config keeps serving.

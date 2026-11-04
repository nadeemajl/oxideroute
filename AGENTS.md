# AGENTS.md — Autonomous Coding Agent Directives for OxideRoute

## 1. Project Overview
**OxideRoute** (`oxide`) is an ultra-low-latency, single-binary LLM router and API gateway written in Rust. It sits between client applications and upstream LLM providers (OpenAI, Anthropic, Google Gemini, AWS Bedrock, and local vLLM/Ollama instances), providing an OpenAI-compatible ingress API (`/v1/chat/completions`, `/v1/embeddings`), declarative fallback chains, EWMA latency-aware load balancing, dual-layer rate limiting (RPM/TPM), and zero-downtime hot configuration reloading.

Always consult the companion specifications in `docs/` before implementing or modifying modules:
- `docs/ARCHITECTURE.md` — Crate stack, module hierarchy, concurrency invariants, and core Rust traits.
- `docs/SPECS_AND_SCHEMAS.md` — TOML configuration syntax, environment interpolation, SSE chunk translation state machines, and unified error schemas.
- `docs/IMPLEMENTATION_PLAN.md` — Sequential 6-milestone build order with file-level deliverables and verification commands.
- `docs/TESTING_AND_BENCHMARKS.md` — Deterministic in-process mock SSE server specifications, chaos test scenarios, and latency budgets.
- `docs/ADMIN_API.md` — The admin API contract shared by the TUI and the SPA: endpoints, redaction rules, reload semantics, and the gaps still to close.

---

## 2. Build, Lint, and Test Commands
Run these commands after every major file or module change:
- **Check compilation:** `cargo check --all-targets`
- **Format check:** `cargo fmt --all -- --check`
- **Strict linter:** `cargo clippy --all-targets -- -D warnings`
- **Run unit & integration tests:** `cargo test --all-targets`
- **Generate JSON Schema:** `cargo run -- schema > oxide.schema.json`
- **Validate example configs:** `cargo run -- validate --config examples/minimal.toml`

---

## 3. Directory & Module Structure
```text
oxide-route/
├── Cargo.toml
├── AGENTS.md
├── docs/
│   ├── ARCHITECTURE.md
│   ├── SPECS_AND_SCHEMAS.md
│   ├── IMPLEMENTATION_PLAN.md
│   ├── TESTING_AND_BENCHMARKS.md
│   └── ADMIN_API.md
├── examples/
│   ├── minimal.toml
│   └── production.toml
├── src/
│   ├── main.rs                 # CLI entrypoint (`clap`): serve, validate, dry-run, init, schema
│   ├── lib.rs                  # Library root exporting AppState, server builder, and config types
│   ├── error.rs                # RouterError enum, AttemptRecord, OpenAI-normalized JSON error response
│   ├── state.rs                # AppState (ArcSwap<RouterConfig>, DashMap providers & health metrics)
│   ├── admin/
│   │   └── mod.rs              # AdminState, redacted config view, health/tenant snapshots, route X-ray
│   ├── config/
│   │   ├── mod.rs              # RouterConfig, ProviderConfig, VirtualModelConfig, RoutingRule
│   │   ├── loader.rs           # TOML parsing, ${ENV_VAR} interpolation, semantic validation
│   │   └── watcher.rs          # notify file watcher + SIGHUP handler for atomic ArcSwap reload
│   ├── proxy/
│   │   ├── mod.rs              # Axum routers: ingress (/v1/chat/completions, /v1/embeddings) + admin (/metrics, /healthz)
│   │   ├── inspector.rs        # Zero-copy Bytes capture + simd-json metadata extraction + token est.
│   │   ├── router_engine.rs    # Rule evaluator, EWMA selector, weighted random & fallback planner
│   │   ├── executor.rs         # Pre-first-token TTFT timeout watchdog & fallback execution loop
│   │   └── ratelimit.rs        # governor GCRA RPM/TPM rate limiter + daily tenant USD budget tracker
│   ├── providers/
│   │   ├── mod.rs              # LlmProvider trait, SseByteStream type alias, HTTP/2 client builder
│   │   ├── openai.rs           # Zero-copy pass-through adapter for OpenAI, vLLM, and Ollama
│   │   ├── anthropic.rs        # Request translator + SSE state-machine normalizer for /v1/messages
│   │   ├── gemini.rs           # Google Gemini streaming adapter
│   │   └── bedrock.rs          # AWS Bedrock ConverseStream adapter
│   └── telemetry/
│       ├── mod.rs              # tracing subscriber, OpenTelemetry OTLP setup, PII redaction filter
│       └── metrics.rs          # Prometheus metrics (/metrics): TTFT, ITL, tokens, cost, fallbacks
└── tests/
    ├── mock_upstreams.rs       # Deterministic in-process Axum SSE mock server with fault injection
    ├── failover_tests.rs       # End-to-end tests for 429/5xx retries, TTFT stalls, and circuit breakers
    └── config_reload_tests.rs  # Concurrent SSE streaming + atomic ArcSwap hot-reload verification
```

---

## 4. Non-Negotiable Architectural Invariants

1. **Zero Panics on the Request Hot Path:**
   - Never use `.unwrap()` or `.expect()` inside `src/proxy/`, `src/providers/`, or `src/telemetry/`.
   - Propagate all failures via `Result<T, RouterError>` and convert unhandled errors into the normalized OpenAI JSON error format (`src/error.rs`).

2. **Wait-Free Configuration Reads (`ArcSwap`):**
   - Never wrap `RouterConfig` in a `std::sync::RwLock` or `tokio::sync::RwLock`.
   - Store configuration in `arc_swap::ArcSwap<RouterConfig>`. Every incoming request must acquire a guard or `Arc` via `state.config.load_full()` in ~2ns. In-flight SSE streams retain their cloned `Arc<RouterConfig>` until completion so hot reloads never drop active streams.

3. **Zero-Copy Streaming & Pre-First-Token Watchdog:**
   - Never buffer an entire streaming response in memory before sending bytes to the client.
   - However, **do not commit `HTTP 200 OK` downstream headers to the client until the upstream provider has both returned HTTP 200 AND yielded its first non-empty content token chunk** within `ttft_timeout_ms`.
   - If an upstream stalls after sending HTTP 200 headers (e.g., GPU queue saturation sending only `: ping` comments) and exceeds `ttft_timeout_ms`, abort the upstream stream and transparently fail over to the next candidate in `fallbacks`.

4. **Lock-Free Health & EWMA Telemetry:**
   - All per-upstream metrics (`ewma_ttft_us`, `consecutive_failures`, `circuit_open_until_ms`) must use `std::sync::atomic` types (`AtomicU64`, `AtomicU32`) stored inside a `DashMap<String, Arc<UpstreamHealthMetrics>>`.

5. **Diagnostic Clarity (`miette` + `schemars`):**
   - All configuration structs in `src/config/mod.rs` must derive `serde::Deserialize`, `serde::Serialize`, and `schemars::JsonSchema`.
   - Configuration validation errors must return descriptive diagnostics (missing environment variables, unknown upstream references in `fallbacks`, or circular routing rules).

6. **Admin Surface Is Read-Only and Secret-Free:**
   - `AdminState` defaults to `WritePolicy::ReadOnly`; the `apply` handler returns `403` unless the surface was explicitly built with `WritePolicy::ReadWrite`.
   - Every admin response goes through `RedactedConfig` (or the `SecretStatus` rows), which masks API keys and URL userinfo by construction. Never serialize a `ProviderConfig` directly to an admin client.
   - The admin API carries only state Prometheus cannot express: configuration, health/breaker phase, tenant budgets, reload diagnostics, and the route X-ray. Counters and the inflight gauge stay on `/metrics`.
   - All reload paths (watcher, `SIGHUP`, manual apply) funnel through `AdminState::note_reload`, so `last_reload` cannot disagree with the config actually serving.

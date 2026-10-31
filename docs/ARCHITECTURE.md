# OxideRoute — Technical Architecture Specification

## 1. Reference `Cargo.toml`
Use this dependency manifest to initialize the workspace:

```toml
[package]
name = "oxide-route"
version = "0.1.0"
edition = "2021"
rust-version = "1.80"
description = "Ultra-low-latency Rust LLM Router and API Gateway"
default-run = "oxide"

[[bin]]
name = "oxide"
path = "src/main.rs"

[dependencies]
# Async Runtime & HTTP Proxy Layer
tokio = { version = "1.40", features = ["full"] }
axum = { version = "0.7", features = ["http2", "macros"] }
hyper = { version = "1.4", features = ["full"] }
tower = { version = "0.5", features = ["util", "timeout", "limit"] }
tower-http = { version = "0.6", features = ["trace", "cors", "compression-gzip"] }
http = "1.1"
http-body-util = "0.1"
bytes = "1.7"
futures-core = "0.3"
futures-util = "0.3"
async-trait = "0.1"
pin-project-lite = "0.2"

# Upstream HTTP/2 Client with Rustls
reqwest = { version = "0.12", default-features = false, features = [
    "rustls-tls",
    "http2",
    "stream",
    "json"
] }

# Serialization, SIMD JSON Inspection & Config
serde = { version = "1.0", features = ["derive"] }
serde_json = "1.0"
simd-json = "0.14"
toml = "0.8"
schemars = "0.8"
regex = "1.11"

# Lock-Free Concurrency, Caching & Rate Limiting
arc-swap = "1.7"
dashmap = "6.1"
governor = "0.6"
moka = { version = "0.12", features = ["future"] }
notify = "6.1"

# CLI, Diagnostics & Telemetry
clap = { version = "4.5", features = ["derive", "env"] }
thiserror = "1.0"
miette = { version = "7.2", features = ["fancy"] }
tracing = "0.1"
tracing-subscriber = { version = "0.3", features = ["env-filter", "json"] }
metrics = "0.23"
metrics-exporter-prometheus = "0.15"
uuid = { version = "1.10", features = ["v4", "fast-rng"] }
rand = "0.8"

[dev-dependencies]
tokio-test = "0.4"
wiremock = "0.6"
criterion = { version = "0.5", features = ["async_tokio"] }
```

---

## 2. Core Data Flow & Memory Model

```text
+-----------------------------------------------------------------------------------+
| Incoming HTTP Request (`POST /v1/chat/completions`)                               |
+-----------------------------------------+-----------------------------------------+
                                          |
                                          v
+-----------------------------------------------------------------------------------+
| `proxy::inspector::inspect_request(headers, raw_bytes: Bytes)`                    |
| - Clones a mutable scratch buffer ONLY for `simd_json::to_borrowed_value`         |
| - Extracts: `model` (&str), `stream` (bool), `user` (Option<&str>)                |
| - Estimates prompt tokens: `raw_bytes.len() / 4` (or `tiktoken-rs` optional)      |
| - Keeps `raw_bytes: Bytes` intact for zero-copy upstream forwarding               |
+-----------------------------------------+-----------------------------------------+
                                          |
                                          v
+-----------------------------------------------------------------------------------+
| `proxy::ratelimit::enforce_limits(tenant_id, estimated_tokens)`                   |
| - Checks `governor` RPM limiter + reserves `estimated_tokens` in TPM limiter      |
| - Checks atomic USD daily spend against `daily_budget_usd`                        |
+-----------------------------------------+-----------------------------------------+
                                          |
                                          v
+-----------------------------------------------------------------------------------+
| `proxy::router_engine::resolve_candidates(&RouterConfig, &ProxyRequest)`          |
| - Evaluates `[[rules]]` in order (token count, headers, regex, complexity score)  |
| - Resolves virtual model `[models."..."]` into ordered `Vec<String>` candidates:  |
|   * `Priority`: `[primary, ...fallbacks]`                                         |
|   * `WeightedRandom`: samples `targets` by weight, appends `fallbacks`            |
|   * `LatencyEwma`: orders healthy `targets` by lowest `ewma_ttft_us`              |
+-----------------------------------------+-----------------------------------------+
                                          |
                                          v
+-----------------------------------------------------------------------------------+
| `proxy::executor::dispatch_with_fallbacks(candidates, &ProxyRequest)`             |
| - Iterates through `candidates` ("provider_name/model_id")                        |
| - Skips candidates with open circuit breakers (`!health.is_healthy()`)            |
| - Wraps `provider.execute_stream()` in `tokio::time::timeout(ttft_timeout_ms)`    |
| - Awaits BOTH HTTP 200 status AND the first SSE data chunk (`first_chunk: Bytes`) |
| - On timeout or 429/5xx: records failure in circuit breaker, tries next fallback  |
| - On success: records TTFT in EWMA, chains `first_chunk` + `rest_stream`          |
+-----------------------------------------------------------------------------------+
```

---

## 3. Core Traits & Struct Signatures

### 3.1 `ProxyRequest` (`src/proxy/inspector.rs`)
```rust
use bytes::Bytes;
use http::HeaderMap;

#[derive(Debug, Clone)]
pub struct ProxyRequest {
    pub request_id: String,
    pub requested_model: String,
    pub is_stream: bool,
    pub estimated_prompt_tokens: usize,
    pub complexity_score: f32,
    pub headers: HeaderMap,
    /// Original immutable JSON payload for zero-copy forwarding to OpenAI-compatible upstreams.
    pub raw_body: Bytes,
}
```

### 3.2 `LlmProvider` Trait (`src/providers/mod.rs`)
```rust
use async_trait::async_trait;
use bytes::Bytes;
use futures_core::Stream;
use std::pin::Pin;
use crate::error::RouterError;
use crate::proxy::inspector::ProxyRequest;

pub type SseByteStream = Pin<Box<dyn Stream<Item = Result<Bytes, RouterError>> + Send + 'static>>;

#[derive(Debug, Clone)]
pub struct NonStreamResponse {
    pub status: u16,
    pub body: Bytes,
    pub prompt_tokens: usize,
    pub completion_tokens: usize,
}

#[async_trait]
pub trait LlmProvider: Send + Sync + 'static {
    fn provider_id(&self) -> &str;

    /// Dispatches a streaming request (`"stream": true`).
    /// MUST await the HTTP response headers AND pull the first non-empty SSE data frame
    /// before returning `Ok((first_chunk, remaining_stream))`.
    async fn execute_stream(
        &self,
        upstream_model: &str,
        req: &ProxyRequest,
    ) -> Result<(Bytes, SseByteStream), RouterError>;

    /// Dispatches a non-streaming request (`"stream": false`).
    async fn execute_json(
        &self,
        upstream_model: &str,
        req: &ProxyRequest,
    ) -> Result<NonStreamResponse, RouterError>;
}
```

### 3.3 Lock-Free EWMA & Circuit Breaker (`src/state.rs`)
- **EWMA Formula:**
  When an upstream yields its first token after $\Delta t$ microseconds:
  $$	ext{EWMA}_{	ext{new}} = egin{cases} \Delta t & 	ext{if } 	ext{EWMA}_{	ext{old}} = 0 \ \lfloor 0.8 \cdot 	ext{EWMA}_{	ext{old}} + 0.2 \cdot \Delta t floor & 	ext{otherwise} \end{cases}$$
- **Passive Circuit Breaker:**
  Each upstream maintains `consecutive_failures: AtomicU32` and `circuit_open_until_ms: AtomicU64`. When `consecutive_failures >= consecutive_failures_threshold` (default `5`), `circuit_open_until_ms` is set to `now_ms + cooldown_seconds * 1000`. Once `now_ms >= circuit_open_until_ms`, the circuit enters a half-open state allowing the next request to probe the upstream.

### 3.4 Upstream Connection Pool Configuration (`src/providers/mod.rs`)
Every provider adapter constructs a shared `reqwest::Client` using these exact settings:
```rust
pub fn build_upstream_http_client(timeout_ms: u64) -> reqwest::Client {
    reqwest::Client::builder()
        .use_rustls_tls()
        .http2_keep_alive_interval(std::time::Duration::from_secs(15))
        .http2_keep_alive_timeout(std::time::Duration::from_secs(5))
        .http2_keep_alive_while_idle(true)
        .http2_adaptive_window(true)
        .pool_max_idle_per_host(128)
        .tcp_nodelay(true)
        .timeout(std::time::Duration::from_millis(timeout_ms))
        .build()
        .expect("Failed to initialize rustls HTTP client")
}
```

# OxideRoute — Testing, Fault Injection & Benchmark Specifications

All integration tests must run completely offline with zero external API keys by spawning an in-process mock upstream server (`tests/mock_upstreams.rs`).

---

## 1. Deterministic In-Process Mock Upstream Server (`tests/mock_upstreams.rs`)

Build a configurable Axum test server that binds to an ephemeral localhost port (`TcpListener::bind("127.0.0.1:0")`) and simulates both OpenAI (`/v1/chat/completions`) and Anthropic (`/v1/messages`) upstreams. Use request headers or model names to trigger deterministic fault modes:

| Simulated Scenario | Trigger Condition | Mock Upstream Behavior | Expected OxideRoute Behavior |
| :--- | :--- | :--- | :--- |
| **Happy Path OpenAI Stream** | `model = "gpt-4o-fast"` | Immediately returns `HTTP 200 OK` (`text/event-stream`) and yields 3 token chunks at 5ms intervals followed by `data: [DONE]

`. | Streams chunks to client with <1.5ms internal routing overhead; updates EWMA TTFT. |
| **Upstream HTTP 429 / 503** | `model = "gpt-4o-ratelimited"` | Immediately returns `HTTP 429 Too Many Requests` with retryable JSON body. | Records failure on primary upstream, transparently retries on configured fallback (`anthropic`), and returns HTTP 200 stream to client. |
| **Pre-Header Timeout Stall** | `model = "gpt-4o-dead"` | Sleeps for 5,000ms before sending HTTP response headers. | `ttft_timeout_ms` (e.g., 200ms in test config) fires, cancels primary request, and fails over to fallback upstream. |
| **Post-Header SSE Stall (Critical)** | `model = "gpt-4o-stalled-gpu"` | Immediately flushes `HTTP 200 OK` headers and `: ping

` comments, then sleeps for 5,000ms before sending the first `data:` token chunk. | Because OxideRoute awaits the **first non-ping data chunk** before committing downstream HTTP 200 headers, the TTFT watchdog fires at 200ms and cleanly fails over to the fallback provider without corrupting the client's SSE stream. |
| **Circuit Breaker Trip** | 5 consecutive requests to `gpt-4o-ratelimited` | Returns `HTTP 503` 5 times in a row. | On the 6th request, OxideRoute skips `gpt-4o-ratelimited` immediately (0ms elapsed, reason `circuit_breaker_open`) and dispatches directly to the fallback. |

---

## 2. Required Integration Test Suite

### 2.1 `tests/failover_tests.rs`
1. **`test_transparent_429_fallback_openai_to_anthropic`**:
   - Primary (`openai`) returns 429; fallback (`anthropic`) returns valid Anthropic SSE events (`message_start`, `content_block_delta`, `message_delta`, `message_stop`).
   - Assert client receives HTTP 200 and valid OpenAI-normalized `chat.completion.chunk` events containing the text from Anthropic.
2. **`test_post_header_ttft_stall_triggers_fallback`**:
   - Configure `ttft_timeout_ms = 150`.
   - Primary flushes HTTP 200 + `: ping

` then sleeps 2,000ms.
   - Assert failover to secondary completes in under 300ms total and client receives a clean OpenAI SSE stream.
3. **`test_circuit_breaker_ejection_and_recovery`**:
   - Configure `consecutive_failures_threshold = 3` and `cooldown_seconds = 1`.
   - Send 3 failing requests -> verify 4th request bypasses primary immediately -> sleep 1.1s -> verify 5th request probes primary again.
4. **`test_ewma_latency_routing_shifts_traffic`**:
   - Configure a virtual model with `strategy = "latency_ewma"` across `fast_upstream` (10ms TTFT) and `slow_upstream` (120ms TTFT).
   - After warmup requests, assert `router_engine` selects `fast_upstream` as the primary candidate.
5. **`test_tenant_rpm_and_tpm_rate_limiting`**:
   - Configure `rpm = 5` and `tpm = 100` on header `X-Tenant-ID`.
   - Assert the 6th request within a minute or a single request with >100 estimated tokens receives HTTP 429 with `type = "rate_limit_error"`.

### 2.2 `tests/config_reload_tests.rs`
1. **`test_atomic_hot_reload_during_active_sse_stream`**:
   - Start an SSE stream that yields 10 chunks over 500ms.
   - At 100ms (while stream is mid-flight), modify the config on disk and trigger `AppState` reload via `ArcSwap::store`.
   - Assert the in-flight stream finishes all 10 chunks without dropping, while a new request dispatched at 150ms immediately uses the newly added route.
2. **`test_invalid_config_reload_is_rejected_safely`**:
   - Attempt to reload a malformed TOML file; assert `reload` returns an error and the existing `ArcSwap<RouterConfig>` remains active.

---

## 3. Microbenchmark Targets (`benches/`)
If running `cargo bench`, verify the following latency budgets on the hot path:
- **`inspect_request` (`simd-json` metadata + token estimation on 4 KB JSON payload):** `< 80 µs`
- **`resolve_candidates` (Rule matching + EWMA lookup + `ArcSwap::load`):** `< 5 µs`
- **Full in-process routing overhead (Ingress -> Mock SSE First Chunk Egress on localhost):** `< 1.5 ms` at p99 under 1,000 concurrent connections.

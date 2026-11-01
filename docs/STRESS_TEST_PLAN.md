# OxideRoute — Stress Test Plan

**Status: plan only.** No campaign has been run against this build; there are no
capacity numbers yet. `cargo bench` covers two microbenchmarks, and the test
suite covers *correctness* under deterministic faults — neither tells you what
happens at 2,000 concurrent streams.

This plan covers load-dependent behaviour only. Deterministic semantics
(failover, breaker recovery, atomic reload) are already asserted by
`tests/failover_tests.rs` and `tests/config_reload_tests.rs`; here we ask whether
they still hold when thousands of requests are in flight.

---

## 1. Objectives

Prove, under sustained, peak, and degraded load, that OxideRoute:

1. produces **no router-attributable errors** (no panics, no 5xx that isn't an
   upstream's fault),
2. keeps **added latency** inside the documented budgets,
3. keeps **failover, circuit breaking, and rate limiting correct under
   concurrency** — not merely under sequential unit tests,
4. survives **config reloads while streams are active**,
5. keeps **resource usage bounded** (memory, file descriptors, sockets,
   connection pool), and
6. **degrades cleanly** past capacity instead of collapsing.

## 2. System under test

Reference topology: [`flow.png`](flow.png) / [`flow.excalidraw`](flow.excalidraw).

```
loadgen ──► oxide (ingress) ──► mock upstream ──► back
   │
   └──────► mock upstream (control path, no router)
```

The **control path** is mandatory. Every latency number is reported as a delta
against the same load hitting the mock upstream directly; absolute numbers from a
loopback test are meaningless on their own.

Components under observation: axum ingress, `inspect_request` (simd-json copy),
`ArcSwap` snapshot, `router_engine`, `executor` (TTFT watchdog + breaker),
`ratelimit` (GCRA + token bucket + budget), provider adapters, `watcher`.

## 3. Prerequisites

### 3.1 Instrumentation

`/metrics` exposes eight families. The first four are the original set; the
rest were added specifically so the pass criteria below are observable:

| Metric | Type | Answers |
|---|---|---|
| `oxide_ttft_seconds` | histogram | how fast did first tokens arrive |
| `oxide_requests_total` | counter | per-upstream outcomes |
| `oxide_fallback_triggered_total` | counter | failover rate and reason |
| `oxide_tokens_total` | counter | prompt/completion volume per tenant |
| `oxide_rate_limited_total{tenant,reason}` | counter | **did we throttle?** (`rpm`/`tpm`/`budget`) — ST-13/14/15/16/17 |
| `oxide_inflight_requests` | gauge | saturation; Little's-law check — ST-02/03 |
| `oxide_reload_total{result,source}` | counter | reload success/rejection under churn — ST-18/19/20 |
| `oxide_breaker_open{upstream}` | gauge | which upstreams are ejected — ST-11/12 |

Still missing, and still needed for the scenarios that name them:

| Needed metric | Why |
|---|---|
| `oxide_body_rejected_total{reason}` | oversized-body path (ST-08) |
| `oxide_pool_idle_connections` (gauge) | connection-pool saturation (ST-03) |

Two measurement caveats that will otherwise produce wrong conclusions:

- `oxide_inflight_requests` counts a stream until its **last frame**, not until
  the handler returns, so a long SSE response is correctly counted as in flight.
- `oxide_breaker_open` is sampled from the health table **at scrape time**. A
  scrape interval longer than `cooldown_seconds` can therefore miss a short
  ejection; record the scrape interval alongside the number.

### 3.2 Harness — does not exist yet

Two new `[[bin]]` targets under `tools/`, deliberately **not** part of the
shipped `oxide` binary:

**`tools/loadgen`** (Rust, tokio + reqwest — no new deps)
- Open-loop mode: hold a target RPS with a token-bucket pacer (measures what
  happens *to* the server, not what a closed loop can sustain).
- Closed-loop mode: N virtual users, one request at a time.
- Per request: `model`, `stream`, payload size, `X-Tenant-ID` chosen from a mix
  profile; records status, total latency, **TTFT (first `data:` frame)**, bytes,
  and whether `[DONE]` arrived.
- Emits JSONL per request + a summary (p50/p90/p99/p999, status histogram, RPS).
- Must never be the bottleneck: verify by running it against the control path and
  confirming RPS scales with concurrency.

**`tools/mock-upstream`** — a standalone server (the `tests/` mock is compiled
into test binaries and cannot be reused):

| Mode | Behaviour |
|---|---|
| `ok` | immediate 200, N chunks at interval |
| `fast` | zero-delay body, for capacity ceilings |
| `slow-ttft` | configurable delay before first chunk |
| `stall-after-headers` | 200 + `: ping`, then silence (TTFT watchdog) |
| `dead` | delay before headers |
| `429` / `5xx` | immediate error status |
| `flaky` | percentage-based mix, deterministic by request index |
| `rag` | 10 chunks over 500 ms (in-flight stream during reload) |

Must sustain the target RPS without being the limiting factor — pin it to its
own cores and confirm its CPU headroom during the run.

### 3.3 Environment

| Item | Requirement |
|---|---|
| Host | dedicated, ≥ 8 physical cores, loopback only, no shared CI runner |
| Pinning | `taskset`: router 0–3, mock 4–5, loadgen 6–7 |
| Limits | `ulimit -n 65536` |
| Kernel | `net.core.somaxconn=4096`, `net.ipv4.tcp_tw_reuse=1` (churn tests) |
| Governor | `performance`; record it |
| Record | commit SHA, rustc version, kernel, CPU model, governor, run duration |

Repeat every scenario **3 times**; report the median. Discard the first 60 s
(warm-up: JIT-free but pools, allocators, and page cache still settle).

## 4. Workload model

| Payload | Approx. tokens | Exercises |
|---|---|---|
| 512 B | ~128 | steady-state throughput |
| 4 KB | ~1 000 | representative chat |
| 128 KB | ~32 000 | crosses the `min_estimated_tokens` rule threshold |
| 1.9 MB | ~480 000 | near the 2 MiB body ceiling |

Default mix: 70 % small non-stream · 20 % small stream · 8 % 4 KB stream ·
2 % 128 KB stream.

Model shapes per scenario: single target; two-target `latency_ewma`;
three-deep fallback chain.

## 5. Service-level objectives

| SLO | Measurement | Target |
|---|---|---|
| Added routing latency | loadgen p99 − control p99 (512 B, non-stream) | **< 2 ms** |
| TTFT overhead | loadgen p99 TTFT − control p99 | **< 5 ms** |
| Router-attributable errors | 5xx not explained by upstream status | **0** |
| Throughput stability | RPS coefficient of variation, steady state | **< 5 %** |
| Memory | RSS growth over a 1 h soak | **< 10 %** |
| Descriptor stability | open FDs at steady state | **no monotonic growth** |
| Failover fidelity | observed fallback ratio vs injected fault ratio | **within 1 pp** |
| Rate-limit exactness | admitted count vs `rpm × minutes` | **≤ +burst, never more** |
| Reload safety | streams dropped during reload | **0** |
| Panics | process aborts, `unwrap` panics, panic=abort | **0** |

Documented budgets from `TESTING_AND_BENCHMARKS.md` (routing overhead
< 1.5 ms p99 at 1 000 concurrent) are the reference for ST-05.

---

## 6. Test matrix

### Phase A — Baseline & capacity

| ID | Intent | Setup | Load | Duration | Pass |
|---|---|---|---|---|---|
| ST-01 | Control baseline | loadgen → mock, `fast` | closed-loop 1→1000 VU | 10 min | no errors; RPS scales linearly with VU |
| ST-02 | Non-streaming ceiling | router → mock `fast`, 512 B | closed-loop ramp 100→5000 | 15 min | find max RPS where p99 < 50 ms and errors < 0.1 % |
| ST-03 | Streaming ceiling | router → mock `ok` (3 chunks) | closed-loop ramp 100→5000 | 15 min | same, with `oxide_inflight_requests` < capacity of the pool (128/host) |

Capacity ceiling = highest RPS sustained for ≥ 5 min inside SLO.

### Phase B — Latency & overhead

| ID | Intent | Setup | Load | Pass |
|---|---|---|---|---|
| ST-04 | Overhead vs payload size | 512 B / 4 KB / 128 KB / 1.9 MB | open-loop at 50 % of ST-02 capacity | p99 delta within SLO; confirm `inspect_request` cost grows with body size |
| ST-05 | TTFT overhead | streaming mix | open-loop at 50 % capacity | TTFT p99 delta < 5 ms; ≥ 1 000 concurrent streams |

### Phase C — Overload & degradation

| ID | Intent | Setup | Pass |
|---|---|---|---|
| ST-06 | Collapse or shed? | 2× ST-02 capacity | no panic; error rate rises via `429`/timeout, **not** connection resets; recovery to SLO within 60 s of load drop |
| ST-07 | Connection churn | `Connection: close`, 5 000 conn/s, 60 s | no FD leak; `ESTABLISHED` returns to baseline after |
| ST-08 | Body ceiling | 2.0 MB and 2.1 MB payloads | 2.0 MB accepted; 2.1 MB → `413`. **Record the response shape** — axum's default rejection is not the OpenAI envelope (see §9.4) |
| ST-09 | Header flood | 100 headers × 8 KB | no crash; request either rejected or served within 2× latency |

### Phase D — Failover under load

| ID | Intent | Setup | Pass |
|---|---|---|---|
| ST-10 | Partial upstream failure | mock `flaky` 10 % → fallback to healthy | success rate ≥ 99.9 %; fallback counter within 1 pp of injected rate; no user-visible errors |
| ST-11 | Post-header stall burst | 30 % of primaries `stall-after-headers`, `ttft_timeout_ms=150` | streams still succeed via fallback; TTFT p99 < 300 ms; no partial SSE frames emitted (verify clients always see `[DONE]` or a clean error) |
| ST-12 | Total outage | all targets `5xx` for 60 s, then recover | all requests → `502` with `all_upstreams_failed`; breaker opens; **auto-recovery** within cooldown + 1 s; no leaked tasks (RSS flat) |
| ST-13 | EWMA shift under load | two targets, one `slow-ttft` 120 ms | traffic shifts to the fast target within 20 requests; `oxide_ttft_seconds` per-upstream reflects the split |

### Phase E — Rate limiting under concurrency

| ID | Intent | Setup | Pass |
|---|---|---|---|
| ST-14 | RPM exactness | 50 tenants, `rpm=100`, burst 1.0, 60 s at 2× aggregate | per-tenant admitted ≤ 100 ± burst; **no** over-admission under concurrency (GCRA is not a naive counter — this is the test that proves it) |
| ST-15 | TPM exactness | `tpm=250000`, mixed payloads, high concurrency | consumed tokens ≤ tpm + burst capacity; the CAS loop in `TokenBucket::try_consume` must not lose updates (a lost update shows as over-admission) |
| ST-16 | Budget enforcement | `[pricing]` set, small `daily_budget_usd` | once exhausted, requests → `429`; spend counter matches `tokens × price` within rounding |
| ST-17 | Tenant-header rotation | client rotates `X-Tenant-ID` per request | **expected finding:** each new value gets a fresh bucket, so limits are evaded and the tenant maps grow. Confirm, quantify, then mitigate (§9.3) |

### Phase F — Hot reload

| ID | Intent | Setup | Pass |
|---|---|---|---|
| ST-18 | Reload during streams | `rag` mock (500 ms streams), rewrite config every 100 ms for 5 min | **0** dropped streams; every response ends with `[DONE]`; reload counter increments; no panic |
| ST-19 | Invalid config churn | alternate valid/broken config every 100 ms | active config never changes on the broken ones; warning per rejection; throughput unaffected beyond < 1 % |
| ST-20 | Provider-set change | reload that adds and removes a provider under load | no request 500s because a provider vanished mid-flight (a request that read the old snapshot must still resolve) |

### Phase G — Soak

| ID | Intent | Setup | Pass |
|---|---|---|---|
| ST-21 | 1 h steady (24 h nightly) | 50 % capacity, periodic ST-10/ST-18 faults | RSS growth < 10 %; FDs flat; TTFT EWMA stable (no drift); zero panics; error rate < 0.1 % |
| ST-22 | Cardinality soak | 100 000 distinct `X-Tenant-ID` over 10 min | measure RSS growth per 10 k tenants. **Expected finding:** unbounded (§9.3) |

### Phase H — Lifecycle

| ID | Intent | Setup | Pass |
|---|---|---|---|
| ST-23 | SIGTERM with in-flight streams | 200 active `rag` streams, send `SIGTERM` | all in-flight streams complete; process exits within `graceful_shutdown_timeout_ms`. **Expected finding:** the drain deadline is logged but not enforced (§9.2) |
| ST-24 | Restart / port reuse | kill -9 after load, restart immediately | binds without `EADDRINUSE`; no stuck `TIME_WAIT` accumulation |

### Phase I — Observability overhead

| ID | Intent | Setup | Pass |
|---|---|---|---|
| ST-25 | Scrape under load | `/metrics` scrape every 1 s at 50 % capacity | router overhead delta < 0.2 ms; counter monotonicity holds |
| ST-26 | Exporter down | `otlp_endpoint` set to a black hole, full load | no unbounded memory growth, no backpressure stall in the request path |
| ST-27 | Redaction cost | log level `info`, 10 k req/s | PII regex pass adds < 50 µs/line; confirm no O(n²) behaviour on long lines |

---

## 7. Fault injection catalogue

All faults are triggered by model name or a query header so a single mock binary
serves every scenario:

| Fault | Trigger | Used by |
|---|---|---|
| fast 200 | `model=fast` | ST-01/02 |
| chunked SSE | `model=rag` | ST-03/05/18/23 |
| pre-header stall | `model=dead` | ST-11 (variant) |
| post-header stall | `model=stalled` | ST-11 |
| slow TTFT | `model=slow` | ST-13 |
| 429 | `model=ratelimited` | ST-10/12 |
| 5xx | `model=broken` | ST-12 |
| probabilistic failure | `header: X-Fault-Rate: 10` | ST-10 |
| close mid-stream | `model=truncated` | ST-11 (partial-frame check) |

---

## 8. Procedure

1. **Instrument first** (§3.1) and build the harness (§3.2).
2. **ST-01** — validate the harness itself against the control path.
3. **Phase A** — establish capacity; every later scenario runs at a stated
   fraction of it, so capacity must be re-measured after any perf-relevant change.
4. **Phases B–E** — one scenario at a time, 3 repetitions, 60 s warm-up, 60 s
   cool-down.
5. **Phases F–H** — these mutate state; run them last and restart the process
   between scenarios.
6. **Phase I** — measure the observer effect only after the rest is stable.

For every run capture, in `results/<timestamp>/<scenario>/`:
`config.toml`, loadgen JSONL + summary, `/metrics` snapshots at 5 s intervals,
router stdout/stderr, `pidstat`/RSS/FD series, `ss -s` before/after, and the
environment record from §3.3.

**Abort conditions** (stop the run, file a bug): router panic; any 5xx not
explained by an upstream status; RSS growing > 20 %/h; FD count monotonically
increasing; p99 > 5× target with CPU < 50 %.

---

## 9. Known defects and risks this plan should confirm

Items 9.1 and 9.2 were fixed after this plan was written; they are kept (and
marked) so the campaign measures the *new* behaviour rather than assuming the
old defect. 9.3–9.5 are still expected to show up as failures, based on reading
the code, and are listed so nobody mistakes them for stress-test noise.

### 9.1 `admin_listen` is never bound — **fixed**
`serve` now binds `admin_listen` as a second listener carrying `/metrics` and
`/healthz`, and the public ingress no longer serves `/metrics`. When
`admin_listen` is unset the scrape endpoint falls back to the ingress and a
warning is logged at startup. ST-25 should now confirm the split holds
end-to-end rather than assert the old defect.

### 9.2 The graceful-shutdown deadline is not enforced — **fixed**
`serve` now waits `graceful_shutdown_timeout_ms` from the moment the signal
arrives, then logs a warning and exits even if streams are still open. ST-23
should still measure the *observable* behaviour (how many streams are cut short
by the deadline) rather than whether the deadline exists.

### 9.3 Tenant identity is client-controlled
`resolve_tenant` uses the `X-Tenant-ID` *value* as the tenant key. The router
sits at the edge and does not authenticate, so:

- rotating the header **evades** RPM/TPM/budget entirely (ST-17),
- every distinct value creates a `governor` limiter, token bucket, and spend
  entry that is never evicted → unbounded memory (ST-22).

Mitigations to evaluate after measurement: cap the tenant map, evict idle
tenants, or bind limits to an authenticated identity (header set by a trusted
proxy, or an API key).

### 9.4 The 2 MiB body cap returns a non-OpenAI error
`axum`'s `Bytes` extractor enforces a 2 097 152-byte default limit; exceeding it
yields axum's `413` shape, not the documented error envelope. ST-08 records it.

### 9.5 Streaming token accounting undercounts
SSE carries no usage block, so streaming requests bill only the prompt estimate
(`record_usage(..., completion_tokens = 0)`). ST-16 must not assume streaming
completions are charged.

---

## 10. Results template

```markdown
## ST-02 — Non-streaming capacity ceiling
Commit: <sha>   Host: <cpu>, <n> cores   Governor: performance

| Metric | Run 1 | Run 2 | Run 3 | Median |
|---|---|---|---|---|
| max RPS in SLO | | | | |
| p50 latency | | | | |
| p99 latency | | | | |
| error rate | | | | |
| CPU (router) | | | | |

Control baseline (ST-01) p99: ____ ms      Added latency p99: ____ ms
Verdict: PASS / FAIL — notes.
```

## 11. Exit criteria

A release is stress-tested when:

- ST-01…ST-05 pass (capacity and overhead are known and inside SLO),
- ST-06, ST-10, ST-11, ST-12, ST-18, ST-23 pass (degradation is clean and
  failover/reload are safe under load),
- ST-21 shows no growth over 1 h,
- every §9 finding is either fixed or explicitly accepted and documented.

## 12. Out of scope

- Upstream provider performance and model quality.
- Multi-host / cross-AZ networking (loopback only here).
- TLS handshake cost (terminate TLS in front of the router for now).
- `cargo bench` microbenchmarks — those gate *code* changes, not capacity.

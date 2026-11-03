# Admin API — the contract shared by the TUI and the SPA

Status: **types implemented** in [`src/admin/mod.rs`](../src/admin/mod.rs); HTTP
wiring is the next step. This document is the contract both clients code against,
so the `oxide top` TUI and the embedded SPA cannot diverge.

## The one design rule

> The admin API carries only what Prometheus cannot express.

`/metrics` already exposes eight families (`oxide_ttft_seconds`,
`oxide_requests_total`, `oxide_fallback_triggered_total`, `oxide_tokens_total`,
`oxide_rate_limited_total`, `oxide_inflight_requests`, `oxide_reload_total`,
`oxide_breaker_open`). Mirroring them into JSON duplicates a registry and creates
two truths. Clients **fuse** the two surfaces:

| Question | Source |
|---|---|
| Rates, p99 TTFT, rejection split, inflight | `GET /metrics` (Prometheus text) |
| Which config is live, and what is in it | `GET /admin/config` |
| EWMA vs watchdog, breaker countdown, orphans | `GET /admin/health` |
| Daily budget left per tenant | `GET /admin/snapshot` → `tenants` |
| "Why would this request go there?" | `POST /admin/explain` |
| "Is this TOML safe to apply?" | `POST /admin/config/validate` |

The one intentional overlap is `oxide_breaker_open`: the health view computes it
from the same table, because a client needs phase *and* countdown in one payload.

## Endpoints

| Method | Path | Returns | Mutates |
|---|---|---|---|
| `GET` | `/admin/snapshot` | [`AdminSnapshot`](#adminsnapshot) | no |
| `GET` | `/admin/config` | `RedactedConfig` + generation | no |
| `GET` | `/admin/health` | `HealthSnapshot` | no |
| `POST` | `/admin/explain` | `RouteExplain` | no (runs the real resolver) |
| `POST` | `/admin/config/validate` | `ValidationReport` (`200` always) | no |
| `POST` | `/admin/config/apply` | `ReloadStatus` (`200` applied, `422` rejected, `403` read-only) | **yes** |

Path constants live in `src/admin/mod.rs` (`CONFIG_PATH`, `HEALTH_PATH`, …) so
the Rust TUI and the SPA's generated client import the same strings.

## `AdminSnapshot`

One request, one first paint. This is what the TUI calls on a timer and the SPA
uses for hydration; the focused endpoints exist so either can refresh one panel
without refetching the config.

```json
{
  "generated_at_ms": 1762192800123,
  "uptime_ms": 3920412,
  "config_generation": 7,
  "listen": "0.0.0.0:8080",
  "admin_listen": "127.0.0.1:9090",
  "last_reload": {
    "generation": 7,
    "source": "manual",
    "outcome": "applied",
    "at_ms": 1762192711000
  },
  "config": {
    "listen": "0.0.0.0:8080",
    "providers": {
      "openai_us": {
        "type": "openai",
        "api_key": "***redacted***",
        "timeout_ms": 60000,
        "ttft_timeout_ms": 2500
      }
    },
    "models": { "smart-router": { "strategy": "latency_ewma", "targets": [] } },
    "rules": []
  },
  "secrets": [
    {
      "provider": "openai_us",
      "kind": "openai",
      "has_api_key": true,
      "base_url_had_credentials": false
    }
  ],
  "health": {
    "at_ms": 1762192800123,
    "open_breakers": 1,
    "orphans": 0,
    "upstreams": [
      {
        "reference": "anthropic_prod/claude-3-7-sonnet-latest",
        "provider": "anthropic_prod",
        "model": "claude-3-7-sonnet-latest",
        "configured": true,
        "ewma_ttft_us": 1900000,
        "ttft_timeout_ms": 3000,
        "ttft_headroom_pct": 63.33,
        "consecutive_failures": 0,
        "failure_threshold": 5,
        "breaker": { "state": "closed" },
        "total_requests": 1841,
        "total_failures": 3,
        "error_rate_pct": 0.16
      }
    ]
  },
  "tenants": [
    {
      "tenant": "default_tenant",
      "match_header": "X-Tenant-ID",
      "rpm": 600,
      "tpm": 250000,
      "daily_budget_usd": 50.0,
      "burst_multiplier": 1.5,
      "spent_usd": 12.5,
      "remaining_usd": 37.5,
      "budget_used_pct": 25.0
    }
  ]
}
```

### Notes clients must respect

- **`ewma_ttft_us: null` means "never observed"**, not zero. The route map draws
  those nodes hollow, not fast.
- **`ttft_headroom_pct`** is the failover-risk bar. `80%` means EWMA is at 80% of
  the provider's `ttft_timeout_ms`; a bar past ~90% predicts a fallback storm.
- **`configured: false`** marks a health row the active config no longer names.
  Show it struck through: it is an orphan of a reload, and `sync_breakers` may
  still be exporting a gauge for it.
- **Breaker phase derivation** (from `is_healthy` + `consecutive_failures` +
  `circuit_open_until_ms`):

  | Condition | `state` | `probe_in_ms` |
  |---|---|---|
  | `now < open_until` | `open` | remaining cooldown |
  | `now ≥ open_until`, failures > 0 | `half_open` | absent |
  | failures == 0 | `closed` | absent |

  `half_open` is the state that matters: the next request is a probe, so one
  unlucky request can re-eject the upstream.
- **`last_reload.outcome: "rejected"`** means the *previous* generation is still
  serving. The status panel must say that explicitly; it is the single most
  important line in the UI.

## Validation and apply

`POST /admin/config/validate` takes the raw document, not a parsed object:

```json
{ "toml": "listen = \"0.0.0.0:8080\"\n..." }
```

```json
{
  "ok": false,
  "diagnostics": [
    {
      "severity": "error",
      "code": "oxide::config::undefined_provider",
      "message": "`models.m.primary` references unknown provider `ghost` (in `ghost/gpt-4o`)",
      "help": "Define the provider in the `[providers]` table, or fix the reference.",
      "field": "models.m.primary"
    },
    {
      "severity": "error",
      "code": "oxide::config::toml",
      "message": "failed to parse configuration TOML",
      "span": { "line": 4, "column": 9, "offset": 63, "length": 11 }
    },
    {
      "severity": "warning",
      "code": "oxide::admin::unresolved_env",
      "message": "`${ANTHROPIC_API_KEY}` is not set in this process",
      "field": "ANTHROPIC_API_KEY"
    }
  ]
}
```

- **`code` is the miette code**, so the editor links to docs by code rather than
  matching prose. `span` comes from `toml::de::Error::span()` and is 1-based.
- **Validation is lenient**: unset `${VAR}` becomes a warning, so a laptop without
  production secrets can still edit and validate.
- **Apply is strict**: it runs inside the serving process, and adopting a document
  whose `${VAR}` stayed literal would install the placeholder as an API key.
  That asymmetry is deliberate; do not "fix" it by passing the mode through.

`POST /admin/config/apply` takes the same body and returns `ReloadStatus`
(`200` applied, `422` rejected with a diagnostic, `403` when the surface was
built `WritePolicy::ReadOnly`). Rejection is data, not an exception: the client
renders `status.error` next to the draft.

### Reload semantics that must be surfaced

- Generation increments only on success, so `generation` is a monotonic proof of
  which config is serving.
- `AppState::apply_config` preserves the health table **and** the tenant spend
  ledger. Reloading quotas does **not** forgive a spent daily budget and does not
  close an open breaker. The Apply confirmation should say so.
- Every reload path (watcher, `SIGHUP`, manual) must funnel through
  `AdminState::note_reload` so `last_reload` cannot disagree with reality. The
  watcher and signal handler already call `apply_config`; adding the `note_reload`
  call is the wiring step.
- Apply does **not** write the file in this version. The SPA's "Apply" should
  eventually write atomically + emit `SIGHUP`; until then the UI is explicit that
  it changed the running process only, and offers "copy TOML" for the operator to
  commit.

## The X-ray: `POST /admin/explain`

```json
{
  "model": "auto",
  "prompt": "Summarise this 40k-token incident report …",
  "headers": { "X-Data-Classification": "internal" }
}
```

```json
{
  "requested_model": "auto",
  "estimated_prompt_tokens": 41200,
  "complexity_score": 0.61,
  "matched_rule": "long-context-to-gemini",
  "retry_on_status": [429, 500, 502, 503, 504, 529],
  "candidates": [
    {
      "order": 0,
      "reference": "gemini_vertex/gemini-2.5-pro",
      "source": { "kind": "rule_target" },
      "breaker": "closed",
      "ewma_ttft_us": 1400000,
      "ttft_timeout_ms": 4000
    }
  ]
}
```

- The synthetic request goes through `inspect_request`, so token and complexity
  estimates are the *live* estimators, not a parallel implementation.
  `estimated_prompt_tokens` / `complexity_score` can be supplied to replay a real
  request's numbers exactly.
- `source.kind` is one of `primary`, `target` (with `weight`), `fallback` (with
  `index`), `rule_target`, `unlisted`. The join is local to the plan's root model
  first, because the same upstream can be a primary under one model and a target
  under another.
- `order` is authoritative; `source` is a display label only.
- No upstream is contacted, so this is safe to run on every keystroke in the
  editor — debounce it, but it has no side effects.

## Security posture

- Default `WritePolicy::ReadOnly`; `apply` returns `403`.
- Bind the admin listener on `127.0.0.1` (the schema default posture is a separate
  `admin_listen`), never on the public ingress. `build_router` already excludes
  `/metrics` for this reason; the admin routes belong to `build_admin_router`.
- Secrets are masked **by construction** in `RedactedConfig`, including
  `user:pass@` in `base_url`. Tests assert the serialized snapshot never contains
  a key.
- The SPA must send a custom header (e.g. `X-Oxide-Admin: 1`) on mutating calls so
  a cross-origin form post cannot trigger `apply`; same-origin-only CORS is not
  enough on its own.
- No payload capture here. `log_payloads` stays off, and the X-ray never calls an
  upstream, so the admin surface cannot leak prompt content by accident.

## Not yet expressible (ranked)

1. **Env-var provenance.** After interpolation we only know a key was set, not
   that it came from `${OPENAI_API_KEY}`. The UI's "$VAR chip with resolved /
   missing dot" needs `load_config_*` to return a `BTreeMap<FieldPath, EnvSource>`
   alongside the config. Highest-value next change; it is what makes the editor
   honest on a laptop.
2. **Per-rule match traces.** `matched_rule_name` returns only the winner. A full
   X-ray wants `Vec<RuleOutcome { name, matched, failed_predicate }>`. Refactor
   `matching_rule` to return outcomes and let `matched_rule_name` take the first
   match — a contained change in `router_engine.rs`, worth doing when the X-ray
   gets its editor panel.
3. **Live RPM/TPM consumption.** `governor` cells cannot be read without
   consuming one. A `TenantRateLimiter::snapshot()` that reads cell state
   read-only is required before the tenant gauges can show "burn" rather than
   just "configured" + rejection rate from `/metrics`.
4. **Live push.** Polling `/admin/snapshot` at 1s is fine for the TUI. The SPA
   wants `GET /admin/stream` (SSE) emitting `snapshot` every second and
   `reload` events immediately. `ArcSwap` + `DashMap` make this cheap: snapshot
   assembly is a handful of relaxed loads.
5. **`inflight` in JSON.** Already a gauge on `/metrics`; do not duplicate it.

## Wiring plan (next commit)

1. Add an `AdminState` layer to `build_admin_router` via `Extension` (the handler
   state stays `Arc<AppState>`), and a small set of handlers in
   `src/admin/http.rs`.
2. `main.rs::serve` constructs `AdminState::new(Arc::clone(&state), &path)` and
   passes it to `build_admin_router`; `WritePolicy::ReadWrite` only when a new
   `ui.allow_writes = true` key is present (add to `RouterConfig` with
   `#[serde(default)]`, plus a schema field).
3. Watcher and `SIGHUP` call `note_reload` on the same `AdminState`.
4. The TUI (`oxide top`) is the first client: `GET /admin/snapshot` every second,
   `POST /admin/explain` on demand. The SPA reuses the same JSON; no server change
   is needed for it.
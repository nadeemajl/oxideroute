//! Read-only admin surface shared by the TUI and the SPA.
//!
//! # Design rule
//!
//! **The admin API carries only what Prometheus cannot express.** Counters,
//! histograms, and the inflight gauge already live on the scrape endpoint; a
//! JSON mirror of them would drift. What is *not* on `/metrics` — the redacted
//! configuration, the per-upstream health table with its breaker countdown, the
//! tenant budget rows, the last reload's diagnostic — is what this module
//! answers. Clients fuse both: scrape for rates, call admin for state.
//!
//! # Secret handling
//!
//! [`RedactedConfig`] is built from a clone of the live [`RouterConfig`], with
//! every provider key replaced by [`REDACTED`] and any URL userinfo stripped.
//! The redaction is applied by construction, not by `skip_serializing`: a field
//! added to [`crate::config::ProviderConfig`] in the future still serializes,
//! so the UI cannot silently lose visibility, while a key pasted directly into
//! the file is masked just like an interpolated one.
//!
//! Environment-variable *names* are not recoverable here — [`load_config_from_str`]
//! interpolates before parsing. Showing `$OPENAI_API_KEY` chips instead of
//! `***` requires the loader to hand back a provenance map; see
//! `docs/ADMIN_API.md` § "Not yet expressible".
//!
//! # Reload semantics the UI must not hide
//!
//! [`crate::state::AppState::apply_config`] preserves the health table and the
//! per-tenant spend ledger: reloading quotas does **not** forgive a spent daily
//! budget, and does not close an open breaker. [`ReloadStatus`] reports the
//! generation so a client can prove which config is actually serving, and a
//! rejected reload keeps the previous one live — both are surfaced rather than
//! swallowed.

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use arc_swap::ArcSwap;
use bytes::Bytes;
use http::{HeaderMap, HeaderName, HeaderValue};
use miette::Diagnostic as _;
use serde::{Deserialize, Serialize};

use crate::config::loader::{load_config_from_str, InterpolationMode};
use crate::config::{ProviderKind, RouterConfig};
use crate::error::{ConfigError, RouterError};
use crate::proxy::inspector::inspect_request;
use crate::proxy::router_engine::{matched_rule_name, resolve_candidates};
use crate::state::{now_ms, AppState};

/// Path serving the redacted configuration.
pub const CONFIG_PATH: &str = "/admin/config";
/// Path serving the health table.
pub const HEALTH_PATH: &str = "/admin/health";
/// Path serving a single fused snapshot (the TUI's first paint).
pub const SNAPSHOT_PATH: &str = "/admin/snapshot";
/// Path validating a candidate TOML document.
pub const VALIDATE_PATH: &str = "/admin/config/validate";
/// Path explaining a routing decision without calling an upstream.
pub const EXPLAIN_PATH: &str = "/admin/explain";
/// Path applying a candidate TOML document.
pub const APPLY_PATH: &str = "/admin/config/apply";
/// Placeholder substituted for every secret in admin responses.
pub const REDACTED: &str = "***redacted***";

/// Where a configuration change came from.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ReloadSource {
    /// The file watcher noticed the on-disk document change.
    File,
    /// `SIGHUP`, or a checkpoint on startup.
    Signal,
    /// An operator pressed Apply in the UI.
    Manual,
}

/// Whether the last reload was adopted or refused.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ReloadOutcome {
    /// The new document is live.
    Applied,
    /// The document was refused and the previous generation still serves.
    Rejected,
}

/// The last reload attempt, as every UI wants to show it.
#[derive(Debug, Clone, Serialize)]
pub struct ReloadStatus {
    /// Monotonic config generation; `1` is the boot document.
    pub generation: u64,
    /// Which trigger produced this attempt.
    pub source: ReloadSource,
    /// Whether it was adopted.
    pub outcome: ReloadOutcome,
    /// Wall-clock milliseconds when it completed.
    pub at_ms: u64,
    /// Why it was refused, when it was.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<Diagnostic>,
}

impl ReloadStatus {
    /// The boot state: generation 1, no reload attempted yet.
    pub fn boot() -> Self {
        Self {
            generation: 1,
            source: ReloadSource::File,
            outcome: ReloadOutcome::Applied,
            at_ms: now_ms(),
            error: None,
        }
    }
}

/// Severity of one validation diagnostic.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Severity {
    /// The document will not load.
    Error,
    /// The document loads, but the operator probably did not mean it.
    Warning,
}

/// A 1-based source position, derived from the byte span `toml` reports.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct SourceSpan {
    /// 1-based line.
    pub line: usize,
    /// 1-based column.
    pub column: usize,
    /// Byte offset into the document.
    pub offset: usize,
    /// Span length in bytes.
    pub length: usize,
}

impl SourceSpan {
    fn from_offset(raw: &str, offset: usize, length: usize) -> Self {
        let offset = offset.min(raw.len());
        let prefix = &raw[..offset];
        let line = prefix.bytes().filter(|byte| *byte == b'\n').count() + 1;
        let column = prefix
            .rsplit('\n')
            .next()
            .map_or(1, |tail| tail.chars().count() + 1);

        Self {
            line,
            column,
            offset,
            length,
        }
    }
}

/// One machine-readable config diagnostic.
///
/// `code` is the `miette` code (`oxide::config::undefined_provider`), so the UI
/// can link straight into the docs without string-matching `message`.
#[derive(Debug, Clone, Serialize)]
pub struct Diagnostic {
    /// Whether this blocks the load.
    pub severity: Severity,
    /// Stable diagnostic code.
    pub code: String,
    /// Human-readable message.
    pub message: String,
    /// Optional remediation hint.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub help: Option<String>,
    /// Dotted field path or cycle hinge, e.g. `models.smart-router.fallbacks[0]`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub field: Option<String>,
    /// Source position, when the failure was a TOML decode.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub span: Option<SourceSpan>,
    /// Cycle members, when the failure was `circular_routing`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub chain: Option<Vec<String>>,
}

impl Diagnostic {
    /// Converts a [`ConfigError`] into the wire shape.
    ///
    /// `raw` is the document the error came from; it is only used to turn a byte
    /// offset into a line/column pair.
    pub fn from_config_error(error: &ConfigError, raw: &str) -> Self {
        let code = error
            .code()
            .map_or_else(|| "oxide::config".to_string(), |code| code.to_string());
        let help = error.help().map(|help| help.to_string());

        let (field, span, chain) = match error {
            ConfigError::Toml { source } => (
                None,
                source
                    .span()
                    .map(|range| SourceSpan::from_offset(raw, range.start, range.len())),
                None,
            ),
            ConfigError::InvalidValue { field, .. } => (Some(field.clone()), None, None),
            ConfigError::UndefinedProvider { context, .. }
            | ConfigError::UndefinedModel { context, .. } => (Some(context.clone()), None, None),
            ConfigError::CircularRouting { chain } => (None, None, Some(chain.nodes().to_vec())),
            ConfigError::DuplicateRule { name } => (Some(name.clone()), None, None),
            ConfigError::MissingEnvVar { var } => (Some(var.clone()), None, None),
            ConfigError::Io { path, .. } => (Some(path.clone()), None, None),
            ConfigError::Interpolation { .. } => (None, None, None),
        };

        Self {
            severity: Severity::Error,
            code,
            message: error.to_string(),
            help,
            field,
            span,
            chain,
        }
    }
}

/// Result of validating a candidate document.
#[derive(Debug, Clone, Serialize)]
pub struct ValidationReport {
    /// True when nothing of [`Severity::Error`] is present.
    pub ok: bool,
    /// Errors first, then warnings, in discovery order.
    pub diagnostics: Vec<Diagnostic>,
}

/// Whether the admin surface may mutate the running configuration.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WritePolicy {
    /// Inspect only. This is the default, and the only correct default.
    ReadOnly,
    /// `POST /admin/config/apply` is permitted.
    ReadWrite,
}

impl WritePolicy {
    /// Whether writes are allowed.
    pub const fn permits_write(self) -> bool {
        matches!(self, Self::ReadWrite)
    }
}

/// A refusal from the admin surface itself (not from the config document).
#[derive(Debug, thiserror::Error)]
pub enum AdminError {
    /// The surface was built read-only.
    #[error("admin surface is read-only; rebuild with `WritePolicy::ReadWrite` to apply")]
    ReadOnly,
    /// The document could not be loaded.
    #[error(transparent)]
    Config(#[from] ConfigError),
}

/// One provider's secret posture, with no secret in it.
#[derive(Debug, Clone, Serialize)]
pub struct SecretStatus {
    /// Provider config key.
    pub provider: String,
    /// Wire protocol.
    pub kind: ProviderKind,
    /// Whether a non-empty key is configured for this process.
    pub has_api_key: bool,
    /// Whether `base_url` carried `user:pass@` that was stripped.
    pub base_url_had_credentials: bool,
}

/// The configuration document with every secret replaced by [`REDACTED`].
///
/// Serializes transparently as a [`RouterConfig`] object.
#[derive(Debug, Clone, Serialize)]
#[serde(transparent)]
pub struct RedactedConfig(pub RouterConfig);

impl RedactedConfig {
    /// Clones `config` and masks every secret-bearing field.
    pub fn from_config(config: &RouterConfig) -> Self {
        let mut masked = config.clone();

        for provider in masked.providers.values_mut() {
            if provider.api_key().is_some() {
                provider.api_key = Some(REDACTED.to_string());
            }
            if let Some(base_url) = provider.base_url.as_deref() {
                provider.base_url = Some(mask_url_credentials(base_url));
            }
        }

        Self(masked)
    }
}

/// Strips `user:pass@` from a URL authority, leaving the host intact.
fn mask_url_credentials(url: &str) -> String {
    let Some(scheme_end) = url.find("://") else {
        return url.to_string();
    };

    let authority_start = scheme_end + 3;
    let authority_end = url[authority_start..]
        .find('/')
        .map_or(url.len(), |offset| authority_start + offset);
    let authority = &url[authority_start..authority_end];

    match authority.rfind('@') {
        Some(at) => format!(
            "{}***@{}{}",
            &url[..authority_start],
            &authority[at + 1..],
            &url[authority_end..]
        ),
        None => url.to_string(),
    }
}

/// Circuit-breaker phase at the instant of the snapshot.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum BreakerState {
    /// Admitting traffic normally.
    Closed,
    /// Ejected; only the cooldown timer runs.
    Open,
    /// Cooldown elapsed, failures above threshold: the next request probes.
    HalfOpen,
}

/// The breaker as the UI draws it.
#[derive(Debug, Clone, Copy, Serialize)]
pub struct BreakerView {
    /// Current phase.
    pub state: BreakerState,
    /// Epoch milliseconds the breaker may be probed, when open.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub open_until_ms: Option<u64>,
    /// Countdown to the probe, when open.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub probe_in_ms: Option<u64>,
}

/// One upstream's live health, joined against the configuration.
#[derive(Debug, Clone, Serialize)]
pub struct UpstreamHealth {
    /// `provider/model`, the health-table key.
    pub reference: String,
    /// Provider config key.
    pub provider: String,
    /// Upstream model id.
    pub model: String,
    /// False when the table holds a reference the active config no longer names
    /// (a survivor of a reload); the UI shows these struck through.
    pub configured: bool,
    /// EWMA TTFT; `None` means "never observed", not "zero latency".
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ewma_ttft_us: Option<u64>,
    /// This provider's watchdog deadline, the headroom denominator.
    pub ttft_timeout_ms: u64,
    /// `ewma_ttft_us / ttft_timeout_ms` as a percentage; the failover-risk bar.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ttft_headroom_pct: Option<f64>,
    /// Failures since the last success.
    pub consecutive_failures: u32,
    /// The configured trip threshold.
    pub failure_threshold: u32,
    /// Breaker phase and countdown.
    pub breaker: BreakerView,
    /// Requests routed here.
    pub total_requests: u64,
    /// Requests that failed here.
    pub total_failures: u64,
    /// `total_failures / total_requests`, when any traffic was seen.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error_rate_pct: Option<f64>,
}

/// Aggregate health view for the route map.
#[derive(Debug, Clone, Serialize)]
pub struct HealthSnapshot {
    /// Wall-clock milliseconds the snapshot was taken.
    pub at_ms: u64,
    /// Sorted by reference for stable rendering and diffing.
    pub upstreams: Vec<UpstreamHealth>,
    /// How many breakers are open right now.
    pub open_breakers: usize,
    /// How many health rows no longer belong to the config.
    pub orphans: usize,
}

/// One tenant's configured limits plus realised spend.
///
/// Live RPM/TPM *consumption* is deliberately absent: `governor` cells are not
/// readable without consuming them. Exposing it needs a read-only peek on
/// [`crate::proxy::ratelimit::TenantRateLimiter`]; until then the UI derives
/// rejection rates from `oxide_rate_limited_total`.
#[derive(Debug, Clone, Serialize)]
pub struct TenantView {
    /// Tenant key from `[rate_limits]`.
    pub tenant: String,
    /// Header carrying tenant identity.
    pub match_header: String,
    /// Sustained requests per minute.
    pub rpm: u32,
    /// Sustained tokens per minute.
    pub tpm: u64,
    /// Rolling 24h ceiling.
    pub daily_budget_usd: f64,
    /// Burst headroom over the sustained rate.
    pub burst_multiplier: f64,
    /// Spend recorded so far; survives reloads by design.
    pub spent_usd: f64,
    /// Budget not yet spent, floored at zero.
    pub remaining_usd: f64,
    /// Share of the budget consumed.
    pub budget_used_pct: f64,
}

/// Everything a client needs for a first paint, in one round trip.
#[derive(Debug, Clone, Serialize)]
pub struct AdminSnapshot {
    /// When the snapshot was assembled.
    pub generated_at_ms: u64,
    /// Process uptime.
    pub uptime_ms: u64,
    /// Monotonic config generation.
    pub config_generation: u64,
    /// Public ingress bind address.
    pub listen: String,
    /// Admin bind address, when separate.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub admin_listen: Option<String>,
    /// The last reload attempt.
    pub last_reload: ReloadStatus,
    /// Redacted configuration.
    pub config: RedactedConfig,
    /// Per-provider secret posture.
    pub secrets: Vec<SecretStatus>,
    /// Live health table.
    pub health: HealthSnapshot,
    /// Tenant limits and spend.
    pub tenants: Vec<TenantView>,
}

/// A synthetic request fed to the real resolver, for the X-ray screen.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExplainRequest {
    /// Model name exactly as the client would send it.
    pub model: String,
    /// Prompt text; drives the token and complexity estimators.
    #[serde(default)]
    pub prompt: Option<String>,
    /// Headers the rules match against.
    #[serde(default)]
    pub headers: BTreeMap<String, String>,
    /// Overrides the estimator, e.g. to replay a real request's numbers.
    #[serde(default)]
    pub estimated_prompt_tokens: Option<usize>,
    /// Overrides the complexity heuristic.
    #[serde(default)]
    pub complexity_score: Option<f32>,
}

/// Where a candidate entered the plan.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case", tag = "kind")]
pub enum CandidateSource {
    /// The virtual model's static primary.
    Primary,
    /// A weighted load-balancing target.
    Target {
        /// Relative sampling weight.
        weight: u32,
    },
    /// An entry on the fallback rail.
    Fallback {
        /// Position in the rail, zero-based.
        index: usize,
    },
    /// The concrete upstream a routing rule redirected the request to.
    RuleTarget,
    /// Reachable through chaining, but not named by any model the UI can see.
    Unlisted,
}

/// One candidate in the explained plan.
#[derive(Debug, Clone, Serialize)]
pub struct CandidateView {
    /// Position in the attempt order; `0` is tried first.
    pub order: usize,
    /// `provider/model`.
    pub reference: String,
    /// How this candidate entered the plan.
    pub source: CandidateSource,
    /// Breaker phase at explain time.
    pub breaker: BreakerState,
    /// EWMA TTFT, when observed.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ewma_ttft_us: Option<u64>,
    /// Watchdog deadline for this provider.
    pub ttft_timeout_ms: u64,
}

/// The full "why did this request go there?" answer.
#[derive(Debug, Clone, Serialize)]
pub struct RouteExplain {
    /// The model as requested.
    pub requested_model: String,
    /// Estimated prompt tokens used for rule evaluation.
    pub estimated_prompt_tokens: usize,
    /// Complexity score used for rule evaluation.
    pub complexity_score: f32,
    /// The rule that fired, if any. Only the winner is reported; a full
    /// per-rule match trace needs an engine change (see `docs/ADMIN_API.md`).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub matched_rule: Option<String>,
    /// Status codes that trigger failover rather than a hard error.
    pub retry_on_status: Vec<u16>,
    /// Attempt order, annotated with live health.
    pub candidates: Vec<CandidateView>,
}

/// The admin surface: a borrowed [`AppState`] plus the little it does not hold.
///
/// `AppState` is the hot-path structure and stays untouched by UI concerns. The
/// generation counter, reload status, and write policy live here because only
/// the admin surface reads them.
pub struct AdminState {
    state: Arc<AppState>,
    config_path: PathBuf,
    reload: ArcSwap<ReloadStatus>,
    generation: AtomicU64,
    policy: WritePolicy,
}

impl AdminState {
    /// Builds a read-only admin surface over `state`.
    ///
    /// `config_path` is reported to clients so the editor can show which file a
    /// change belongs in.
    pub fn new(state: Arc<AppState>, config_path: impl Into<PathBuf>) -> Self {
        Self {
            state,
            config_path: config_path.into(),
            reload: ArcSwap::from_pointee(ReloadStatus::boot()),
            generation: AtomicU64::new(1),
            policy: WritePolicy::ReadOnly,
        }
    }

    /// Opts into mutation. Callers should only do this on a trusted listener.
    pub fn with_write_policy(mut self, policy: WritePolicy) -> Self {
        self.policy = policy;
        self
    }

    /// The file a reload or write targets.
    pub fn config_path(&self) -> &Path {
        &self.config_path
    }

    /// The write policy in force.
    pub fn write_policy(&self) -> WritePolicy {
        self.policy
    }

    /// The live config generation.
    pub fn generation(&self) -> u64 {
        self.generation.load(Ordering::Relaxed)
    }

    /// Records a reload performed elsewhere (the file watcher, `SIGHUP`).
    ///
    /// Every reload path should funnel through here so the UI's "config v7
    /// loaded 3m ago" cannot disagree with reality.
    pub fn note_reload(
        &self,
        source: ReloadSource,
        outcome: ReloadOutcome,
        error: Option<Diagnostic>,
    ) -> ReloadStatus {
        let generation = match outcome {
            ReloadOutcome::Applied => self.generation.fetch_add(1, Ordering::Relaxed) + 1,
            ReloadOutcome::Rejected => self.generation(),
        };

        let status = ReloadStatus {
            generation,
            source,
            outcome,
            at_ms: now_ms(),
            error,
        };
        self.reload.store(Arc::new(status.clone()));
        status
    }

    /// Everything, in one round trip.
    pub fn snapshot(&self) -> AdminSnapshot {
        let config = self.state.config();
        let at_ms = now_ms();
        let health = self.health_view();

        AdminSnapshot {
            generated_at_ms: at_ms,
            uptime_ms: self.state.uptime().as_millis() as u64,
            config_generation: self.generation(),
            listen: config.listen.clone(),
            admin_listen: config.admin_listen.clone(),
            last_reload: self.reload.load_full().as_ref().clone(),
            secrets: secret_report(&config),
            tenants: tenant_views(&config, &self.state),
            config: RedactedConfig::from_config(&config),
            health,
        }
    }

    /// The redacted configuration only.
    pub fn config_view(&self) -> RedactedConfig {
        RedactedConfig::from_config(&self.state.config())
    }

    /// The health table, joined against the active config.
    pub fn health_view(&self) -> HealthSnapshot {
        let config = self.state.config();
        let at_ms = now_ms();
        let upstreams = upstream_health(&config, &self.state, at_ms);

        HealthSnapshot {
            at_ms,
            open_breakers: upstreams
                .iter()
                .filter(|upstream| upstream.breaker.state == BreakerState::Open)
                .count(),
            orphans: upstreams
                .iter()
                .filter(|upstream| !upstream.configured)
                .count(),
            upstreams,
        }
    }

    /// Validates a candidate document without touching the running config.
    ///
    /// Uses [`InterpolationMode::Lenient`]: the editor must work on a laptop
    /// that does not hold production secrets, so unset variables become
    /// warnings rather than hard failures.
    pub fn validate(&self, raw: &str) -> ValidationReport {
        let mut diagnostics = Vec::new();

        for var in unresolved_env_vars(raw) {
            diagnostics.push(Diagnostic {
                severity: Severity::Warning,
                code: "oxide::admin::unresolved_env".to_string(),
                message: format!("`${{{var}}}` is not set in this process"),
                help: Some(
                    "The placeholder was kept verbatim for validation. Set the variable, or \
                     write `${VAR:-default}`."
                        .to_string(),
                ),
                field: Some(var),
                span: None,
                chain: None,
            });
        }

        match load_config_from_str(raw, InterpolationMode::Lenient) {
            Ok(_) => ValidationReport {
                ok: true,
                diagnostics,
            },
            Err(error) => {
                diagnostics.push(Diagnostic::from_config_error(&error, raw));
                ValidationReport {
                    ok: false,
                    diagnostics,
                }
            }
        }
    }

    /// Validates and adopts a document.
    ///
    /// Interpolation is **strict** here, unlike [`Self::validate`]: this runs
    /// inside the serving process, and adopting a config whose `${VAR}` was
    /// preserved literally would install the placeholder as an API key.
    ///
    /// A rejected document is returned as [`ReloadOutcome::Rejected`] rather
    /// than an `Err`, because its diagnostics are the payload the UI renders;
    /// only a read-only policy or an unreadable body is a transport error.
    pub fn apply_toml(&self, raw: &str, source: ReloadSource) -> Result<ReloadStatus, AdminError> {
        if !self.policy.permits_write() {
            return Err(AdminError::ReadOnly);
        }

        let status = match load_config_from_str(raw, InterpolationMode::Strict) {
            Ok(config) => match self.state.apply_config(config) {
                Ok(()) => {
                    let generation = self.generation.fetch_add(1, Ordering::Relaxed) + 1;
                    ReloadStatus {
                        generation,
                        source,
                        outcome: ReloadOutcome::Applied,
                        at_ms: now_ms(),
                        error: None,
                    }
                }
                Err(error) => ReloadStatus {
                    generation: self.generation(),
                    source,
                    outcome: ReloadOutcome::Rejected,
                    at_ms: now_ms(),
                    error: Some(diagnostic_from_router_error(&error, raw)),
                },
            },
            Err(error) => ReloadStatus {
                generation: self.generation(),
                source,
                outcome: ReloadOutcome::Rejected,
                at_ms: now_ms(),
                error: Some(Diagnostic::from_config_error(&error, raw)),
            },
        };

        self.reload.store(Arc::new(status.clone()));
        Ok(status)
    }

    /// Explains a routing decision without calling any upstream.
    pub fn explain(&self, request: &ExplainRequest) -> Result<RouteExplain, RouterError> {
        let config = self.state.config();
        explain(&config, &self.state, request)
    }
}

/// Explains a routing decision against an explicit config snapshot.
///
/// The synthetic request goes through [`inspect_request`], so the X-ray uses the
/// same token and complexity estimators as live traffic rather than a second
/// implementation that can drift.
pub fn explain(
    config: &RouterConfig,
    state: &AppState,
    request: &ExplainRequest,
) -> Result<RouteExplain, RouterError> {
    let body = synthetic_body(request);
    let mut parsed = inspect_request(header_map(&request.headers), body)?;

    if let Some(tokens) = request.estimated_prompt_tokens {
        parsed.estimated_prompt_tokens = tokens;
    }
    if let Some(score) = request.complexity_score {
        parsed.complexity_score = score.clamp(0.0, 1.0);
    }

    let matched_rule = matched_rule_name(config, &parsed).map(str::to_string);
    let resolution = resolve_candidates(config, &parsed, state.health())?;

    // A rule may redirect to either a virtual model or a concrete upstream; only
    // the former has a slot table worth labelling against.
    let root_model = matched_rule
        .as_deref()
        .filter(|rule| config.models.contains_key(*rule))
        .unwrap_or(parsed.requested_model.as_str());

    let candidates = resolution
        .candidates
        .iter()
        .enumerate()
        .map(|(order, candidate)| {
            let reference = candidate.reference();
            let metrics = state
                .health()
                .get(&reference)
                .map(|entry| Arc::clone(entry.value()));
            let at_ms = now_ms();
            let ttft_timeout_ms = config
                .providers
                .get(&candidate.provider)
                .map_or(crate::config::DEFAULT_TTFT_TIMEOUT_MS, |provider| {
                    provider.ttft_timeout_ms
                });

            CandidateView {
                order,
                source: classify(config, root_model, &reference, matched_rule.as_deref()),
                reference,
                breaker: metrics.as_ref().map_or(BreakerState::Closed, |metrics| {
                    breaker_state(metrics, at_ms)
                }),
                ewma_ttft_us: metrics
                    .as_ref()
                    .map(|metrics| metrics.ewma_ttft_us())
                    .filter(|ewma| *ewma > 0),
                ttft_timeout_ms,
            }
        })
        .collect();

    Ok(RouteExplain {
        requested_model: parsed.requested_model,
        estimated_prompt_tokens: parsed.estimated_prompt_tokens,
        complexity_score: parsed.complexity_score,
        matched_rule,
        retry_on_status: resolution.retry_on_status,
        candidates,
    })
}

fn diagnostic_from_router_error(error: &RouterError, raw: &str) -> Diagnostic {
    match error {
        RouterError::Config(config) => Diagnostic::from_config_error(config, raw),
        other => Diagnostic {
            severity: Severity::Error,
            code: "oxide::admin::apply".to_string(),
            message: other.to_string(),
            help: None,
            field: None,
            span: None,
            chain: None,
        },
    }
}

fn synthetic_body(request: &ExplainRequest) -> Bytes {
    let prompt = request.prompt.as_deref().unwrap_or_default();
    let payload = serde_json::json!({
        "model": request.model,
        "stream": true,
        "messages": [{"role": "user", "content": prompt}],
    });

    Bytes::from(payload.to_string())
}

fn header_map(headers: &BTreeMap<String, String>) -> HeaderMap {
    let mut map = HeaderMap::new();

    for (name, value) in headers {
        if let (Ok(name), Ok(value)) = (
            HeaderName::try_from(name.as_str()),
            HeaderValue::try_from(value.as_str()),
        ) {
            map.insert(name, value);
        }
    }

    map
}

fn secret_report(config: &RouterConfig) -> Vec<SecretStatus> {
    config
        .providers
        .iter()
        .map(|(name, provider)| SecretStatus {
            provider: name.clone(),
            kind: provider.kind,
            has_api_key: provider.api_key().is_some(),
            base_url_had_credentials: provider
                .base_url
                .as_deref()
                .is_some_and(|base_url| base_url != mask_url_credentials(base_url)),
        })
        .collect()
}

/// Every `provider/model` the active config can dispatch to.
///
/// Rules are included when their target is a concrete upstream; a bare name is
/// a virtual model and contributes its own references instead.
fn configured_references(config: &RouterConfig) -> BTreeSet<String> {
    let mut references = BTreeSet::new();

    for model in config.models.values() {
        references.extend(model.upstream_references().map(str::to_string));
    }
    for rule in &config.rules {
        if rule.route_to.contains('/') {
            references.insert(rule.route_to.clone());
        }
    }

    references
}

fn upstream_health(config: &RouterConfig, state: &AppState, at_ms: u64) -> Vec<UpstreamHealth> {
    let configured = configured_references(config);
    let threshold = config
        .resiliency
        .circuit_breaker
        .consecutive_failures_threshold;

    // The union matters: a reference dropped from the config keeps its health
    // row, and hiding it would hide an ejected upstream nobody owns any more.
    let mut references: BTreeSet<String> = configured.clone();
    references.extend(state.health().iter().map(|entry| entry.key().clone()));

    references
        .into_iter()
        .map(|reference| {
            let metrics = state
                .health()
                .get(&reference)
                .map(|entry| Arc::clone(entry.value()));
            let (provider, model) = reference
                .split_once('/')
                .unwrap_or((reference.as_str(), ""));
            let ttft_timeout_ms = config
                .providers
                .get(provider)
                .map_or(crate::config::DEFAULT_TTFT_TIMEOUT_MS, |config| {
                    config.ttft_timeout_ms
                });
            let is_configured = configured.contains(&reference);
            let provider_owned = provider.to_string();
            let model_owned = model.to_string();

            let ewma_ttft_us = metrics
                .as_ref()
                .map(|metrics| metrics.ewma_ttft_us())
                .filter(|ewma| *ewma > 0);
            let total_requests = metrics
                .as_ref()
                .map_or(0, |metrics| metrics.total_requests());
            let total_failures = metrics
                .as_ref()
                .map_or(0, |metrics| metrics.total_failures());
            let consecutive_failures = metrics
                .as_ref()
                .map_or(0, |metrics| metrics.consecutive_failures());
            let breaker = metrics.as_ref().map_or(
                BreakerView {
                    state: BreakerState::Closed,
                    open_until_ms: None,
                    probe_in_ms: None,
                },
                |metrics| breaker_view(metrics, at_ms),
            );

            UpstreamHealth {
                reference,
                provider: provider_owned,
                model: model_owned,
                configured: is_configured,
                ewma_ttft_us,
                ttft_timeout_ms,
                ttft_headroom_pct: ewma_ttft_us
                    .and_then(|ewma| headroom_pct(ewma, ttft_timeout_ms)),
                consecutive_failures,
                failure_threshold: threshold,
                breaker,
                total_requests,
                total_failures,
                error_rate_pct: (total_requests > 0)
                    .then(|| total_failures as f64 / total_requests as f64 * 100.0),
            }
        })
        .collect()
}

fn headroom_pct(ewma_ttft_us: u64, ttft_timeout_ms: u64) -> Option<f64> {
    if ttft_timeout_ms == 0 {
        return None;
    }

    let deadline_us = (ttft_timeout_ms as f64) * 1_000.0;
    Some(ewma_ttft_us as f64 / deadline_us * 100.0)
}

fn breaker_state(metrics: &crate::state::UpstreamHealthMetrics, at_ms: u64) -> BreakerState {
    if !metrics.is_healthy(at_ms) {
        BreakerState::Open
    } else if metrics.consecutive_failures() > 0 {
        // Cooldown has elapsed while failures remain: the next request probes.
        BreakerState::HalfOpen
    } else {
        BreakerState::Closed
    }
}

fn breaker_view(metrics: &crate::state::UpstreamHealthMetrics, at_ms: u64) -> BreakerView {
    let state = breaker_state(metrics, at_ms);
    let open_until = metrics.circuit_open_until_ms();

    match state {
        BreakerState::Open if open_until > 0 => BreakerView {
            state,
            open_until_ms: Some(open_until),
            probe_in_ms: Some(open_until.saturating_sub(at_ms)),
        },
        _ => BreakerView {
            state,
            open_until_ms: None,
            probe_in_ms: None,
        },
    }
}

fn tenant_views(config: &RouterConfig, state: &AppState) -> Vec<TenantView> {
    config
        .rate_limits
        .iter()
        .map(|(tenant, limits)| {
            let spent_usd = state.rate_limiter().spent_usd(tenant);
            let remaining_usd = (limits.daily_budget_usd - spent_usd).max(0.0);
            let budget_used_pct = if limits.daily_budget_usd > 0.0 {
                spent_usd / limits.daily_budget_usd * 100.0
            } else {
                0.0
            };

            TenantView {
                tenant: tenant.clone(),
                match_header: limits.match_header.clone(),
                rpm: limits.rpm,
                tpm: limits.tpm,
                daily_budget_usd: limits.daily_budget_usd,
                burst_multiplier: limits.burst_multiplier,
                spent_usd,
                remaining_usd,
                budget_used_pct,
            }
        })
        .collect()
}

/// Heuristic display join: which configured slot names this candidate.
///
/// `resolve_candidates` is authoritative about order; this only labels the node
/// so the X-ray can say "fallback #1" without re-deriving the plan. The lookup
/// is *local to the plan's root model first* — the same upstream can be a
/// primary under one virtual model and a weighted target under another, and the
/// global answer would be whichever model sorted first.
fn classify(
    config: &RouterConfig,
    root_model: &str,
    reference: &str,
    matched_rule: Option<&str>,
) -> CandidateSource {
    if let Some(model) = config.models.get(root_model) {
        if model.primary.as_deref() == Some(reference) {
            return CandidateSource::Primary;
        }
        if let Some(target) = model
            .targets
            .iter()
            .find(|target| target.upstream == reference)
        {
            return CandidateSource::Target {
                weight: target.weight,
            };
        }
        if let Some(index) = model
            .fallbacks
            .iter()
            .position(|fallback| fallback == reference)
        {
            return CandidateSource::Fallback { index };
        }
    }

    // A chained virtual model may contribute candidates the root never names.
    if config
        .models
        .values()
        .any(|model| model.primary.as_deref() == Some(reference))
    {
        return CandidateSource::Primary;
    }

    if let Some(target) = config
        .models
        .values()
        .flat_map(|model| model.targets.iter())
        .find(|target| target.upstream == reference)
    {
        return CandidateSource::Target {
            weight: target.weight,
        };
    }

    if let Some(index) = config.models.values().find_map(|model| {
        model
            .fallbacks
            .iter()
            .position(|fallback| fallback == reference)
    }) {
        return CandidateSource::Fallback { index };
    }

    if matched_rule.is_some_and(|name| {
        config
            .rules
            .iter()
            .any(|rule| rule.name == name && rule.route_to == reference)
    }) {
        return CandidateSource::RuleTarget;
    }

    CandidateSource::Unlisted
}

/// `${VAR}` names in `raw` that this process does not define.
fn unresolved_env_vars(raw: &str) -> BTreeSet<String> {
    let mut vars = BTreeSet::new();
    let mut rest = raw;

    while let Some(start) = rest.find("${") {
        let after = &rest[start + 2..];
        let Some(end) = after.find('}') else {
            break;
        };

        let placeholder = &after[..end];

        // `${VAR:-default}` has a value by definition, so it is never unresolved.
        if !placeholder.contains(":-") {
            let valid = !placeholder.is_empty()
                && placeholder
                    .bytes()
                    .all(|byte| byte.is_ascii_uppercase() || byte.is_ascii_digit() || byte == b'_');

            if valid && std::env::var(placeholder).is_err() {
                vars.insert(placeholder.to_string());
            }
        }

        rest = &after[end + 1..];
    }

    vars
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{ProviderConfig, VirtualModelConfig, WeightedTarget};

    fn provider(kind: ProviderKind, api_key: Option<&str>) -> ProviderConfig {
        ProviderConfig {
            kind,
            api_key: api_key.map(str::to_string),
            base_url: None,
            timeout_ms: 60_000,
            ttft_timeout_ms: 2_500,
        }
    }

    fn base_config() -> RouterConfig {
        let mut config = RouterConfig::default();
        config.providers.insert(
            "openai".to_string(),
            provider(ProviderKind::Openai, Some("sk-live-secret")),
        );
        config.providers.insert(
            "anthropic".to_string(),
            provider(ProviderKind::Anthropic, Some("sk-ant-secret")),
        );
        config.models.insert(
            "gpt-4o".to_string(),
            VirtualModelConfig {
                primary: Some("openai/gpt-4o".to_string()),
                fallbacks: vec!["anthropic/claude-3-7-sonnet-latest".to_string()],
                ..VirtualModelConfig::default()
            },
        );
        config
    }

    fn state_and_admin() -> (Arc<AppState>, AdminState) {
        let state = Arc::new(AppState::new(base_config()).expect("state builds"));
        let admin = AdminState::new(Arc::clone(&state), "oxide.toml");
        (state, admin)
    }

    #[test]
    fn redacted_config_contains_no_secret_material() {
        let admin = AdminState::new(
            Arc::new(AppState::new(base_config()).expect("state builds")),
            "oxide.toml",
        );

        let rendered =
            serde_json::to_string(&admin.config_view()).expect("redacted config serializes");

        assert!(!rendered.contains("sk-live-secret"));
        assert!(!rendered.contains("sk-ant-secret"));
        assert!(rendered.contains(REDACTED));
    }

    #[test]
    fn base_url_credentials_are_masked_but_host_survives() {
        let masked = mask_url_credentials("https://user:pass@proxy.internal/v1");
        assert_eq!(masked, "https://***@proxy.internal/v1");

        assert_eq!(
            mask_url_credentials("http://127.0.0.1:11434/v1"),
            "http://127.0.0.1:11434/v1"
        );
    }

    #[test]
    fn secret_report_flags_keys_without_revealing_them() {
        let mut config = base_config();
        config
            .providers
            .get_mut("anthropic")
            .expect("provider exists")
            .base_url = Some("https://u:p@gateway/v1".to_string());

        let report = secret_report(&config);
        let openai = report
            .iter()
            .find(|row| row.provider == "openai")
            .expect("row exists");
        let anthropic = report
            .iter()
            .find(|row| row.provider == "anthropic")
            .expect("row exists");

        assert!(openai.has_api_key);
        assert!(!openai.base_url_had_credentials);
        assert!(anthropic.base_url_had_credentials);

        let rendered = serde_json::to_string(&report).expect("report serializes");
        assert!(!rendered.contains("u:p"));
    }

    #[test]
    fn health_derives_breaker_phase_and_countdown() {
        let (state, admin) = state_and_admin();
        let metrics = state.health_for("openai/gpt-4o");
        for _ in 0..5 {
            metrics.record_failure(5, std::time::Duration::from_secs(30));
        }

        let health = admin.health_view();
        let openai = health
            .upstreams
            .iter()
            .find(|row| row.reference == "openai/gpt-4o")
            .expect("row exists");

        assert_eq!(openai.breaker.state, BreakerState::Open);
        assert!(openai.breaker.probe_in_ms.is_some_and(|ms| ms <= 30_000));
        assert_eq!(openai.consecutive_failures, 5);
        assert_eq!(openai.failure_threshold, 5);
        assert!(openai.configured);
        assert_eq!(health.open_breakers, 1);
    }

    #[test]
    fn health_reports_orphans_and_headroom() {
        let (state, admin) = state_and_admin();
        state
            .health_for("openai/gpt-4o")
            .record_success(std::time::Duration::from_millis(2_000));
        state
            .health_for("retired/none")
            .record_failure(5, std::time::Duration::from_secs(1));

        let health = admin.health_view();
        let live = health
            .upstreams
            .iter()
            .find(|row| row.reference == "openai/gpt-4o")
            .expect("row exists");
        let orphan = health
            .upstreams
            .iter()
            .find(|row| row.reference == "retired/none")
            .expect("orphan row exists");

        assert_eq!(live.ewma_ttft_us, Some(2_000_000));
        // 2s against a 2500ms watchdog is 80% of the deadline.
        assert!(live
            .ttft_headroom_pct
            .is_some_and(|pct| (pct - 80.0).abs() < 0.01));
        assert!(!orphan.configured);
        assert_eq!(health.orphans, 1);
    }

    #[test]
    fn validate_accepts_a_good_document() {
        let (_state, admin) = state_and_admin();
        let report = admin.validate(
            "listen = \"0.0.0.0:8080\"\n[providers.openai]\ntype = \"openai\"\napi_key = \"x\"\n\n\
             [models.m]\nprimary = \"openai/gpt-4o\"\n",
        );

        assert!(report.ok, "diagnostics: {:?}", report.diagnostics);
        assert!(report
            .diagnostics
            .iter()
            .all(|diagnostic| diagnostic.severity == Severity::Warning));
    }

    #[test]
    fn validate_points_at_the_offending_line() {
        let (_state, admin) = state_and_admin();
        let report = admin.validate("listen = \"0.0.0.0:8080\"\napi_keey = \"typo\"\n");

        assert!(!report.ok);
        let error = report
            .diagnostics
            .iter()
            .find(|diagnostic| diagnostic.severity == Severity::Error)
            .expect("error present");
        assert_eq!(error.code, "oxide::config::toml");
        assert!(error.span.is_some_and(|span| span.line >= 2));
    }

    #[test]
    fn validate_reports_undefined_provider_with_its_context() {
        let (_state, admin) = state_and_admin();
        let report = admin.validate(
            "[providers.openai]\ntype = \"openai\"\napi_key = \"x\"\n\n\
             [models.m]\nprimary = \"ghost/gpt-4o\"\n",
        );

        assert!(!report.ok);
        let error = report
            .diagnostics
            .iter()
            .find(|diagnostic| diagnostic.severity == Severity::Error)
            .expect("error present");
        assert_eq!(error.code, "oxide::config::undefined_provider");
        assert_eq!(error.field.as_deref(), Some("models.m.primary"));
    }

    #[test]
    fn validate_warns_about_unset_environment_variables() {
        let (_state, admin) = state_and_admin();
        let report = admin.validate(
            "[providers.openai]\ntype = \"openai\"\napi_key = \"${OXIDE_TEST_DEFINITELY_UNSET}\"\n",
        );

        assert!(report.ok, "an unset key must not block editing");
        assert!(report.diagnostics.iter().any(|diagnostic| diagnostic.code
            == "oxide::admin::unresolved_env"
            && diagnostic.field.as_deref() == Some("OXIDE_TEST_DEFINITELY_UNSET")));
    }

    #[test]
    fn apply_is_refused_when_read_only() {
        let (_state, admin) = state_and_admin();
        assert!(matches!(
            admin.apply_toml("listen = \"0.0.0.0:8080\"\n", ReloadSource::Manual),
            Err(AdminError::ReadOnly)
        ));
        assert_eq!(admin.generation(), 1);
    }

    #[test]
    fn apply_increments_generation_and_swaps_config() {
        let (state, admin) = state_and_admin();
        let admin = admin.with_write_policy(WritePolicy::ReadWrite);

        let status = admin
            .apply_toml(
                "listen = \"127.0.0.1:9999\"\n[providers.openai]\ntype = \"openai\"\napi_key = \"x\"\n",
                ReloadSource::Manual,
            )
            .expect("admin permits writes");

        assert_eq!(status.outcome, ReloadOutcome::Applied);
        assert_eq!(status.generation, 2);
        assert_eq!(admin.generation(), 2);
        assert_eq!(state.config().listen, "127.0.0.1:9999");
        assert_eq!(admin.snapshot().last_reload.generation, 2);
    }

    #[test]
    fn rejected_apply_keeps_the_previous_config_live() {
        let (state, admin) = state_and_admin();
        let admin = admin.with_write_policy(WritePolicy::ReadWrite);

        let status = admin
            .apply_toml(
                "listen = \"127.0.0.1:9999\"\n[providers.openai]\ntype = \"openai\"\napi_key = \"x\"\n\n\
                 [models.m]\nprimary = \"ghost/gpt-4o\"\n",
                ReloadSource::Manual,
            )
            .expect("admin permits writes");

        assert_eq!(status.outcome, ReloadOutcome::Rejected);
        assert_eq!(status.generation, 1);
        assert_eq!(
            status.error.as_ref().map(|error| error.code.as_str()),
            Some("oxide::config::undefined_provider")
        );
        assert_eq!(state.config().listen, RouterConfig::default().listen);
    }

    #[test]
    fn explain_renders_the_attempt_order_with_sources() {
        let (state, _admin) = state_and_admin();
        let config = state.config();

        let explained = explain(
            &config,
            &state,
            &ExplainRequest {
                model: "gpt-4o".to_string(),
                prompt: Some("hello".to_string()),
                headers: BTreeMap::new(),
                estimated_prompt_tokens: None,
                complexity_score: None,
            },
        )
        .expect("explain succeeds");

        assert_eq!(explained.candidates.len(), 2);
        assert_eq!(explained.candidates[0].reference, "openai/gpt-4o");
        assert_eq!(explained.candidates[0].source, CandidateSource::Primary);
        assert_eq!(
            explained.candidates[1].source,
            CandidateSource::Fallback { index: 0 }
        );
        assert_eq!(explained.candidates[1].order, 1);
        assert!(explained.matched_rule.is_none());
    }

    #[test]
    fn explain_uses_the_configured_weights_and_watchdog() {
        let mut config = base_config();
        config.models.insert(
            "smart".to_string(),
            VirtualModelConfig {
                strategy: crate::config::RoutingStrategy::WeightedRandom,
                targets: vec![
                    WeightedTarget {
                        upstream: "openai/gpt-4o".to_string(),
                        weight: 70,
                    },
                    WeightedTarget {
                        upstream: "anthropic/claude-3-7-sonnet-latest".to_string(),
                        weight: 30,
                    },
                ],
                ..VirtualModelConfig::default()
            },
        );
        let state = Arc::new(AppState::new(config).expect("state builds"));

        let explained = explain(
            &state.config(),
            &state,
            &ExplainRequest {
                model: "smart".to_string(),
                prompt: Some("hello".to_string()),
                headers: BTreeMap::new(),
                estimated_prompt_tokens: None,
                complexity_score: None,
            },
        )
        .expect("explain succeeds");

        assert_eq!(explained.candidates.len(), 1);

        // `weighted_random` samples exactly one target; the sample is random, so
        // the assertion is on the label rather than the winner.
        let chosen = &explained.candidates[0];
        let expected_weight = match chosen.reference.as_str() {
            "openai/gpt-4o" => 70,
            _ => 30,
        };
        assert_eq!(
            chosen.source,
            CandidateSource::Target {
                weight: expected_weight
            }
        );
        assert_eq!(chosen.ttft_timeout_ms, 2_500);
    }

    #[test]
    fn snapshot_never_serializes_a_secret() {
        let (_state, admin) = state_and_admin();
        let rendered = serde_json::to_string(&admin.snapshot()).expect("snapshot serializes");

        assert!(!rendered.contains("sk-live-secret"));
        assert!(!rendered.contains("sk-ant-secret"));
        assert!(rendered.contains("\"config_generation\":1"));
    }

    #[test]
    fn tenant_views_report_spend_that_survives_reload() {
        let mut config = base_config();
        config.rate_limits.insert(
            "acme".to_string(),
            crate::config::RateLimitConfig {
                match_header: "X-Tenant-ID".to_string(),
                rpm: 600,
                tpm: 250_000,
                daily_budget_usd: 50.0,
                burst_multiplier: 1.5,
            },
        );
        let state = Arc::new(AppState::new(config).expect("state builds"));
        state.rate_limiter().record_spend_usd("acme", 12.5);

        let admin = AdminState::new(Arc::clone(&state), "oxide.toml");
        let snapshot = admin.snapshot();
        let tenant = snapshot
            .tenants
            .iter()
            .find(|view| view.tenant == "acme")
            .expect("tenant row exists");

        assert!((tenant.spent_usd - 12.5).abs() < 1e-9);
        assert!((tenant.remaining_usd - 37.5).abs() < 1e-9);
        assert!((tenant.budget_used_pct - 25.0).abs() < 1e-9);
    }

    #[test]
    fn diagnostics_carry_the_miette_code_and_help() {
        let error = ConfigError::UndefinedProvider {
            reference: "ghost/gpt-4o".to_string(),
            provider: "ghost".to_string(),
            context: "models.m.primary".to_string(),
        };
        let diagnostic = Diagnostic::from_config_error(&error, "");

        assert_eq!(diagnostic.code, "oxide::config::undefined_provider");
        assert!(diagnostic.help.is_some());
        assert_eq!(diagnostic.field.as_deref(), Some("models.m.primary"));
    }

    #[test]
    fn unresolved_env_scan_ignores_lowercase_and_defaulted_names() {
        let vars = unresolved_env_vars(
            "${ALSO_UNSET_XYZ} ${also_lower} ${ALSO_UNSET_XYZ:-fallback} ${SET_EMPTY:-x}",
        );

        assert!(vars.contains("ALSO_UNSET_XYZ"));
        assert!(!vars.iter().any(|var| var == "also_lower"));
    }
}

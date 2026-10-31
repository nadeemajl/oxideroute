//! Declarative configuration model for OxideRoute (`oxide.toml`).
//!
//! Every struct here derives [`serde::Deserialize`], [`serde::Serialize`], and
//! [`schemars::JsonSchema`] so the same definitions drive parsing, round-trip
//! serialization, and the published JSON schema (`oxide schema`).
//!
//! Unknown keys are rejected (`deny_unknown_fields`) so a typo in a config file
//! fails loudly at load time instead of silently falling back to a default.
//!
//! Maps use [`BTreeMap`] rather than `HashMap` to keep schema output and
//! serialization deterministic across runs.

use std::collections::BTreeMap;

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

pub mod loader;
pub mod watcher;

/// Address the public ingress binds to when unspecified.
pub const DEFAULT_LISTEN: &str = "0.0.0.0:8080";
/// Default per-attempt upstream request timeout.
pub const DEFAULT_TIMEOUT_MS: u64 = 60_000;
/// Default pre-first-token watchdog deadline.
pub const DEFAULT_TTFT_TIMEOUT_MS: u64 = 2_500;
/// Default tenant header used to attribute rate limits and budgets.
pub const DEFAULT_TENANT_HEADER: &str = "X-Tenant-ID";
/// Default Prometheus scrape path.
pub const DEFAULT_PROMETHEUS_ENDPOINT: &str = "/metrics";

fn default_listen() -> String {
    DEFAULT_LISTEN.to_string()
}

fn default_prometheus_endpoint() -> String {
    DEFAULT_PROMETHEUS_ENDPOINT.to_string()
}

fn default_timeout_ms() -> u64 {
    DEFAULT_TIMEOUT_MS
}

fn default_ttft_timeout_ms() -> u64 {
    DEFAULT_TTFT_TIMEOUT_MS
}

fn default_tenant_header() -> String {
    DEFAULT_TENANT_HEADER.to_string()
}

fn default_graceful_shutdown_ms() -> u64 {
    30_000
}

fn default_weight() -> u32 {
    1
}

fn default_burst_multiplier() -> f64 {
    1.0
}

/// Root configuration document.
#[derive(Debug, Clone, PartialEq, Deserialize, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct RouterConfig {
    /// Public ingress bind address, e.g. `0.0.0.0:8080`.
    #[serde(default = "default_listen")]
    pub listen: String,

    /// Optional admin/health bind address, kept off the public listener.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub admin_listen: Option<String>,

    /// How long in-flight streams may drain during shutdown.
    #[serde(default = "default_graceful_shutdown_ms")]
    pub graceful_shutdown_timeout_ms: u64,

    /// Telemetry and logging behaviour.
    #[serde(default)]
    pub telemetry: TelemetryConfig,

    /// Response cache settings.
    #[serde(default)]
    pub cache: CacheConfig,

    /// Circuit breaker and retry behaviour.
    #[serde(default)]
    pub resiliency: ResiliencyConfig,

    /// Per-tenant limits, keyed by tenant name.
    #[serde(default)]
    pub rate_limits: BTreeMap<String, RateLimitConfig>,

    /// Upstream provider credentials and endpoints, keyed by provider name.
    #[serde(default)]
    pub providers: BTreeMap<String, ProviderConfig>,

    /// Virtual models exposed on the ingress API, keyed by model name.
    #[serde(default)]
    pub models: BTreeMap<String, VirtualModelConfig>,

    /// Routing rules, evaluated in `priority` order then declaration order.
    #[serde(default)]
    pub rules: Vec<RoutingRule>,

    /// Token pricing, keyed by `provider/model`, used for tenant spend tracking.
    ///
    /// Entries are optional: an unpriced upstream costs `0.0`, which makes the
    /// daily budget inert for it rather than silently guessing a price.
    #[serde(default)]
    pub pricing: BTreeMap<String, PricingConfig>,
}

impl RouterConfig {
    /// Cost of one exchange, in USD.
    ///
    /// Returns `0.0` for upstreams without a `[pricing."provider/model"]`
    /// entry, so unpriced traffic never trips a budget it cannot be charged for.
    pub fn cost_usd(
        &self,
        provider: &str,
        model: &str,
        prompt_tokens: usize,
        completion_tokens: usize,
    ) -> f64 {
        let key = format!("{provider}/{model}");
        let Some(pricing) = self.pricing.get(&key) else {
            return 0.0;
        };

        (prompt_tokens as f64 * pricing.prompt_per_million_usd
            + completion_tokens as f64 * pricing.completion_per_million_usd)
            / 1_000_000.0
    }
}

impl Default for RouterConfig {
    fn default() -> Self {
        Self {
            listen: default_listen(),
            admin_listen: None,
            graceful_shutdown_timeout_ms: default_graceful_shutdown_ms(),
            telemetry: TelemetryConfig::default(),
            cache: CacheConfig::default(),
            resiliency: ResiliencyConfig::default(),
            rate_limits: BTreeMap::new(),
            providers: BTreeMap::new(),
            models: BTreeMap::new(),
            rules: Vec::new(),
            pricing: BTreeMap::new(),
        }
    }
}

/// USD price per million tokens for one upstream.
#[derive(Debug, Clone, PartialEq, Deserialize, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct PricingConfig {
    /// USD per million prompt (input) tokens.
    pub prompt_per_million_usd: f64,
    /// USD per million completion (output) tokens.
    pub completion_per_million_usd: f64,
}

/// Telemetry, logging, and PII handling.
#[derive(Debug, Clone, PartialEq, Deserialize, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct TelemetryConfig {
    /// Path serving the Prometheus exposition format.
    #[serde(default = "default_prometheus_endpoint")]
    pub prometheus_endpoint: String,

    /// OTLP gRPC collector endpoint; unset disables trace export.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub otlp_endpoint: Option<String>,

    /// Scrub PII from logs and traces. Defaults to on.
    #[serde(default = "default_true")]
    pub redact_pii: bool,

    /// Log raw request/response payloads. Off by default; never enable in production.
    #[serde(default)]
    pub log_payloads: bool,
}

fn default_true() -> bool {
    true
}

impl Default for TelemetryConfig {
    fn default() -> Self {
        Self {
            prometheus_endpoint: default_prometheus_endpoint(),
            otlp_endpoint: None,
            redact_pii: true,
            log_payloads: false,
        }
    }
}

/// Backing store for the response cache.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum CacheBackend {
    /// In-process cache; the only backend defined today.
    #[default]
    Memory,
}

/// Response cache settings.
#[derive(Debug, Clone, PartialEq, Deserialize, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct CacheConfig {
    /// Whether the response cache is active.
    #[serde(default)]
    pub enabled: bool,

    /// Which backing store to use.
    #[serde(default)]
    pub backend: CacheBackend,

    /// Upper bound on cache memory, in megabytes.
    #[serde(default = "default_cache_capacity_mb")]
    pub max_capacity_mb: u64,

    /// Entry time-to-live in seconds.
    #[serde(default = "default_cache_ttl_seconds")]
    pub ttl_seconds: u64,
}

fn default_cache_capacity_mb() -> u64 {
    512
}

fn default_cache_ttl_seconds() -> u64 {
    3_600
}

impl Default for CacheConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            backend: CacheBackend::Memory,
            max_capacity_mb: default_cache_capacity_mb(),
            ttl_seconds: default_cache_ttl_seconds(),
        }
    }
}

/// Resilience settings; currently the circuit breaker block.
#[derive(Debug, Clone, Default, PartialEq, Deserialize, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ResiliencyConfig {
    /// Passive circuit breaker tuning.
    #[serde(default)]
    pub circuit_breaker: CircuitBreakerConfig,
}

/// Passive per-upstream circuit breaker tuning.
#[derive(Debug, Clone, PartialEq, Deserialize, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct CircuitBreakerConfig {
    /// Consecutive failures that trip the breaker.
    #[serde(default = "default_consecutive_failure_threshold")]
    pub consecutive_failures_threshold: u32,

    /// Error-rate percentage within the window that also trips the breaker.
    #[serde(default = "default_error_rate_threshold_pct")]
    pub error_rate_threshold_pct: f64,

    /// Sliding window length for error-rate calculation, in seconds.
    #[serde(default = "default_window_seconds")]
    pub window_seconds: u64,

    /// How long the breaker stays open before allowing a probe.
    #[serde(default = "default_cooldown_seconds")]
    pub cooldown_seconds: u64,

    /// Interval between active probes while half-open.
    #[serde(default = "default_active_probe_interval_seconds")]
    pub active_probe_interval_seconds: u64,
}

fn default_consecutive_failure_threshold() -> u32 {
    5
}

fn default_error_rate_threshold_pct() -> f64 {
    50.0
}

fn default_window_seconds() -> u64 {
    30
}

fn default_cooldown_seconds() -> u64 {
    15
}

fn default_active_probe_interval_seconds() -> u64 {
    10
}

impl Default for CircuitBreakerConfig {
    fn default() -> Self {
        Self {
            consecutive_failures_threshold: default_consecutive_failure_threshold(),
            error_rate_threshold_pct: default_error_rate_threshold_pct(),
            window_seconds: default_window_seconds(),
            cooldown_seconds: default_cooldown_seconds(),
            active_probe_interval_seconds: default_active_probe_interval_seconds(),
        }
    }
}

/// Per-tenant request, token, and spend limits.
#[derive(Debug, Clone, PartialEq, Deserialize, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct RateLimitConfig {
    /// Header carrying the tenant identity.
    #[serde(default = "default_tenant_header")]
    pub match_header: String,

    /// Requests per minute allowed for this tenant.
    pub rpm: u32,

    /// Tokens per minute allowed for this tenant.
    pub tpm: u64,

    /// Rolling 24h USD spend ceiling.
    pub daily_budget_usd: f64,

    /// Burst headroom multiplier applied on top of the sustained rate.
    #[serde(default = "default_burst_multiplier")]
    pub burst_multiplier: f64,
}

/// Upstream provider definition.
#[derive(Debug, Clone, PartialEq, Deserialize, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ProviderConfig {
    /// Which wire protocol the provider speaks.
    #[serde(rename = "type")]
    pub kind: ProviderKind,

    /// Bearer/API key. Interpolated from the environment in normal use.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub api_key: Option<String>,

    /// Override for the provider base URL.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub base_url: Option<String>,

    /// Total per-attempt request timeout, in milliseconds.
    #[serde(default = "default_timeout_ms")]
    pub timeout_ms: u64,

    /// Pre-first-token watchdog deadline, in milliseconds.
    #[serde(default = "default_ttft_timeout_ms")]
    pub ttft_timeout_ms: u64,
}

impl ProviderConfig {
    /// Whether this provider kind requires an API key to authenticate.
    ///
    /// `openai_compatible` targets (vLLM, Ollama, LM Studio) are typically
    /// unauthenticated, and Bedrock authenticates via the AWS credential chain
    /// rather than a static key.
    pub fn requires_api_key(kind: ProviderKind) -> bool {
        matches!(
            kind,
            ProviderKind::Openai | ProviderKind::Anthropic | ProviderKind::Gemini
        )
    }

    /// Resolved API key, treating whitespace-only values as absent.
    pub fn api_key(&self) -> Option<&str> {
        self.api_key
            .as_deref()
            .map(str::trim)
            .filter(|key| !key.is_empty())
    }
}

/// Wire protocols supported by the upstream adapters.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum ProviderKind {
    /// OpenAI `/v1/chat/completions`.
    Openai,
    /// Any OpenAI-compatible server (vLLM, Ollama, LM Studio, ...).
    OpenaiCompatible,
    /// Anthropic `/v1/messages`.
    Anthropic,
    /// Google Gemini `streamGenerateContent`.
    Gemini,
    /// AWS Bedrock `ConverseStream`.
    Bedrock,
}

/// A model exposed to clients, resolved into one or more upstream candidates.
///
/// Either use the priority form:
///
/// ```toml
/// [models."gpt-4o"]
/// primary = "openai/gpt-4o"
/// fallbacks = ["anthropic/claude-3-7-sonnet-latest"]
/// ```
///
/// or a load-balancing strategy:
///
/// ```toml
/// [models."smart-router"]
/// strategy = "latency_ewma"
/// targets = [{ upstream = "openai_us/gpt-4o", weight = 70 }]
/// fallbacks = ["gemini_vertex/gemini-2.5-pro"]
/// ```
#[derive(Debug, Clone, Default, PartialEq, Deserialize, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct VirtualModelConfig {
    /// Static primary upstream. Required for the `priority` strategy.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub primary: Option<String>,

    /// How `targets` are ordered or sampled.
    #[serde(default)]
    pub strategy: RoutingStrategy,

    /// Candidate upstreams for load-balancing strategies.
    #[serde(default)]
    pub targets: Vec<WeightedTarget>,

    /// Ordered failover chain, tried after the primary/target candidate fails.
    #[serde(default)]
    pub fallbacks: Vec<String>,

    /// Upstream status codes that trigger failover rather than a hard error.
    #[serde(default)]
    pub retry_on_status: Vec<u16>,
}

impl VirtualModelConfig {
    /// Every upstream reference this model can dispatch to, in priority order.
    ///
    /// Used by validation and cycle detection; deliberately excludes nothing so
    /// a misconfigured reference cannot hide in an unvisited field.
    pub fn upstream_references(&self) -> impl Iterator<Item = &str> {
        self.primary
            .as_deref()
            .into_iter()
            .chain(self.targets.iter().map(|target| target.upstream.as_str()))
            .chain(self.fallbacks.iter().map(String::as_str))
    }
}

/// A weighted upstream candidate for load-balancing strategies.
#[derive(Debug, Clone, PartialEq, Deserialize, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct WeightedTarget {
    /// `provider/model` reference.
    pub upstream: String,

    /// Relative sampling weight. Higher wins more traffic.
    #[serde(default = "default_weight")]
    pub weight: u32,
}

/// How a virtual model turns its `targets` into an ordered candidate list.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum RoutingStrategy {
    /// Always try `primary`, then `fallbacks` in order.
    #[default]
    Priority,
    /// Sample the first candidate by weight, then fall back in order.
    WeightedRandom,
    /// Order healthy targets by lowest observed TTFT (EWMA).
    LatencyEwma,
}

/// A conditional routing rule that can override model resolution.
#[derive(Debug, Clone, PartialEq, Deserialize, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct RoutingRule {
    /// Unique, human-readable rule name; appears in traces and `dry-run` output.
    pub name: String,

    /// Evaluation order; lower runs first. Defaults to declaration position.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub priority: Option<u32>,

    /// Predicates that must all match for the rule to fire.
    pub condition: RuleCondition,

    /// Where matching requests are sent: `provider/model` or a virtual model.
    pub route_to: String,
}

/// Predicates combined with logical AND. An empty condition is rejected.
#[derive(Debug, Clone, Default, PartialEq, Deserialize, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct RuleCondition {
    /// Matches when the requested model equals this value (e.g. `auto`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model: Option<String>,

    /// Matches when the estimated prompt size is at least this many tokens.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub min_estimated_tokens: Option<usize>,

    /// Matches when the estimated prompt size is at most this many tokens.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_estimated_tokens: Option<usize>,

    /// Matches when the heuristic complexity score is below this threshold.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub complexity_score_below: Option<f32>,

    /// Matches when the heuristic complexity score is above this threshold.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub complexity_score_above: Option<f32>,

    /// Exact-match requirements on request headers.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub header_matches: Option<BTreeMap<String, String>>,

    /// Regex-match requirements on request headers.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub header_regex_matches: Option<BTreeMap<String, String>>,
}

impl RuleCondition {
    /// Whether the condition constrains anything at all.
    ///
    /// A rule with no predicates would match every request, which is almost
    /// always a mistake, so validation rejects it.
    pub fn is_empty(&self) -> bool {
        self.model.is_none()
            && self.min_estimated_tokens.is_none()
            && self.max_estimated_tokens.is_none()
            && self.complexity_score_below.is_none()
            && self.complexity_score_above.is_none()
            && self
                .header_matches
                .as_ref()
                .filter(|map| !map.is_empty())
                .is_none()
            && self
                .header_regex_matches
                .as_ref()
                .filter(|map| !map.is_empty())
                .is_none()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn provider_defaults_are_applied() {
        let parsed: ProviderConfig =
            toml::from_str("type = \"openai\"\napi_key = \"sk-test\"").expect("provider parses");

        assert_eq!(parsed.kind, ProviderKind::Openai);
        assert_eq!(parsed.timeout_ms, DEFAULT_TIMEOUT_MS);
        assert_eq!(parsed.ttft_timeout_ms, DEFAULT_TTFT_TIMEOUT_MS);
        assert!(parsed.base_url.is_none());
    }

    #[test]
    fn provider_kind_rejects_unknown_protocol() {
        let result: Result<ProviderConfig, _> = toml::from_str("type = \"mystery\"");
        assert!(result.is_err(), "unknown provider type must not parse");
    }

    #[test]
    fn unknown_field_is_rejected_rather_than_ignored() {
        let result: Result<ProviderConfig, _> =
            toml::from_str("type = \"openai\"\napi_keey = \"typo\"");
        assert!(result.is_err(), "unknown keys must fail loudly");
    }

    #[test]
    fn virtual_model_defaults_to_priority_strategy() {
        let parsed: VirtualModelConfig =
            toml::from_str("primary = \"openai/gpt-4o\"").expect("model parses");

        assert_eq!(parsed.strategy, RoutingStrategy::Priority);
        assert!(parsed.targets.is_empty());
        assert!(parsed.fallbacks.is_empty());
    }

    #[test]
    fn weighted_target_defaults_to_weight_one() {
        let parsed: WeightedTarget = toml::from_str("upstream = \"p/m\"").expect("target parses");
        assert_eq!(parsed.weight, 1);
    }

    #[test]
    fn upstream_references_covers_primary_targets_and_fallbacks() {
        let model = VirtualModelConfig {
            primary: Some("a/primary".to_string()),
            strategy: RoutingStrategy::LatencyEwma,
            targets: vec![WeightedTarget {
                upstream: "b/target".to_string(),
                weight: 10,
            }],
            fallbacks: vec!["c/fallback".to_string()],
            retry_on_status: vec![429],
        };

        let refs: Vec<&str> = model.upstream_references().collect();
        assert_eq!(refs, vec!["a/primary", "b/target", "c/fallback"]);
    }

    #[test]
    fn empty_condition_is_detected() {
        assert!(RuleCondition::default().is_empty());
        assert!(!RuleCondition {
            min_estimated_tokens: Some(0),
            ..RuleCondition::default()
        }
        .is_empty());
        assert!(!RuleCondition {
            header_matches: Some(BTreeMap::from([("X".to_string(), "Y".to_string())])),
            ..RuleCondition::default()
        }
        .is_empty());
    }

    #[test]
    fn config_round_trips_through_toml() {
        let config = RouterConfig {
            listen: "127.0.0.1:1234".to_string(),
            ..RouterConfig::default()
        };

        let encoded = toml::to_string(&config).expect("config serializes");
        let decoded: RouterConfig = toml::from_str(&encoded).expect("config deserializes");
        assert_eq!(decoded, config);
    }

    #[test]
    fn cost_uses_the_pricing_table() {
        let mut config = RouterConfig::default();
        config.pricing.insert(
            "p/m".to_string(),
            PricingConfig {
                prompt_per_million_usd: 3.0,
                completion_per_million_usd: 15.0,
            },
        );

        let cost = config.cost_usd("p", "m", 1_000_000, 1_000_000);
        assert!((cost - 18.0).abs() < 1e-9, "got {cost}");
    }

    #[test]
    fn unpriced_upstreams_cost_nothing() {
        let config = RouterConfig::default();
        assert_eq!(config.cost_usd("p", "m", 1_000_000, 1_000_000), 0.0);
    }

    #[test]
    fn json_schema_covers_the_root_config() {
        let schema = schemars::schema_for!(RouterConfig);
        let json = serde_json::to_value(&schema).expect("schema serializes");

        for field in [
            "listen",
            "admin_listen",
            "telemetry",
            "cache",
            "resiliency",
            "rate_limits",
            "providers",
            "models",
            "rules",
        ] {
            assert!(
                json["properties"].get(field).is_some(),
                "schema is missing `{field}`"
            );
        }
    }

    #[test]
    fn json_schema_enumerates_provider_kinds() {
        let schema = schemars::schema_for!(ProviderConfig);
        let json = serde_json::to_value(&schema).expect("schema serializes");
        let rendered = json.to_string();

        for kind in [
            "openai",
            "openai_compatible",
            "anthropic",
            "gemini",
            "bedrock",
        ] {
            assert!(
                rendered.contains(&format!("\"{kind}\"")),
                "provider schema is missing kind `{kind}`"
            );
        }
    }
}

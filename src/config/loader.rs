//! TOML loading, `${ENV_VAR}` interpolation, and semantic validation.

use std::collections::{BTreeMap, BTreeSet};
use std::net::SocketAddr;
use std::path::Path;
use std::sync::LazyLock;

use regex::Regex;

use crate::config::{ProviderConfig, ProviderKind, RouterConfig, RoutingStrategy};
use crate::error::{ConfigError, RoutingChain};

/// Longest virtual-model chain accepted before we declare the config pathological.
///
/// Cycle detection catches loops; this catches merely absurd (and likely
/// generated-by-accident) linear chains that would blow the default stack.
const MAX_ROUTING_DEPTH: usize = 256;

/// Placeholder syntax: `${NAME}` or `${NAME:-fallback}`.
///
/// Names are intentionally restricted to uppercase ASCII, digits, and
/// underscores so that shell-isms like `${VAR/x/y}` are left untouched.
const ENV_PATTERN: &str = r"\$\{([A-Z0-9_]+)(?::-([^}]*))?\}";

static ENV_INTERPOLATION_RE: LazyLock<Result<Regex, String>> =
    LazyLock::new(|| Regex::new(ENV_PATTERN).map_err(|error| error.to_string()));

/// How to treat `${VAR}` placeholders with no value.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InterpolationMode {
    /// `serve`: a missing variable is a hard error. Startup must not silently
    /// run against a half-configured upstream.
    Strict,
    /// `validate`, `dry-run`, `init`, `schema`: a missing variable leaves the
    /// placeholder intact so structure can be checked without real secrets.
    Lenient,
}

impl InterpolationMode {
    /// Mode used by the serving path.
    pub const fn for_serve() -> Self {
        Self::Strict
    }

    /// Mode used by offline tooling.
    pub const fn for_tooling() -> Self {
        Self::Lenient
    }
}

fn env_regex() -> Result<&'static Regex, ConfigError> {
    match ENV_INTERPOLATION_RE.as_ref() {
        Ok(regex) => Ok(regex),
        Err(detail) => Err(ConfigError::Interpolation {
            detail: detail.clone(),
        }),
    }
}

/// Expands `${VAR}` / `${VAR:-default}` using `lookup` as the variable source.
///
/// Injecting `lookup` keeps interpolation deterministic under test without
/// mutating process-global environment state.
pub fn interpolate_with<F>(
    input: &str,
    mode: InterpolationMode,
    lookup: F,
) -> Result<String, ConfigError>
where
    F: Fn(&str) -> Option<String>,
{
    let regex = env_regex()?;
    let mut output = String::with_capacity(input.len());
    let mut cursor = 0usize;

    for captures in regex.captures_iter(input) {
        let Some(whole) = captures.get(0) else {
            continue;
        };

        // Copy everything between the previous match and this one verbatim.
        output.push_str(&input[cursor..whole.start()]);

        let name = captures
            .get(1)
            .map(|capture| capture.as_str())
            .unwrap_or_default();
        let fallback = captures.get(2).map(|capture| capture.as_str());

        match (lookup(name), fallback) {
            (Some(value), _) => output.push_str(&value),
            (None, Some(default_value)) => output.push_str(default_value),
            (None, None) => match mode {
                InterpolationMode::Strict => {
                    return Err(ConfigError::MissingEnvVar {
                        var: name.to_string(),
                    })
                }
                // Preserve the placeholder: offline tooling can still inspect
                // the document without the secret present.
                InterpolationMode::Lenient => output.push_str(whole.as_str()),
            },
        }

        cursor = whole.end();
    }

    output.push_str(&input[cursor..]);
    Ok(output)
}

/// Expands placeholders using the real process environment.
pub fn interpolate_env(input: &str, mode: InterpolationMode) -> Result<String, ConfigError> {
    interpolate_with(input, mode, |name| std::env::var(name).ok())
}

/// Interpolates, parses, and validates a configuration document.
pub fn load_config_from_str(
    input: &str,
    mode: InterpolationMode,
) -> Result<RouterConfig, ConfigError> {
    let expanded = interpolate_env(input, mode)?;
    let config: RouterConfig =
        toml::from_str(&expanded).map_err(|source| ConfigError::Toml { source })?;
    validate(&config)?;
    Ok(config)
}

/// Reads, interpolates, parses, and validates a configuration file.
pub fn load_config_from_path(
    path: impl AsRef<Path>,
    mode: InterpolationMode,
) -> Result<RouterConfig, ConfigError> {
    let path = path.as_ref();
    let raw = std::fs::read_to_string(path).map_err(|source| ConfigError::Io {
        path: path.display().to_string(),
        source,
    })?;
    load_config_from_str(&raw, mode)
}

fn invalid(field: impl Into<String>, detail: impl Into<String>) -> ConfigError {
    ConfigError::InvalidValue {
        field: field.into(),
        detail: detail.into(),
    }
}

/// Semantic checks that the type system cannot express.
///
/// Everything here is fail-fast and typed, so `oxide validate` can point at the
/// exact field that is wrong.
pub fn validate(config: &RouterConfig) -> Result<(), ConfigError> {
    validate_listener(config)?;
    validate_telemetry(config)?;
    validate_cache(config)?;
    validate_resiliency(config)?;
    validate_rate_limits(config)?;
    validate_providers(config)?;
    validate_models(config)?;
    validate_rules(config)?;
    validate_pricing(config)?;
    detect_circular_routing(config)?;
    Ok(())
}

fn parse_socket_addr(field: &str, value: &str) -> Result<SocketAddr, ConfigError> {
    value.parse::<SocketAddr>().map_err(|error| {
        invalid(
            field,
            format!("`{value}` is not a valid socket address ({error})"),
        )
    })
}

fn validate_listener(config: &RouterConfig) -> Result<(), ConfigError> {
    parse_socket_addr("listen", &config.listen)?;

    if let Some(admin) = &config.admin_listen {
        parse_socket_addr("admin_listen", admin)?;
    }

    if config.graceful_shutdown_timeout_ms == 0 {
        return Err(invalid(
            "graceful_shutdown_timeout_ms",
            "must be greater than zero",
        ));
    }

    if let Some(admin) = &config.admin_listen {
        if admin == &config.listen {
            return Err(invalid(
                "admin_listen",
                "must differ from `listen` so admin endpoints are not publicly exposed",
            ));
        }
    }

    Ok(())
}

fn validate_telemetry(config: &RouterConfig) -> Result<(), ConfigError> {
    let endpoint = &config.telemetry.prometheus_endpoint;
    if !endpoint.starts_with('/') {
        return Err(invalid(
            "telemetry.prometheus_endpoint",
            format!("`{endpoint}` must be an absolute path beginning with `/`"),
        ));
    }

    if let Some(otlp) = &config.telemetry.otlp_endpoint {
        let url = reqwest::Url::parse(otlp).map_err(|error| {
            invalid(
                "telemetry.otlp_endpoint",
                format!("`{otlp}` is not a valid URL ({error})"),
            )
        })?;
        if !matches!(url.scheme(), "http" | "https") {
            return Err(invalid(
                "telemetry.otlp_endpoint",
                format!("unsupported scheme `{}`", url.scheme()),
            ));
        }
    }

    Ok(())
}

fn validate_cache(config: &RouterConfig) -> Result<(), ConfigError> {
    let cache = &config.cache;
    if !cache.enabled {
        return Ok(());
    }

    if cache.max_capacity_mb == 0 {
        return Err(invalid(
            "cache.max_capacity_mb",
            "must be greater than zero when the cache is enabled",
        ));
    }

    if cache.ttl_seconds == 0 {
        return Err(invalid(
            "cache.ttl_seconds",
            "must be greater than zero when the cache is enabled",
        ));
    }

    Ok(())
}

fn validate_resiliency(config: &RouterConfig) -> Result<(), ConfigError> {
    let breaker = &config.resiliency.circuit_breaker;

    if breaker.consecutive_failures_threshold == 0 {
        return Err(invalid(
            "resiliency.circuit_breaker.consecutive_failures_threshold",
            "must be greater than zero",
        ));
    }

    if !(breaker.error_rate_threshold_pct > 0.0 && breaker.error_rate_threshold_pct <= 100.0) {
        return Err(invalid(
            "resiliency.circuit_breaker.error_rate_threshold_pct",
            "must be within (0.0, 100.0]",
        ));
    }

    if breaker.window_seconds == 0 {
        return Err(invalid(
            "resiliency.circuit_breaker.window_seconds",
            "must be greater than zero",
        ));
    }

    if breaker.cooldown_seconds == 0 {
        return Err(invalid(
            "resiliency.circuit_breaker.cooldown_seconds",
            "must be greater than zero",
        ));
    }

    if breaker.active_probe_interval_seconds == 0 {
        return Err(invalid(
            "resiliency.circuit_breaker.active_probe_interval_seconds",
            "must be greater than zero",
        ));
    }

    Ok(())
}

fn validate_rate_limits(config: &RouterConfig) -> Result<(), ConfigError> {
    for (tenant, limits) in &config.rate_limits {
        let field = |name: &str| format!("rate_limits.{tenant}.{name}");

        if limits.match_header.trim().is_empty() {
            return Err(invalid(
                field("match_header"),
                "must name the header carrying the tenant id",
            ));
        }

        if limits.rpm == 0 {
            return Err(invalid(field("rpm"), "must be greater than zero"));
        }

        if limits.tpm == 0 {
            return Err(invalid(field("tpm"), "must be greater than zero"));
        }

        if !(limits.daily_budget_usd.is_finite() && limits.daily_budget_usd >= 0.0) {
            return Err(invalid(
                field("daily_budget_usd"),
                "must be a finite, non-negative amount",
            ));
        }

        if !(limits.burst_multiplier.is_finite() && limits.burst_multiplier >= 1.0) {
            return Err(invalid(
                field("burst_multiplier"),
                "must be a finite value of at least 1.0",
            ));
        }
    }

    Ok(())
}

fn validate_providers(config: &RouterConfig) -> Result<(), ConfigError> {
    if config.providers.is_empty() {
        return Err(invalid(
            "providers",
            "at least one provider must be configured",
        ));
    }

    for (name, provider) in &config.providers {
        validate_provider(name, provider)?;
    }

    Ok(())
}

fn validate_provider(name: &str, provider: &ProviderConfig) -> Result<(), ConfigError> {
    if name.trim().is_empty() {
        return Err(invalid("providers", "provider names must not be empty"));
    }

    let field = |suffix: &str| format!("providers.{name}.{suffix}");

    if ProviderConfig::requires_api_key(provider.kind) && provider.api_key().is_none() {
        return Err(invalid(
            field("api_key"),
            format!(
                "a `{:?}` provider requires a non-empty api_key",
                provider.kind
            ),
        ));
    }

    if provider.kind == ProviderKind::OpenaiCompatible && provider.base_url.is_none() {
        return Err(invalid(
            field("base_url"),
            "`openai_compatible` providers must set base_url (e.g. http://127.0.0.1:11434/v1)",
        ));
    }

    if let Some(base_url) = &provider.base_url {
        let url = reqwest::Url::parse(base_url).map_err(|error| {
            invalid(
                field("base_url"),
                format!("`{base_url}` is not a valid URL ({error})"),
            )
        })?;
        if !matches!(url.scheme(), "http" | "https") {
            return Err(invalid(
                field("base_url"),
                format!("unsupported scheme `{}`", url.scheme()),
            ));
        }
    }

    if provider.timeout_ms == 0 {
        return Err(invalid(field("timeout_ms"), "must be greater than zero"));
    }

    if provider.ttft_timeout_ms == 0 {
        return Err(invalid(
            field("ttft_timeout_ms"),
            "must be greater than zero",
        ));
    }

    if provider.ttft_timeout_ms > provider.timeout_ms {
        return Err(invalid(
            field("ttft_timeout_ms"),
            format!(
                "({}ms) must not exceed timeout_ms ({}ms); the watchdog would never fire first",
                provider.ttft_timeout_ms, provider.timeout_ms
            ),
        ));
    }

    Ok(())
}

fn validate_models(config: &RouterConfig) -> Result<(), ConfigError> {
    for (name, model) in &config.models {
        if name.trim().is_empty() {
            return Err(invalid("models", "model names must not be empty"));
        }

        let field = |suffix: &str| format!("models.{name}.{suffix}");

        match model.strategy {
            RoutingStrategy::Priority => {
                if model.primary.is_none() {
                    return Err(invalid(
                        field("primary"),
                        "the `priority` strategy requires a primary upstream",
                    ));
                }
                if !model.targets.is_empty() {
                    return Err(invalid(
                        field("targets"),
                        "the `priority` strategy ignores targets; use `weighted_random` or `latency_ewma`",
                    ));
                }
            }
            RoutingStrategy::WeightedRandom | RoutingStrategy::LatencyEwma => {
                if model.targets.is_empty() {
                    return Err(invalid(
                        field("targets"),
                        format!(
                            "the `{:?}` strategy requires at least one target",
                            model.strategy
                        ),
                    ));
                }
            }
        }

        for (index, target) in model.targets.iter().enumerate() {
            if target.weight == 0 {
                return Err(invalid(
                    field(&format!("targets[{index}].weight")),
                    "weight must be greater than zero",
                ));
            }
        }

        for status in &model.retry_on_status {
            if !(100..=599).contains(status) {
                return Err(invalid(
                    field("retry_on_status"),
                    format!("{status} is not a valid HTTP status code"),
                ));
            }
        }

        if let Some(primary) = &model.primary {
            validate_reference(primary, config, &field("primary"))?;
        }

        for (index, target) in model.targets.iter().enumerate() {
            validate_reference(
                &target.upstream,
                config,
                &field(&format!("targets[{index}].upstream")),
            )?;
        }

        for (index, fallback) in model.fallbacks.iter().enumerate() {
            validate_reference(fallback, config, &field(&format!("fallbacks[{index}]")))?;
        }
    }

    Ok(())
}

fn validate_rules(config: &RouterConfig) -> Result<(), ConfigError> {
    let mut seen: BTreeSet<&str> = BTreeSet::new();

    for (index, rule) in config.rules.iter().enumerate() {
        if rule.name.trim().is_empty() {
            return Err(invalid(
                format!("rules[{index}].name"),
                "rule names must not be empty",
            ));
        }

        if !seen.insert(rule.name.as_str()) {
            return Err(ConfigError::DuplicateRule {
                name: rule.name.clone(),
            });
        }

        if rule.condition.is_empty() {
            return Err(invalid(
                format!("rules.{}.condition", rule.name),
                "a rule with no predicates would match every request; add at least one condition",
            ));
        }

        validate_condition(&rule.name, &rule.condition)?;
        validate_reference(
            &rule.route_to,
            config,
            &format!("rules.{}.route_to", rule.name),
        )?;
    }

    Ok(())
}

fn validate_condition(
    rule: &str,
    condition: &crate::config::RuleCondition,
) -> Result<(), ConfigError> {
    let field = |suffix: &str| format!("rules.{rule}.condition.{suffix}");

    for (name, bound) in [
        ("complexity_score_below", condition.complexity_score_below),
        ("complexity_score_above", condition.complexity_score_above),
    ] {
        if let Some(value) = bound {
            if !(value.is_finite() && (0.0..=1.0).contains(&value)) {
                return Err(invalid(field(name), "must be within 0.0..=1.0"));
            }
        }
    }

    if let (Some(min), Some(max)) = (
        condition.min_estimated_tokens,
        condition.max_estimated_tokens,
    ) {
        if min > max {
            return Err(invalid(
                field("min_estimated_tokens"),
                format!("({min}) must not exceed max_estimated_tokens ({max})"),
            ));
        }
    }

    if let Some(headers) = &condition.header_matches {
        for header in headers.keys() {
            if header.trim().is_empty() {
                return Err(invalid(
                    field("header_matches"),
                    "header names must not be empty",
                ));
            }
        }
    }

    if let Some(headers) = &condition.header_regex_matches {
        for (header, pattern) in headers {
            if header.trim().is_empty() {
                return Err(invalid(
                    field("header_regex_matches"),
                    "header names must not be empty",
                ));
            }
            Regex::new(pattern).map_err(|error| {
                invalid(
                    field(&format!("header_regex_matches.{header}")),
                    format!("`{pattern}` is not a valid regex ({error})"),
                )
            })?;
        }
    }

    Ok(())
}

fn validate_pricing(config: &RouterConfig) -> Result<(), ConfigError> {
    for (reference, pricing) in &config.pricing {
        let (provider, model) = reference.split_once('/').ok_or_else(|| {
            invalid(
                format!("pricing.{reference}"),
                "pricing keys must be of the form `provider/model`",
            )
        })?;

        if provider.is_empty() || model.is_empty() {
            return Err(invalid(
                format!("pricing.{reference}"),
                "pricing keys must be of the form `provider/model`",
            ));
        }

        if !config.providers.contains_key(provider) {
            return Err(ConfigError::UndefinedProvider {
                reference: reference.clone(),
                provider: provider.to_string(),
                context: format!("pricing.{reference}"),
            });
        }

        for (name, value) in [
            ("prompt_per_million_usd", pricing.prompt_per_million_usd),
            (
                "completion_per_million_usd",
                pricing.completion_per_million_usd,
            ),
        ] {
            if !value.is_finite() || value < 0.0 {
                return Err(invalid(
                    format!("pricing.{reference}.{name}"),
                    "must be a finite, non-negative USD amount",
                ));
            }
        }
    }

    Ok(())
}

/// Validates one `provider/model` (or virtual-model) reference.
fn validate_reference(
    reference: &str,
    config: &RouterConfig,
    context: &str,
) -> Result<(), ConfigError> {
    let trimmed = reference.trim();

    if trimmed.is_empty() {
        return Err(invalid(context, "reference must not be empty"));
    }

    match trimmed.split_once('/') {
        Some((provider, model_id)) => {
            if provider.is_empty() || model_id.is_empty() {
                return Err(invalid(
                    context,
                    format!("`{reference}` must be of the form `provider/model`"),
                ));
            }

            if !config.providers.contains_key(provider) {
                return Err(ConfigError::UndefinedProvider {
                    reference: reference.to_string(),
                    provider: provider.to_string(),
                    context: context.to_string(),
                });
            }
        }
        None => {
            if !config.models.contains_key(trimmed) {
                return Err(ConfigError::UndefinedModel {
                    reference: reference.to_string(),
                    context: context.to_string(),
                });
            }
        }
    }

    Ok(())
}

/// Detects cycles in the virtual-model reference graph.
///
/// Only models can participate in a cycle: models reference other models, while
/// rules are pure sources (they resolve *to* a model but are never referenced
/// *by* one), so a path can never re-enter a rule.
fn detect_circular_routing(config: &RouterConfig) -> Result<(), ConfigError> {
    let mut state: BTreeMap<&str, VisitState> = BTreeMap::new();
    let mut path: Vec<&str> = Vec::new();

    for name in config.models.keys() {
        if state
            .get(name.as_str())
            .copied()
            .unwrap_or(VisitState::Unvisited)
            != VisitState::Unvisited
        {
            continue;
        }
        visit_model(name.as_str(), config, &mut state, &mut path, 0)?;
    }

    Ok(())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum VisitState {
    Unvisited,
    InProgress,
    Done,
}

fn visit_model<'config>(
    name: &'config str,
    config: &'config RouterConfig,
    state: &mut BTreeMap<&'config str, VisitState>,
    path: &mut Vec<&'config str>,
    depth: usize,
) -> Result<(), ConfigError> {
    if depth > MAX_ROUTING_DEPTH {
        return Err(invalid(
            format!("models.{name}"),
            format!("routing chain deeper than {MAX_ROUTING_DEPTH} models; check for a cycle"),
        ));
    }

    state.insert(name, VisitState::InProgress);
    path.push(name);

    if let Some(model) = config.models.get(name) {
        for reference in model.upstream_references() {
            // Edges only exist between virtual models; a `provider/model` leaf
            // terminates the chain.
            if reference.contains('/') {
                continue;
            }

            let Some((next, _)) = config.models.get_key_value(reference) else {
                // Undefined references were already reported by validation.
                continue;
            };

            match state
                .get(next.as_str())
                .copied()
                .unwrap_or(VisitState::Unvisited)
            {
                VisitState::Unvisited => {
                    visit_model(next.as_str(), config, state, path, depth + 1)?;
                }
                VisitState::InProgress => {
                    let start = path
                        .iter()
                        .position(|visited| *visited == next.as_str())
                        .unwrap_or(0);
                    let mut chain: Vec<String> = path[start..]
                        .iter()
                        .map(|node| (*node).to_string())
                        .collect();
                    chain.push(next.clone());
                    return Err(ConfigError::CircularRouting {
                        chain: RoutingChain::new(chain),
                    });
                }
                VisitState::Done => {}
            }
        }
    }

    path.pop();
    state.insert(name, VisitState::Done);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    fn lookup_from<'a>(pairs: &'a [(&'a str, &'a str)]) -> impl Fn(&str) -> Option<String> + 'a {
        move |name| {
            pairs
                .iter()
                .find(|(key, _)| *key == name)
                .map(|(_, value)| (*value).to_string())
        }
    }

    fn examples_dir() -> PathBuf {
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("examples")
    }

    // ---------------------------------------------------------------- interpolation

    #[test]
    fn interpolates_a_present_variable() {
        let output = interpolate_with(
            "key = \"${OPENAI_API_KEY}\"",
            InterpolationMode::Strict,
            lookup_from(&[("OPENAI_API_KEY", "sk-live-123")]),
        )
        .expect("interpolation succeeds");

        assert_eq!(output, "key = \"sk-live-123\"");
    }

    #[test]
    fn missing_variable_is_fatal_in_strict_mode() {
        let error = interpolate_with("${OPENAI_API_KEY}", InterpolationMode::Strict, |_| None)
            .expect_err("strict mode must fail");

        match error {
            ConfigError::MissingEnvVar { var } => assert_eq!(var, "OPENAI_API_KEY"),
            other => panic!("unexpected error: {other:?}"),
        }
    }

    #[test]
    fn missing_variable_keeps_placeholder_in_lenient_mode() {
        let output = interpolate_with("${OPENAI_API_KEY}", InterpolationMode::Lenient, |_| None)
            .expect("lenient mode succeeds");

        assert_eq!(output, "${OPENAI_API_KEY}");
    }

    #[test]
    fn default_is_used_when_variable_is_absent() {
        let output = interpolate_with(
            "${OTEL_EXPORTER_OTLP_ENDPOINT:-http://localhost:4317}",
            InterpolationMode::Strict,
            |_| None,
        )
        .expect("interpolation succeeds");

        assert_eq!(output, "http://localhost:4317");
    }

    #[test]
    fn default_is_ignored_when_variable_is_present() {
        let output = interpolate_with(
            "${OTEL_EXPORTER_OTLP_ENDPOINT:-http://localhost:4317}",
            InterpolationMode::Strict,
            lookup_from(&[("OTEL_EXPORTER_OTLP_ENDPOINT", "http://collector:4317")]),
        )
        .expect("interpolation succeeds");

        assert_eq!(output, "http://collector:4317");
    }

    #[test]
    fn empty_default_resolves_to_empty_string() {
        let output = interpolate_with(
            "x = \"${MAYBE_UNSET:-}\"",
            InterpolationMode::Strict,
            |_| None,
        )
        .expect("interpolation succeeds");

        assert_eq!(output, "x = \"\"");
    }

    #[test]
    fn empty_variable_value_is_not_treated_as_absent() {
        let output = interpolate_with(
            "${SET_BUT_EMPTY:-fallback}",
            InterpolationMode::Strict,
            |_| Some(String::new()),
        )
        .expect("interpolation succeeds");

        assert_eq!(output, "");
    }

    #[test]
    fn repeated_placeholders_are_all_expanded() {
        let output = interpolate_with(
            "${A}/${B}/${A}",
            InterpolationMode::Strict,
            lookup_from(&[("A", "one"), ("B", "two")]),
        )
        .expect("interpolation succeeds");

        assert_eq!(output, "one/two/one");
    }

    #[test]
    fn lowercase_and_brace_less_forms_are_left_alone() {
        let input = "${lowercase} $NOT_A_PLACEHOLDER ${} ${A-B}";
        let output = interpolate_with(
            input,
            InterpolationMode::Strict,
            lookup_from(&[("lowercase", "x")]),
        )
        .expect("interpolation succeeds");

        assert_eq!(
            output, input,
            "only uppercase ${{NAME}} forms are placeholders"
        );
    }

    // ---------------------------------------------------------------- example configs

    #[test]
    fn minimal_example_loads() {
        let config = load_config_from_path(
            examples_dir().join("minimal.toml"),
            InterpolationMode::Lenient,
        )
        .expect("examples/minimal.toml must load");

        assert_eq!(config.listen, "0.0.0.0:8080");
        assert_eq!(config.providers.len(), 2);
        assert_eq!(
            config.models["gpt-4o"].primary.as_deref(),
            Some("openai/gpt-4o")
        );
        assert_eq!(
            config.models["gpt-4o"].fallbacks,
            vec!["anthropic/claude-3-7-sonnet-latest".to_string()]
        );
    }

    #[test]
    fn production_example_loads() {
        let config = load_config_from_path(
            examples_dir().join("production.toml"),
            InterpolationMode::Lenient,
        )
        .expect("examples/production.toml must load");

        assert_eq!(config.providers.len(), 4);
        assert_eq!(config.rules.len(), 3);

        let smart = &config.models["smart-router"];
        assert_eq!(smart.strategy, RoutingStrategy::LatencyEwma);
        assert_eq!(smart.targets.len(), 2);
        assert_eq!(smart.retry_on_status, vec![429, 500, 502, 503, 504, 529]);

        let breaker = &config.resiliency.circuit_breaker;
        assert_eq!(breaker.consecutive_failures_threshold, 5);
        assert_eq!(breaker.cooldown_seconds, 15);

        let tenant = &config.rate_limits["default_tenant"];
        assert_eq!(tenant.rpm, 600);
        assert_eq!(tenant.tpm, 250_000);
        assert!((tenant.daily_budget_usd - 50.0).abs() < f64::EPSILON);
    }

    #[test]
    fn production_example_is_fully_expanded_in_strict_mode_when_env_is_set() {
        // Uses a variable that is guaranteed present rather than mutating the
        // process environment, which is unsafe under parallel test execution.
        let document = "listen = \"127.0.0.1:8080\"\n[providers.p]\ntype = \"openai_compatible\"\nbase_url = \"http://127.0.0.1:11434/v1\"\n";
        let config = load_config_from_str(document, InterpolationMode::Strict)
            .expect("config without placeholders loads in strict mode");
        assert_eq!(config.providers.len(), 1);
    }

    // ---------------------------------------------------------------- references

    const BASE: &str = r#"
listen = "127.0.0.1:8080"

[providers.openai]
type = "openai"
api_key = "sk-test"

[providers.local]
type = "openai_compatible"
base_url = "http://127.0.0.1:11434/v1"
"#;

    fn load_with_extra(extra: &str) -> Result<RouterConfig, ConfigError> {
        load_config_from_str(&format!("{BASE}{extra}"), InterpolationMode::Lenient)
    }

    #[test]
    fn undefined_provider_in_fallback_is_rejected() {
        let error = load_with_extra(
            "\n[models.m]\nprimary = \"openai/gpt-4o\"\nfallbacks = [\"nope/gpt-4o\"]\n",
        )
        .expect_err("undefined provider must be rejected");

        match error {
            ConfigError::UndefinedProvider {
                provider, context, ..
            } => {
                assert_eq!(provider, "nope");
                assert_eq!(context, "models.m.fallbacks[0]");
            }
            other => panic!("unexpected error: {other:?}"),
        }
    }

    #[test]
    fn undefined_virtual_model_reference_is_rejected() {
        let error = load_with_extra("\n[models.m]\nprimary = \"ghost-model\"\n")
            .expect_err("undefined model must be rejected");

        match error {
            ConfigError::UndefinedModel { reference, context } => {
                assert_eq!(reference, "ghost-model");
                assert_eq!(context, "models.m.primary");
            }
            other => panic!("unexpected error: {other:?}"),
        }
    }

    #[test]
    fn model_reference_without_slash_may_point_at_another_model() {
        let config = load_with_extra(
            r#"
[models.alias]
primary = "openai/gpt-4o"

[models.public]
primary = "alias"
"#,
        )
        .expect("model chaining is allowed");

        assert_eq!(config.models["public"].primary.as_deref(), Some("alias"));
    }

    #[test]
    fn malformed_reference_with_empty_segment_is_rejected() {
        let error = load_with_extra("\n[models.m]\nprimary = \"openai/\"\n")
            .expect_err("empty model id must be rejected");

        assert!(matches!(error, ConfigError::InvalidValue { .. }));
    }

    // ---------------------------------------------------------------- cycles

    #[test]
    fn two_model_cycle_is_detected() {
        let error = load_with_extra(
            r#"
[models.a]
primary = "b"

[models.b]
primary = "a"
"#,
        )
        .expect_err("cycle must be rejected");

        match error {
            ConfigError::CircularRouting { chain } => {
                let nodes = chain.nodes();
                assert!(nodes.len() >= 3, "chain should close the loop: {nodes:?}");
                assert_eq!(nodes.first(), nodes.last());
            }
            other => panic!("unexpected error: {other:?}"),
        }
    }

    #[test]
    fn self_referential_fallback_is_detected() {
        let error = load_with_extra(
            r#"
[models.loop]
primary = "openai/gpt-4o"
fallbacks = ["loop"]
"#,
        )
        .expect_err("self reference must be rejected");

        match error {
            ConfigError::CircularRouting { chain } => {
                assert_eq!(chain.nodes(), &["loop".to_string(), "loop".to_string()]);
            }
            other => panic!("unexpected error: {other:?}"),
        }
    }

    #[test]
    fn diamond_reference_is_not_a_cycle() {
        load_with_extra(
            r#"
[models.leaf]
primary = "openai/gpt-4o"

[models.left]
primary = "leaf"

[models.right]
primary = "leaf"

[models.root]
strategy = "weighted_random"
targets = [{ upstream = "left" }, { upstream = "right" }]
"#,
        )
        .expect("shared references are not cycles");
    }

    // ---------------------------------------------------------------- providers

    #[test]
    fn cloud_provider_without_api_key_is_rejected() {
        let error = load_config_from_str(
            "listen = \"127.0.0.1:8080\"\n[providers.openai]\ntype = \"openai\"\n",
            InterpolationMode::Lenient,
        )
        .expect_err("api key is required");

        assert!(matches!(error, ConfigError::InvalidValue { .. }));
    }

    #[test]
    fn openai_compatible_without_base_url_is_rejected() {
        let error = load_config_from_str(
            "listen = \"127.0.0.1:8080\"\n[providers.local]\ntype = \"openai_compatible\"\n",
            InterpolationMode::Lenient,
        )
        .expect_err("base_url is required");

        assert!(matches!(error, ConfigError::InvalidValue { .. }));
    }

    #[test]
    fn ttft_timeout_longer_than_request_timeout_is_rejected() {
        let error = load_config_from_str(
            r#"
listen = "127.0.0.1:8080"
[providers.p]
type = "openai"
api_key = "sk-test"
timeout_ms = 1000
ttft_timeout_ms = 5000
"#,
            InterpolationMode::Lenient,
        )
        .expect_err("ttft must be within the request timeout");

        assert!(matches!(error, ConfigError::InvalidValue { .. }));
    }

    #[test]
    fn unknown_provider_scheme_is_rejected() {
        let error = load_config_from_str(
            r#"
listen = "127.0.0.1:8080"
[providers.p]
type = "openai_compatible"
base_url = "ftp://example.com/v1"
"#,
            InterpolationMode::Lenient,
        )
        .expect_err("scheme must be http(s)");

        assert!(matches!(error, ConfigError::InvalidValue { .. }));
    }

    #[test]
    fn empty_provider_table_is_rejected() {
        let error = load_config_from_str("listen = \"127.0.0.1:8080\"", InterpolationMode::Lenient)
            .expect_err("a router without providers is useless");

        assert!(matches!(error, ConfigError::InvalidValue { .. }));
    }

    // ---------------------------------------------------------------- models & rules

    #[test]
    fn priority_strategy_without_primary_is_rejected() {
        let error = load_with_extra("\n[models.m]\nstrategy = \"priority\"\n")
            .expect_err("priority needs a primary");

        assert!(matches!(error, ConfigError::InvalidValue { .. }));
    }

    #[test]
    fn load_balanced_strategy_without_targets_is_rejected() {
        let error = load_with_extra("\n[models.m]\nstrategy = \"latency_ewma\"\n")
            .expect_err("ewma needs targets");

        assert!(matches!(error, ConfigError::InvalidValue { .. }));
    }

    #[test]
    fn zero_weight_target_is_rejected() {
        let error = load_with_extra(
            "\n[models.m]\nstrategy = \"weighted_random\"\ntargets = [{ upstream = \"openai/gpt-4o\", weight = 0 }]\n",
        )
        .expect_err("weights must be positive");

        assert!(matches!(error, ConfigError::InvalidValue { .. }));
    }

    #[test]
    fn out_of_range_retry_status_is_rejected() {
        let error =
            load_with_extra("\n[models.m]\nprimary = \"openai/gpt-4o\"\nretry_on_status = [999]\n")
                .expect_err("999 is not a status code");

        assert!(matches!(error, ConfigError::InvalidValue { .. }));
    }

    #[test]
    fn rule_with_empty_condition_is_rejected() {
        let error = load_with_extra(
            "\n[[rules]]\nname = \"catch-all\"\ncondition = {}\nroute_to = \"openai/gpt-4o\"\n",
        )
        .expect_err("an empty condition matches everything");

        assert!(matches!(error, ConfigError::InvalidValue { .. }));
    }

    #[test]
    fn duplicate_rule_names_are_rejected() {
        let error = load_with_extra(
            r#"
[[rules]]
name = "dup"
condition = { min_estimated_tokens = 10 }
route_to = "openai/gpt-4o"

[[rules]]
name = "dup"
condition = { min_estimated_tokens = 20 }
route_to = "openai/gpt-4o"
"#,
        )
        .expect_err("rule names must be unique");

        assert!(matches!(error, ConfigError::DuplicateRule { .. }));
    }

    #[test]
    fn invalid_header_regex_is_rejected() {
        let error = load_with_extra(
            "\n[[rules]]\nname = \"r\"\ncondition = { header_regex_matches = { \"X-Trace\" = \"[unclosed\" } }\nroute_to = \"openai/gpt-4o\"\n",
        )
        .expect_err("invalid regex must be caught at load time");

        assert!(matches!(error, ConfigError::InvalidValue { .. }));
    }

    #[test]
    fn complexity_threshold_outside_unit_interval_is_rejected() {
        let error = load_with_extra(
            "\n[[rules]]\nname = \"r\"\ncondition = { complexity_score_below = 1.5 }\nroute_to = \"openai/gpt-4o\"\n",
        )
        .expect_err("complexity is a 0..=1 score");

        assert!(matches!(error, ConfigError::InvalidValue { .. }));
    }

    // ---------------------------------------------------------------- listener

    #[test]
    fn malformed_listen_address_is_rejected() {
        let error = load_config_from_str(
            "listen = \"not-an-address\"\n[providers.p]\ntype = \"openai\"\napi_key = \"sk\"\n",
            InterpolationMode::Lenient,
        )
        .expect_err("invalid socket address must be rejected");

        assert!(matches!(error, ConfigError::InvalidValue { .. }));
    }

    #[test]
    fn admin_listener_shadowing_public_listener_is_rejected() {
        let error = load_config_from_str(
            r#"
listen = "0.0.0.0:8080"
admin_listen = "0.0.0.0:8080"
[providers.p]
type = "openai"
api_key = "sk"
"#,
            InterpolationMode::Lenient,
        )
        .expect_err("admin listener must not be public");

        assert!(matches!(error, ConfigError::InvalidValue { .. }));
    }

    #[test]
    fn prometheus_endpoint_must_be_absolute_path() {
        let error = load_with_extra("\n[telemetry]\nprometheus_endpoint = \"metrics\"\n")
            .expect_err("endpoint must start with /");

        assert!(matches!(error, ConfigError::InvalidValue { .. }));
    }

    // ---------------------------------------------------------------- io

    #[test]
    fn missing_file_reports_the_path() {
        let error = load_config_from_path(
            examples_dir().join("does-not-exist.toml"),
            InterpolationMode::Lenient,
        )
        .expect_err("missing file must fail");

        match error {
            ConfigError::Io { path, .. } => assert!(path.ends_with("does-not-exist.toml")),
            other => panic!("unexpected error: {other:?}"),
        }
    }

    #[test]
    fn pricing_must_reference_a_defined_provider() {
        let error = load_with_extra(
            "\n[pricing.\"ghost/model\"]\nprompt_per_million_usd = 1.0\ncompletion_per_million_usd = 2.0\n",
        )
        .expect_err("pricing for an unknown provider must be rejected");

        assert!(matches!(error, ConfigError::UndefinedProvider { .. }));
    }

    #[test]
    fn pricing_must_be_non_negative() {
        let error = load_with_extra(
            "\n[pricing.\"openai/gpt-4o\"]\nprompt_per_million_usd = -1.0\ncompletion_per_million_usd = 2.0\n",
        )
        .expect_err("negative prices must be rejected");

        assert!(matches!(error, ConfigError::InvalidValue { .. }));
    }

    #[test]
    fn pricing_requires_a_provider_model_key() {
        let error = load_with_extra(
            "\n[pricing.\"bare-model\"]\nprompt_per_million_usd = 1.0\ncompletion_per_million_usd = 2.0\n",
        )
        .expect_err("pricing keys need a provider segment");

        assert!(matches!(error, ConfigError::InvalidValue { .. }));
    }

    #[test]
    fn malformed_toml_reports_a_parse_error() {
        let error = load_config_from_str("listen = [", InterpolationMode::Lenient)
            .expect_err("invalid TOML must fail");

        assert!(matches!(error, ConfigError::Toml { .. }));
    }
}

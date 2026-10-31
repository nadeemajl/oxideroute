//! Rule evaluation and virtual-model resolution.
//!
//! Resolution happens entirely against an `Arc<RouterConfig>` snapshot, so a
//! reload mid-request cannot change which upstreams a request may use.
//!
//! The output is an ordered candidate list: the executor walks it from the
//! front, failing over to later entries.

use std::sync::Arc;

use dashmap::DashMap;
use rand::Rng;

use crate::config::{RouterConfig, RoutingStrategy, RuleCondition, WeightedTarget};
use crate::error::RouterError;
use crate::proxy::inspector::ProxyRequest;
use crate::state::{now_ms, UpstreamHealthMetrics};

/// Status codes worth failing over on when a model does not specify its own list.
pub const DEFAULT_RETRY_STATUSES: &[u16] = &[429, 500, 502, 503, 504, 529];

/// Maximum model-chaining depth before we conclude the config is pathological.
///
/// Validation already rejects cycles; this bounds generated configs that are
/// merely enormous.
const MAX_CHAIN_DEPTH: usize = 32;

/// One concrete upstream to attempt.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Candidate {
    /// Provider config key.
    pub provider: String,
    /// Upstream model id (everything after the first `/`).
    pub model: String,
}

impl Candidate {
    /// Parses a `provider/model` reference.
    ///
    /// Splits on the *first* slash so model ids containing slashes (common for
    /// OpenRouter-style names) survive intact.
    pub fn parse(reference: &str) -> Option<Self> {
        let (provider, model) = reference.split_once('/')?;
        if provider.is_empty() || model.is_empty() {
            return None;
        }
        Some(Self {
            provider: provider.to_string(),
            model: model.to_string(),
        })
    }

    /// Renders back to `provider/model`, the key used for health metrics.
    pub fn reference(&self) -> String {
        format!("{}/{}", self.provider, self.model)
    }
}

/// The ordered plan for one request.
#[derive(Debug, Clone)]
pub struct Resolution {
    /// Upstreams to try, in order.
    pub candidates: Vec<Candidate>,
    /// Upstream statuses that should trigger a failover rather than a hard error.
    pub retry_on_status: Vec<u16>,
}

/// Resolves a request into an ordered candidate list.
///
/// Rules are evaluated first (lowest `priority` wins, ties broken by
/// declaration order); if none match, the requested virtual model is expanded
/// according to its strategy.
///
/// # Errors
///
/// Returns [`RouterError::InvalidRequest`] when the model is unknown or a
/// reference cannot be parsed.
pub fn resolve_candidates(
    config: &RouterConfig,
    request: &ProxyRequest,
    health: &DashMap<String, Arc<UpstreamHealthMetrics>>,
) -> Result<Resolution, RouterError> {
    let mut candidates = Vec::new();

    if let Some(rule) = matching_rule(config, request) {
        expand_reference(&rule.route_to, config, health, 0, &mut candidates)?;

        // A rule may point at a virtual model, in which case that model's retry
        // policy is the relevant one.
        let retry_on_status = retry_statuses_for(config, Some(&rule.route_to))
            .or_else(|| retry_statuses_for(config, Some(&request.requested_model)))
            .unwrap_or_else(|| DEFAULT_RETRY_STATUSES.to_vec());

        return Ok(Resolution {
            candidates: dedupe(candidates),
            retry_on_status,
        });
    }

    if !config.models.contains_key(&request.requested_model) {
        return Err(RouterError::InvalidRequest {
            message: format!("unknown model `{}`", request.requested_model),
        });
    }

    expand_model(&request.requested_model, config, health, 0, &mut candidates)?;

    let retry_on_status = retry_statuses_for(config, Some(&request.requested_model))
        .unwrap_or_else(|| DEFAULT_RETRY_STATUSES.to_vec());

    Ok(Resolution {
        candidates: dedupe(candidates),
        retry_on_status,
    })
}

fn retry_statuses_for(config: &RouterConfig, name: Option<&str>) -> Option<Vec<u16>> {
    let model = config.models.get(name?)?;
    if model.retry_on_status.is_empty() {
        return None;
    }
    Some(model.retry_on_status.clone())
}

fn expand_model(
    name: &str,
    config: &RouterConfig,
    health: &DashMap<String, Arc<UpstreamHealthMetrics>>,
    depth: usize,
    out: &mut Vec<Candidate>,
) -> Result<(), RouterError> {
    if depth > MAX_CHAIN_DEPTH {
        return Err(RouterError::Internal {
            message: format!("model chain deeper than {MAX_CHAIN_DEPTH} while resolving `{name}`"),
        });
    }

    let Some(model) = config.models.get(name) else {
        return Err(RouterError::InvalidRequest {
            message: format!("unknown model `{name}`"),
        });
    };

    match model.strategy {
        RoutingStrategy::Priority => {
            if let Some(primary) = &model.primary {
                expand_reference(primary, config, health, depth + 1, out)?;
            }
        }
        RoutingStrategy::WeightedRandom => {
            if let Some(target) = sample_target(&model.targets) {
                expand_reference(&target.upstream, config, health, depth + 1, out)?;
            }
        }
        RoutingStrategy::LatencyEwma => {
            for target in ordered_by_latency(&model.targets, health) {
                expand_reference(&target.upstream, config, health, depth + 1, out)?;
            }
        }
    }

    for fallback in &model.fallbacks {
        expand_reference(fallback, config, health, depth + 1, out)?;
    }

    Ok(())
}

fn expand_reference(
    reference: &str,
    config: &RouterConfig,
    health: &DashMap<String, Arc<UpstreamHealthMetrics>>,
    depth: usize,
    out: &mut Vec<Candidate>,
) -> Result<(), RouterError> {
    if reference.contains('/') {
        let candidate = Candidate::parse(reference).ok_or_else(|| RouterError::InvalidRequest {
            message: format!("`{reference}` is not a valid `provider/model` reference"),
        })?;
        out.push(candidate);
        return Ok(());
    }

    // A bare name refers to another virtual model.
    expand_model(reference, config, health, depth, out)
}

/// Orders targets healthiest-first, then by lowest observed TTFT.
///
/// An unobserved EWMA (`0`) sorts last: a target nobody has measured should not
/// win purely because it has no datapoint.
fn ordered_by_latency<'a>(
    targets: &'a [WeightedTarget],
    health: &DashMap<String, Arc<UpstreamHealthMetrics>>,
) -> Vec<&'a WeightedTarget> {
    let at = now_ms();

    let mut ordered: Vec<&WeightedTarget> = targets.iter().collect();
    ordered.sort_by_key(|target| {
        let metrics = health
            .get(&target.upstream)
            .map(|entry| Arc::clone(entry.value()));

        match metrics {
            Some(metrics) => {
                let healthy = u8::from(!metrics.is_healthy(at));
                let latency = match metrics.ewma_ttft_us() {
                    0 => u64::MAX,
                    observed => observed,
                };
                (healthy, latency)
            }
            None => (0, u64::MAX),
        }
    });

    ordered
}

fn sample_target(targets: &[WeightedTarget]) -> Option<&WeightedTarget> {
    let total: u64 = targets.iter().map(|target| u64::from(target.weight)).sum();
    if total == 0 {
        return None;
    }

    let mut roll = rand::thread_rng().gen_range(0..total);
    for target in targets {
        let weight = u64::from(target.weight);
        if roll < weight {
            return Some(target);
        }
        roll -= weight;
    }

    targets.last()
}

fn matching_rule<'a>(
    config: &'a RouterConfig,
    request: &ProxyRequest,
) -> Option<&'a crate::config::RoutingRule> {
    let mut ordered: Vec<(u32, usize, &crate::config::RoutingRule)> = config
        .rules
        .iter()
        .enumerate()
        .map(|(index, rule)| (rule.priority.unwrap_or(u32::MAX), index, rule))
        .collect();

    ordered.sort_by_key(|(priority, index, _)| (*priority, *index));

    ordered
        .into_iter()
        .find(|(_, _, rule)| condition_matches(&rule.condition, request))
        .map(|(_, _, rule)| rule)
}

/// Every populated predicate must match (logical AND).
fn condition_matches(condition: &RuleCondition, request: &ProxyRequest) -> bool {
    if let Some(model) = &condition.model {
        if model != &request.requested_model {
            return false;
        }
    }

    if let Some(minimum) = condition.min_estimated_tokens {
        if request.estimated_prompt_tokens < minimum {
            return false;
        }
    }

    if let Some(maximum) = condition.max_estimated_tokens {
        if request.estimated_prompt_tokens > maximum {
            return false;
        }
    }

    if let Some(bound) = condition.complexity_score_below {
        if request.complexity_score >= bound {
            return false;
        }
    }

    if let Some(bound) = condition.complexity_score_above {
        if request.complexity_score <= bound {
            return false;
        }
    }

    if let Some(expected) = &condition.header_matches {
        for (name, value) in expected {
            let actual = request
                .headers
                .get(name.as_str())
                .and_then(|header| header.to_str().ok());
            if actual != Some(value.as_str()) {
                return false;
            }
        }
    }

    if let Some(patterns) = &condition.header_regex_matches {
        for (name, pattern) in patterns {
            let Some(actual) = request
                .headers
                .get(name.as_str())
                .and_then(|header| header.to_str().ok())
            else {
                return false;
            };

            // Compiled per evaluation: rules with regex conditions are rare, and
            // caching is a later optimisation rather than a correctness concern.
            match regex::Regex::new(pattern) {
                Ok(regex) if regex.is_match(actual) => {}
                Ok(_) => return false,
                Err(_) => return false,
            }
        }
    }

    true
}

fn dedupe(candidates: Vec<Candidate>) -> Vec<Candidate> {
    let mut seen = std::collections::HashSet::new();
    candidates
        .into_iter()
        .filter(|candidate| seen.insert(candidate.clone()))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{ProviderConfig, ProviderKind, RoutingRule, VirtualModelConfig};
    use crate::proxy::inspector::inspect_request;
    use bytes::Bytes;
    use http::HeaderMap;

    fn config_with_provider() -> RouterConfig {
        let mut config = RouterConfig::default();
        config.providers.insert(
            "p".to_string(),
            ProviderConfig {
                kind: ProviderKind::OpenaiCompatible,
                api_key: None,
                base_url: Some("http://127.0.0.1:11434/v1".to_string()),
                timeout_ms: 1_000,
                ttft_timeout_ms: 500,
            },
        );
        config
    }

    fn model(primary: &str, fallbacks: &[&str]) -> VirtualModelConfig {
        VirtualModelConfig {
            primary: Some(primary.to_string()),
            strategy: RoutingStrategy::Priority,
            targets: Vec::new(),
            fallbacks: fallbacks.iter().map(|item| item.to_string()).collect(),
            retry_on_status: Vec::new(),
        }
    }

    fn request(json: serde_json::Value) -> ProxyRequest {
        inspect_request(HeaderMap::new(), Bytes::from(json.to_string())).expect("inspects")
    }

    fn health() -> DashMap<String, Arc<UpstreamHealthMetrics>> {
        DashMap::new()
    }

    #[test]
    fn priority_orders_primary_then_fallbacks() {
        let mut config = config_with_provider();
        config
            .models
            .insert("m".to_string(), model("p/a", &["p/b", "p/c"]));

        let resolution = resolve_candidates(
            &config,
            &request(serde_json::json!({"model": "m"})),
            &health(),
        )
        .expect("resolves");

        let references: Vec<String> = resolution
            .candidates
            .iter()
            .map(Candidate::reference)
            .collect();
        assert_eq!(references, vec!["p/a", "p/b", "p/c"]);
    }

    #[test]
    fn model_ids_may_contain_slashes() {
        let mut config = config_with_provider();
        config
            .models
            .insert("m".to_string(), model("p/meta-llama/Llama-3-70b", &[]));

        let resolution = resolve_candidates(
            &config,
            &request(serde_json::json!({"model": "m"})),
            &health(),
        )
        .expect("resolves");

        assert_eq!(resolution.candidates[0].provider, "p");
        assert_eq!(resolution.candidates[0].model, "meta-llama/Llama-3-70b");
    }

    #[test]
    fn model_chaining_is_expanded() {
        let mut config = config_with_provider();
        config.models.insert("alias".to_string(), model("p/a", &[]));
        config
            .models
            .insert("m".to_string(), model("alias", &["p/z"]));

        let resolution = resolve_candidates(
            &config,
            &request(serde_json::json!({"model": "m"})),
            &health(),
        )
        .expect("resolves");

        let references: Vec<String> = resolution
            .candidates
            .iter()
            .map(Candidate::reference)
            .collect();
        assert_eq!(references, vec!["p/a", "p/z"]);
    }

    #[test]
    fn duplicate_candidates_are_collapsed() {
        let mut config = config_with_provider();
        config
            .models
            .insert("m".to_string(), model("p/a", &["p/a", "p/b"]));

        let resolution = resolve_candidates(
            &config,
            &request(serde_json::json!({"model": "m"})),
            &health(),
        )
        .expect("resolves");

        assert_eq!(resolution.candidates.len(), 2);
    }

    #[test]
    fn unknown_model_is_rejected() {
        let config = config_with_provider();
        let error = resolve_candidates(
            &config,
            &request(serde_json::json!({"model": "nope"})),
            &health(),
        )
        .expect_err("unknown model");

        assert!(matches!(error, RouterError::InvalidRequest { .. }));
    }

    #[test]
    fn ewma_orders_the_faster_target_first() {
        let mut config = config_with_provider();
        config.models.insert(
            "m".to_string(),
            VirtualModelConfig {
                primary: None,
                strategy: RoutingStrategy::LatencyEwma,
                targets: vec![
                    WeightedTarget {
                        upstream: "p/slow".to_string(),
                        weight: 1,
                    },
                    WeightedTarget {
                        upstream: "p/fast".to_string(),
                        weight: 1,
                    },
                ],
                fallbacks: vec!["p/backup".to_string()],
                retry_on_status: Vec::new(),
            },
        );

        let health = health();
        health.insert("p/slow".to_string(), Arc::new(UpstreamHealthMetrics::new()));
        health
            .get("p/slow")
            .expect("present")
            .record_success(std::time::Duration::from_millis(120));

        let fast = Arc::new(UpstreamHealthMetrics::new());
        fast.record_success(std::time::Duration::from_millis(10));
        health.insert("p/fast".to_string(), fast);

        let resolution = resolve_candidates(
            &config,
            &request(serde_json::json!({"model": "m"})),
            &health,
        )
        .expect("resolves");

        let references: Vec<String> = resolution
            .candidates
            .iter()
            .map(Candidate::reference)
            .collect();
        assert_eq!(references, vec!["p/fast", "p/slow", "p/backup"]);
    }

    #[test]
    fn unhealthy_targets_sort_last() {
        let mut config = config_with_provider();
        config.models.insert(
            "m".to_string(),
            VirtualModelConfig {
                primary: None,
                strategy: RoutingStrategy::LatencyEwma,
                targets: vec![
                    WeightedTarget {
                        upstream: "p/fast".to_string(),
                        weight: 1,
                    },
                    WeightedTarget {
                        upstream: "p/slow".to_string(),
                        weight: 1,
                    },
                ],
                fallbacks: Vec::new(),
                retry_on_status: Vec::new(),
            },
        );

        let health = health();
        let fast = Arc::new(UpstreamHealthMetrics::new());
        fast.record_success(std::time::Duration::from_millis(5));
        for _ in 0..3 {
            fast.record_failure(1, std::time::Duration::from_secs(60));
        }
        health.insert("p/fast".to_string(), fast);

        let slow = Arc::new(UpstreamHealthMetrics::new());
        slow.record_success(std::time::Duration::from_millis(500));
        health.insert("p/slow".to_string(), slow);

        let resolution = resolve_candidates(
            &config,
            &request(serde_json::json!({"model": "m"})),
            &health,
        )
        .expect("resolves");

        assert_eq!(resolution.candidates[0].reference(), "p/slow");
    }

    #[test]
    fn weighted_random_always_produces_one_leading_candidate() {
        let mut config = config_with_provider();
        config.models.insert(
            "m".to_string(),
            VirtualModelConfig {
                primary: None,
                strategy: RoutingStrategy::WeightedRandom,
                targets: vec![
                    WeightedTarget {
                        upstream: "p/a".to_string(),
                        weight: 70,
                    },
                    WeightedTarget {
                        upstream: "p/b".to_string(),
                        weight: 30,
                    },
                ],
                fallbacks: vec!["p/z".to_string()],
                retry_on_status: Vec::new(),
            },
        );

        for _ in 0..50 {
            let resolution = resolve_candidates(
                &config,
                &request(serde_json::json!({"model": "m"})),
                &health(),
            )
            .expect("resolves");

            assert_eq!(resolution.candidates.len(), 2);
            assert!(
                resolution.candidates[0].reference() == "p/a"
                    || resolution.candidates[0].reference() == "p/b"
            );
            assert_eq!(resolution.candidates[1].reference(), "p/z");
        }
    }

    #[test]
    fn rules_route_before_the_virtual_model() {
        let mut config = config_with_provider();
        config
            .models
            .insert("m".to_string(), model("p/default", &[]));
        config.rules.push(RoutingRule {
            name: "big".to_string(),
            priority: None,
            condition: RuleCondition {
                min_estimated_tokens: Some(1),
                ..RuleCondition::default()
            },
            route_to: "p/big".to_string(),
        });

        let resolution = resolve_candidates(
            &config,
            &request(serde_json::json!({"model": "m"})),
            &health(),
        )
        .expect("resolves");

        assert_eq!(resolution.candidates[0].reference(), "p/big");
    }

    #[test]
    fn lower_priority_number_wins() {
        let mut config = config_with_provider();
        config
            .models
            .insert("m".to_string(), model("p/default", &[]));
        config.rules.push(RoutingRule {
            name: "late".to_string(),
            priority: Some(10),
            condition: RuleCondition {
                model: Some("m".to_string()),
                ..RuleCondition::default()
            },
            route_to: "p/late".to_string(),
        });
        config.rules.push(RoutingRule {
            name: "early".to_string(),
            priority: Some(1),
            condition: RuleCondition {
                model: Some("m".to_string()),
                ..RuleCondition::default()
            },
            route_to: "p/early".to_string(),
        });

        let resolution = resolve_candidates(
            &config,
            &request(serde_json::json!({"model": "m"})),
            &health(),
        )
        .expect("resolves");

        assert_eq!(resolution.candidates[0].reference(), "p/early");
    }

    #[test]
    fn header_conditions_are_matched_exactly() {
        let mut config = config_with_provider();
        config
            .models
            .insert("m".to_string(), model("p/default", &[]));
        config.rules.push(RoutingRule {
            name: "confidential".to_string(),
            priority: None,
            condition: RuleCondition {
                header_matches: Some(std::collections::BTreeMap::from([(
                    "X-Data-Classification".to_string(),
                    "confidential".to_string(),
                )])),
                ..RuleCondition::default()
            },
            route_to: "p/private".to_string(),
        });

        let mut headers = HeaderMap::new();
        headers.insert(
            "x-data-classification",
            "confidential".parse().expect("header"),
        );
        let tagged = inspect_request(headers, Bytes::from(r#"{"model":"m"}"#.to_string()))
            .expect("inspects");

        let resolution = resolve_candidates(&config, &tagged, &health()).expect("resolves");
        assert_eq!(resolution.candidates[0].reference(), "p/private");

        let untagged = request(serde_json::json!({"model": "m"}));
        let resolution = resolve_candidates(&config, &untagged, &health()).expect("resolves");
        assert_eq!(resolution.candidates[0].reference(), "p/default");
    }

    #[test]
    fn complexity_condition_uses_the_inspector_score() {
        let mut config = config_with_provider();
        config
            .models
            .insert("m".to_string(), model("p/default", &[]));
        config.rules.push(RoutingRule {
            name: "cheap".to_string(),
            priority: None,
            condition: RuleCondition {
                complexity_score_below: Some(0.35),
                ..RuleCondition::default()
            },
            route_to: "p/cheap".to_string(),
        });

        let resolution = resolve_candidates(
            &config,
            &request(serde_json::json!({
                "model": "m",
                "messages": [{"role": "user", "content": "hi"}],
            })),
            &health(),
        )
        .expect("resolves");

        assert_eq!(resolution.candidates[0].reference(), "p/cheap");
    }

    #[test]
    fn regex_header_condition_matches() {
        let mut config = config_with_provider();
        config
            .models
            .insert("m".to_string(), model("p/default", &[]));
        config.rules.push(RoutingRule {
            name: "beta".to_string(),
            priority: None,
            condition: RuleCondition {
                header_regex_matches: Some(std::collections::BTreeMap::from([(
                    "X-Client".to_string(),
                    "^beta-".to_string(),
                )])),
                ..RuleCondition::default()
            },
            route_to: "p/beta".to_string(),
        });

        let mut headers = HeaderMap::new();
        headers.insert("x-client", "beta-canary".parse().expect("header"));
        let tagged = inspect_request(headers, Bytes::from(r#"{"model":"m"}"#.to_string()))
            .expect("inspects");

        let resolution = resolve_candidates(&config, &tagged, &health()).expect("resolves");
        assert_eq!(resolution.candidates[0].reference(), "p/beta");
    }

    #[test]
    fn retry_statuses_come_from_the_model_config() {
        let mut config = config_with_provider();
        let mut m = model("p/a", &[]);
        m.retry_on_status = vec![429, 503];
        config.models.insert("m".to_string(), m);

        let resolution = resolve_candidates(
            &config,
            &request(serde_json::json!({"model": "m"})),
            &health(),
        )
        .expect("resolves");

        assert_eq!(resolution.retry_on_status, vec![429, 503]);
    }

    #[test]
    fn retry_statuses_fall_back_to_the_defaults() {
        let mut config = config_with_provider();
        config.models.insert("m".to_string(), model("p/a", &[]));

        let resolution = resolve_candidates(
            &config,
            &request(serde_json::json!({"model": "m"})),
            &health(),
        )
        .expect("resolves");

        assert_eq!(resolution.retry_on_status, DEFAULT_RETRY_STATUSES);
    }
}

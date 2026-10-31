//! Unified error types and the OpenAI-normalized error envelope.
//!
//! Every failure surfaced to a client is rendered through [`RouterError`] so that
//! downstream SDKs written against OpenAI see a consistent JSON shape:
//!
//! ```json
//! {
//!   "error": {
//!     "message": "...",
//!     "type": "upstream_router_error",
//!     "param": null,
//!     "code": "all_upstreams_failed",
//!     "oxide_metadata": { "request_id": "...", "attempted_upstreams": [] }
//!   }
//! }
//! ```
//!
//! `oxide_metadata` is additive: OpenAI clients ignore unknown keys.

use std::io;

use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::Json;
use serde::Serialize;

/// A single upstream attempt recorded while trying to satisfy a request.
///
/// Rendered inside [`OxideMetadata`] so operators can see exactly why the
/// router gave up: which upstream, why, and how long it burned.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct AttemptRecord {
    /// `provider_name/upstream_model`, e.g. `openai_us/gpt-4o`.
    pub provider: String,
    /// Human-readable failure reason, e.g. `ttft_timeout_exceeded (2500ms)`.
    pub reason: String,
    /// Wall-clock milliseconds spent on this attempt.
    pub elapsed_ms: u64,
}

impl AttemptRecord {
    /// Builds a record for an upstream attempt that failed.
    pub fn new(provider: impl Into<String>, reason: impl Into<String>, elapsed_ms: u64) -> Self {
        Self {
            provider: provider.into(),
            reason: reason.into(),
            elapsed_ms,
        }
    }
}

/// Extra diagnostic context attached to the error envelope.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct OxideMetadata {
    /// Correlation id echoed back to the client and into traces.
    pub request_id: String,
    /// Ordered list of upstreams that were attempted before giving up.
    pub attempted_upstreams: Vec<AttemptRecord>,
}

/// Configuration-time failures.
///
/// These are startup / reload errors, never request-path errors, and therefore
/// carry `miette` diagnostics with codes that can be looked up in the docs.
#[derive(Debug, thiserror::Error, miette::Diagnostic)]
pub enum ConfigError {
    /// A `${VAR}` placeholder had no value in strict (serve) mode.
    #[error("missing required environment variable `{var}`")]
    #[diagnostic(
        code(oxide::config::missing_env_var),
        help("Export the variable before starting the router, or supply a fallback with the default-value interpolation syntax.")
    )]
    MissingEnvVar {
        /// Name of the absent variable.
        var: String,
    },

    /// The interpolation pass itself failed to initialise.
    #[error("environment interpolation failed: {detail}")]
    #[diagnostic(code(oxide::config::interpolation))]
    Interpolation {
        /// Underlying reason.
        detail: String,
    },

    /// The configuration file could not be read.
    #[error("failed to read configuration file `{path}`")]
    #[diagnostic(code(oxide::config::io))]
    Io {
        /// Path that was attempted.
        path: String,
        /// Underlying OS error.
        #[source]
        source: io::Error,
    },

    /// The file was readable but not valid TOML, or did not match the schema.
    #[error("failed to parse configuration TOML")]
    #[diagnostic(code(oxide::config::toml))]
    Toml {
        /// Underlying `toml` decoder error, including line/column.
        #[source]
        source: toml::de::Error,
    },

    /// A field parsed but its value is semantically invalid.
    #[error("invalid value for `{field}`: {detail}")]
    #[diagnostic(code(oxide::config::invalid_value))]
    InvalidValue {
        /// Dotted path of the offending field.
        field: String,
        /// Why the value was rejected.
        detail: String,
    },

    /// A `provider/model` reference named a provider that does not exist.
    #[error("`{context}` references unknown provider `{provider}` (in `{reference}`)")]
    #[diagnostic(
        code(oxide::config::undefined_provider),
        help("Define the provider in the `[providers]` table, or fix the reference.")
    )]
    UndefinedProvider {
        /// The full reference string that failed to resolve.
        reference: String,
        /// The provider segment that was not found.
        provider: String,
        /// Where the bad reference appeared, e.g. `models.smart-router.fallbacks[0]`.
        context: String,
    },

    /// A bare (provider-less) reference named a virtual model that does not exist.
    #[error("`{context}` references unknown model `{reference}`")]
    #[diagnostic(
        code(oxide::config::undefined_model),
        help(
            "A reference must be either `provider/model` or the key of a `[models.\"...\"]` entry."
        )
    )]
    UndefinedModel {
        /// The reference string that failed to resolve.
        reference: String,
        /// Where the bad reference appeared.
        context: String,
    },

    /// Virtual models reference each other in a loop, so resolution could never terminate.
    #[error("circular routing detected: {chain}")]
    #[diagnostic(
        code(oxide::config::circular_routing),
        help(
            "Break the cycle so that every model chain terminates at a concrete `provider/model`."
        )
    )]
    CircularRouting {
        /// The cycle, starting and ending at the same model name.
        chain: RoutingChain,
    },

    /// Two `[[rules]]` entries share a name, making traces ambiguous.
    #[error("duplicate routing rule name `{name}`")]
    #[diagnostic(code(oxide::config::duplicate_rule))]
    DuplicateRule {
        /// The duplicated rule name.
        name: String,
    },
}

/// An ordered cycle of virtual-model names, rendered as `a -> b -> a`.
///
/// A dedicated type (rather than a bare `Vec<String>`) gives the error a real
/// `Display` implementation without leaning on `Debug` formatting.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RoutingChain(Vec<String>);

impl RoutingChain {
    /// Builds a chain from the visited node order.
    pub fn new(nodes: impl IntoIterator<Item = String>) -> Self {
        Self(nodes.into_iter().collect())
    }

    /// The node names in cycle order.
    pub fn nodes(&self) -> &[String] {
        &self.0
    }
}

impl std::fmt::Display for RoutingChain {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0.join(" -> "))
    }
}

/// Every failure the router can surface to a client.
#[derive(Debug, thiserror::Error)]
pub enum RouterError {
    /// Configuration was invalid; surfaced by `validate` and on reload.
    #[error(transparent)]
    Config(#[from] ConfigError),

    /// Every candidate upstream failed; the client gets a 502 with a full attempt log.
    #[error("all configured upstreams failed for model '{model}'")]
    AllUpstreamsFailed {
        /// The model the client asked for.
        model: String,
        /// Correlation id for this request.
        request_id: String,
        /// Ordered attempt log.
        attempts: Vec<AttemptRecord>,
    },

    /// A tenant exceeded RPM, TPM, or its daily USD budget.
    #[error("rate limit exceeded for tenant '{tenant}': {detail}")]
    RateLimitExceeded {
        /// Tenant identifier resolved from the configured header.
        tenant: String,
        /// Which limit tripped and the current consumption.
        detail: String,
    },

    /// The client sent something the router cannot act on.
    #[error("invalid request: {message}")]
    InvalidRequest {
        /// What was wrong.
        message: String,
    },

    /// An upstream answered with a non-2xx status.
    #[error("upstream '{provider}' returned HTTP {status}")]
    UpstreamHttp {
        /// `provider/model`.
        provider: String,
        /// Status code returned by the upstream.
        status: u16,
    },

    /// An upstream committed HTTP 200 but never produced a content token in time.
    #[error("upstream '{provider}' exceeded TTFT timeout of {timeout_ms}ms")]
    TtftTimeout {
        /// `provider/model`.
        provider: String,
        /// The watchdog deadline that fired.
        timeout_ms: u64,
    },

    /// The circuit breaker is open for this upstream, so it was skipped.
    #[error("circuit breaker open for upstream '{provider}'")]
    CircuitBreakerOpen {
        /// `provider/model`.
        provider: String,
    },

    /// Transport-level failure talking to an upstream.
    #[error("upstream transport failure: {message}")]
    Transport {
        /// `provider/model`.
        provider: String,
        /// Underlying transport description.
        message: String,
    },

    /// Anything else. Never leaks internal detail to the client.
    #[error("internal error: {message}")]
    Internal {
        /// Operator-facing detail.
        message: String,
    },
}

impl RouterError {
    /// HTTP status returned to the client.
    pub fn status(&self) -> StatusCode {
        match self {
            Self::Config(_) | Self::Internal { .. } => StatusCode::INTERNAL_SERVER_ERROR,
            Self::AllUpstreamsFailed { .. }
            | Self::UpstreamHttp { .. }
            | Self::Transport { .. } => StatusCode::BAD_GATEWAY,
            Self::RateLimitExceeded { .. } => StatusCode::TOO_MANY_REQUESTS,
            Self::InvalidRequest { .. } => StatusCode::BAD_REQUEST,
            Self::TtftTimeout { .. } => StatusCode::GATEWAY_TIMEOUT,
            Self::CircuitBreakerOpen { .. } => StatusCode::SERVICE_UNAVAILABLE,
        }
    }

    /// The OpenAI-compatible `error.type` discriminator.
    pub fn error_type(&self) -> &'static str {
        match self {
            Self::Config(_) => "configuration_error",
            Self::AllUpstreamsFailed { .. }
            | Self::UpstreamHttp { .. }
            | Self::TtftTimeout { .. }
            | Self::CircuitBreakerOpen { .. }
            | Self::Transport { .. } => "upstream_router_error",
            Self::RateLimitExceeded { .. } => "rate_limit_error",
            Self::InvalidRequest { .. } => "invalid_request_error",
            Self::Internal { .. } => "api_error",
        }
    }

    /// Stable machine-readable `error.code`.
    pub fn code(&self) -> &'static str {
        match self {
            Self::Config(_) => "invalid_configuration",
            Self::AllUpstreamsFailed { .. } => "all_upstreams_failed",
            Self::UpstreamHttp { .. } => "upstream_http_error",
            Self::TtftTimeout { .. } => "ttft_timeout_exceeded",
            Self::CircuitBreakerOpen { .. } => "circuit_breaker_open",
            Self::Transport { .. } => "upstream_transport_error",
            Self::RateLimitExceeded { .. } => "rate_limit_exceeded",
            Self::InvalidRequest { .. } => "invalid_request",
            Self::Internal { .. } => "internal_error",
        }
    }

    /// Router-specific context, when the variant carries any.
    pub fn metadata(&self) -> Option<OxideMetadata> {
        match self {
            Self::AllUpstreamsFailed {
                request_id,
                attempts,
                ..
            } => Some(OxideMetadata {
                request_id: request_id.clone(),
                attempted_upstreams: attempts.clone(),
            }),
            _ => None,
        }
    }

    /// Renders the error into the normalized envelope.
    pub fn envelope(&self) -> ErrorEnvelope {
        ErrorEnvelope {
            error: ErrorBody {
                message: self.to_string(),
                error_type: self.error_type(),
                param: None,
                code: self.code(),
                oxide_metadata: self.metadata(),
            },
        }
    }
}

impl IntoResponse for RouterError {
    fn into_response(self) -> Response {
        let status = self.status();
        let envelope = self.envelope();
        (status, Json(envelope)).into_response()
    }
}

/// Top-level JSON body: `{ "error": { ... } }`.
#[derive(Debug, Clone, Serialize)]
pub struct ErrorEnvelope {
    /// The OpenAI-shaped error object.
    pub error: ErrorBody,
}

/// The OpenAI-shaped error object.
///
/// `param` is serialized as `null` (not omitted) because OpenAI clients expect
/// the key to be present.
#[derive(Debug, Clone, Serialize)]
pub struct ErrorBody {
    /// Human-readable message.
    pub message: String,
    /// Discriminator, mapped from [`RouterError::error_type`].
    #[serde(rename = "type")]
    pub error_type: &'static str,
    /// Offending parameter, when one can be identified.
    pub param: Option<String>,
    /// Stable machine-readable code.
    pub code: &'static str,
    /// Router-specific diagnostics; omitted for client errors.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub oxide_metadata: Option<OxideMetadata>,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_attempts() -> Vec<AttemptRecord> {
        vec![
            AttemptRecord::new("openai_us/gpt-4o", "ttft_timeout_exceeded (2500ms)", 2501),
            AttemptRecord::new("anthropic_prod/claude-3-7-sonnet", "http_status_429", 84),
        ]
    }

    #[test]
    fn all_upstreams_failed_renders_documented_envelope() {
        let err = RouterError::AllUpstreamsFailed {
            model: "smart-router".to_string(),
            request_id: "req_01j9z8x7w6v5u4t3s2r1".to_string(),
            attempts: sample_attempts(),
        };

        assert_eq!(err.status(), StatusCode::BAD_GATEWAY);
        assert_eq!(err.error_type(), "upstream_router_error");
        assert_eq!(err.code(), "all_upstreams_failed");

        let json = serde_json::to_value(err.envelope()).expect("envelope serializes");
        let error = json.get("error").expect("error key present");

        assert_eq!(error["type"], "upstream_router_error");
        assert_eq!(error["code"], "all_upstreams_failed");
        assert!(error["param"].is_null(), "param must serialize as null");
        assert_eq!(
            error["message"],
            "all configured upstreams failed for model 'smart-router'"
        );

        let attempted = error["oxide_metadata"]["attempted_upstreams"]
            .as_array()
            .expect("attempt list present");
        assert_eq!(attempted.len(), 2);
        assert_eq!(attempted[0]["provider"], "openai_us/gpt-4o");
        assert_eq!(attempted[0]["reason"], "ttft_timeout_exceeded (2500ms)");
        assert_eq!(attempted[0]["elapsed_ms"], 2501);
        assert_eq!(
            error["oxide_metadata"]["request_id"],
            "req_01j9z8x7w6v5u4t3s2r1"
        );
    }

    #[test]
    fn metadata_is_omitted_for_errors_without_attempts() {
        let err = RouterError::RateLimitExceeded {
            tenant: "acme".to_string(),
            detail: "rpm 601/600".to_string(),
        };

        assert_eq!(err.status(), StatusCode::TOO_MANY_REQUESTS);
        assert_eq!(err.error_type(), "rate_limit_error");

        let json = serde_json::to_value(err.envelope()).expect("envelope serializes");
        assert!(json["error"].get("oxide_metadata").is_none());
    }

    #[test]
    fn config_errors_are_internal_failures() {
        let err = RouterError::from(ConfigError::MissingEnvVar {
            var: "OPENAI_API_KEY".to_string(),
        });

        assert_eq!(err.status(), StatusCode::INTERNAL_SERVER_ERROR);
        assert_eq!(err.code(), "invalid_configuration");
        assert!(err.to_string().contains("OPENAI_API_KEY"));
    }

    #[test]
    fn circular_routing_message_renders_the_chain() {
        let err = ConfigError::CircularRouting {
            chain: RoutingChain::new(["a".to_string(), "b".to_string(), "a".to_string()]),
        };

        assert_eq!(err.to_string(), "circular routing detected: a -> b -> a");
    }

    #[test]
    fn every_variant_maps_to_a_success_class_status() {
        let variants = [
            RouterError::InvalidRequest {
                message: "bad".into(),
            },
            RouterError::UpstreamHttp {
                provider: "p/m".into(),
                status: 503,
            },
            RouterError::TtftTimeout {
                provider: "p/m".into(),
                timeout_ms: 1,
            },
            RouterError::CircuitBreakerOpen {
                provider: "p/m".into(),
            },
            RouterError::Transport {
                provider: "p/m".into(),
                message: "refused".into(),
            },
            RouterError::Internal {
                message: "boom".into(),
            },
        ];

        for variant in variants {
            // Classification must be exhaustive and in range for an HTTP response.
            let status = variant.status();
            assert!(
                status.is_client_error() || status.is_server_error(),
                "unexpected status {status} for {variant:?}"
            );
            assert!(!variant.code().is_empty());
            assert!(!variant.error_type().is_empty());
        }
    }
}

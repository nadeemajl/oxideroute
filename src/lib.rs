//! OxideRoute — an ultra-low-latency LLM router and API gateway.
//!
//! The library crate exists so that the server, the CLI subcommands, and the
//! integration tests in `tests/` all share one set of types. Modules are added
//! milestone by milestone per `docs/IMPLEMENTATION_PLAN.md`:
//!
//! * Milestone 1 — configuration model and the unified error envelope.
//! * Milestone 2 — request inspector, provider trait, OpenAI pass-through.
//! * Milestone 3 — Anthropic and Gemini request/response normalisers.
//! * Milestone 4 — routing, fallback execution, rate limiting, hot reload.
//! * Milestone 5 — Prometheus metrics, tracing with PII redaction, CLI.

pub mod admin;
pub mod config;
pub mod error;
pub mod providers;
pub mod proxy;
pub mod state;
pub mod telemetry;

// The deterministic mock upstream lives in `tests/` so the integration tests can
// `mod mock_upstreams;` it directly. Including it here by path lets the provider
// unit tests drive the same server instead of duplicating it.
#[cfg(test)]
#[path = "../tests/mock_upstreams.rs"]
mod mock_upstreams;

pub use config::loader::{
    interpolate_env, interpolate_with, load_config_from_path, load_config_from_str,
    InterpolationMode,
};
pub use config::RouterConfig;
pub use error::{AttemptRecord, ConfigError, OxideMetadata, RouterError};
pub use providers::anthropic::AnthropicProvider;
pub use providers::bedrock::BedrockProvider;
pub use providers::gemini::GeminiProvider;
pub use providers::openai::OpenAiProvider;
pub use providers::{LlmProvider, NonStreamResponse, SseByteStream};
pub use proxy::build_router;
pub use proxy::executor::{dispatch_embeddings, dispatch_json, dispatch_stream};
pub use proxy::inspector::{inspect_request, ProxyRequest};
pub use proxy::ratelimit::TenantRateLimiter;
pub use proxy::router_engine::{resolve_candidates, Candidate, Resolution};
pub use state::{AppState, UpstreamHealthMetrics};
pub use telemetry::metrics::Metrics;

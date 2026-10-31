//! OxideRoute — an ultra-low-latency LLM router and API gateway.
//!
//! The library crate exists so that the server, the CLI subcommands, and the
//! integration tests in `tests/` all share one set of types. Modules are added
//! milestone by milestone per `docs/IMPLEMENTATION_PLAN.md`; Milestone 1
//! delivers the configuration model and the unified error envelope.

pub mod config;
pub mod error;

pub use config::loader::{
    interpolate_env, interpolate_with, load_config_from_path, load_config_from_str,
    InterpolationMode,
};
pub use config::RouterConfig;
pub use error::{AttemptRecord, ConfigError, OxideMetadata, RouterError};

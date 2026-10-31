//! `oxide` CLI entry point.
//!
//! Subcommands:
//!
//! * `serve`     — run the ingress API with hot reload and a metrics endpoint.
//! * `validate`  — check a config, optionally probing upstream reachability.
//! * `dry-run`   — show the routing decision for a prompt without calling anything.
//! * `init`      — write a starter config.
//! * `schema`    — dump the JSON Schema for the config format.
//!
//! Configs are loaded **leniently** everywhere except `serve`: tooling should
//! work on a machine that does not hold the production secrets, so an unset
//! `${VAR}` is preserved rather than treated as fatal.

use std::path::PathBuf;
use std::process::ExitCode;
use std::sync::Arc;
use std::time::Duration;

use clap::{Parser, Subcommand};
use http::{HeaderMap, HeaderName, HeaderValue};

use oxide_route::config::loader::{load_config_from_path, InterpolationMode};
use oxide_route::config::RouterConfig;
use oxide_route::error::{ConfigError, RouterError};
use oxide_route::proxy::build_router_with_metrics;
use oxide_route::proxy::inspector::inspect_request;
use oxide_route::proxy::router_engine::{matched_rule_name, resolve_candidates};
use oxide_route::state::AppState;
use oxide_route::telemetry::metrics::Metrics;

/// Starter configuration written by `oxide init`.
const INITIAL_CONFIG: &str = r#"#:schema https://schema.oxideroute.dev/v1/oxide.schema.json
listen = "0.0.0.0:8080"

[providers.openai]
type = "openai"
api_key = "${OPENAI_API_KEY}"

[providers.anthropic]
type = "anthropic"
api_key = "${ANTHROPIC_API_KEY}"

[models."gpt-4o"]
primary = "openai/gpt-4o"
fallbacks = ["anthropic/claude-3-7-sonnet-latest"]
"#;

/// Reachability probe budget for `validate --check-upstreams`.
const PROBE_TIMEOUT: Duration = Duration::from_secs(3);

#[derive(Debug, Parser)]
#[command(
    name = "oxide",
    version,
    about = "Ultra-low-latency LLM router and API gateway"
)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Serve the OpenAI-compatible ingress API.
    Serve {
        /// Path to the TOML configuration.
        #[arg(long, default_value = "oxide.toml")]
        config: PathBuf,
    },

    /// Validate a configuration file without serving it.
    Validate {
        /// Path to the TOML configuration.
        #[arg(long, default_value = "oxide.toml")]
        config: PathBuf,

        /// TCP-probe each provider's base URL.
        #[arg(long)]
        check_upstreams: bool,
    },

    /// Show the routing decision for a prompt, without calling any upstream.
    DryRun {
        /// Path to the TOML configuration.
        #[arg(long, default_value = "oxide.toml")]
        config: PathBuf,

        /// Model name as the client would send it.
        #[arg(long)]
        model: String,

        /// Request header as `KEY=VALUE`; may be repeated.
        #[arg(long = "header", value_name = "KEY=VALUE")]
        headers: Vec<String>,

        /// Prompt text used for the token and complexity estimates.
        #[arg(long)]
        prompt: String,
    },

    /// Write a minimal starter configuration.
    Init {
        /// Destination path.
        #[arg(long, default_value = "oxide.toml")]
        output: PathBuf,

        /// Overwrite an existing file.
        #[arg(long)]
        force: bool,
    },

    /// Print the JSON Schema for the configuration format.
    Schema,
}

#[tokio::main]
async fn main() -> ExitCode {
    let cli = Cli::parse();

    match run(cli).await {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("oxide: {error}");
            ExitCode::FAILURE
        }
    }
}

async fn run(cli: Cli) -> Result<(), RouterError> {
    match cli.command {
        Command::Serve { config } => serve(config).await,
        Command::Validate {
            config,
            check_upstreams,
        } => validate(config, check_upstreams).await,
        Command::DryRun {
            config,
            model,
            headers,
            prompt,
        } => dry_run(config, &model, &headers, &prompt),
        Command::Init { output, force } => init(output, force),
        Command::Schema => schema(),
    }
}

// --------------------------------------------------------------------------- serve

async fn serve(path: PathBuf) -> Result<(), RouterError> {
    // Strict: never start against a half-configured upstream.
    let config = load_config_from_path(&path, InterpolationMode::Strict)?;
    let telemetry = oxide_route::telemetry::init_tracing(&config.telemetry)?;

    let listen = config.listen.clone();
    let drain_ms = config.graceful_shutdown_timeout_ms;

    let state = Arc::new(AppState::new(config)?);
    let metrics = Metrics::install()?;

    // Hold the watcher for the process lifetime; dropping it stops the watch.
    let _watcher =
        oxide_route::config::watcher::spawn_file_watcher(Arc::clone(&state), path.clone())?;
    oxide_route::config::watcher::spawn_sighup_listener(Arc::clone(&state), path.clone())?;

    let app = build_router_with_metrics(Arc::clone(&state), metrics);

    let listener = tokio::net::TcpListener::bind(&listen)
        .await
        .map_err(|error| RouterError::Internal {
            message: format!("failed to bind `{listen}`: {error}"),
        })?;

    tracing::info!(address = %listen, config = %path.display(), "oxide serving");

    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal(drain_ms))
        .await
        .map_err(|error| RouterError::Internal {
            message: format!("server error: {error}"),
        })?;

    telemetry.shutdown();
    tracing::info!("shutdown complete");
    Ok(())
}

async fn shutdown_signal(drain_ms: u64) {
    let ctrl_c = async {
        if let Err(error) = tokio::signal::ctrl_c().await {
            tracing::error!(%error, "failed to listen for Ctrl-C or SIGINT");
            std::future::pending::<()>().await;
        }
    };

    #[cfg(unix)]
    let terminate = async {
        match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
            Ok(mut signal) => {
                signal.recv().await;
            }
            Err(error) => {
                tracing::error!(%error, "failed to listen for SIGTERM");
                std::future::pending::<()>().await;
            }
        }
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {}
        _ = terminate => {}
    }

    tracing::info!(drain_ms, "shutting down; draining in-flight requests");
}

// --------------------------------------------------------------------------- validate

async fn validate(path: PathBuf, check_upstreams: bool) -> Result<(), RouterError> {
    let config = load_config_from_path(&path, InterpolationMode::Lenient)?;

    println!("configuration OK: {}", path.display());
    println!("  listen:               {}", config.listen);
    if let Some(admin) = &config.admin_listen {
        println!("  admin_listen:         {admin}");
    }
    println!("  providers:            {}", config.providers.len());
    println!("  models:               {}", config.models.len());
    println!("  routing rules:        {}", config.rules.len());
    println!("  rate-limited tenants: {}", config.rate_limits.len());

    if !check_upstreams {
        return Ok(());
    }

    println!();
    println!("upstream reachability:");

    let mut unreachable = Vec::new();
    for (name, provider) in &config.providers {
        let target = provider.base_url.as_deref();
        let reachable = probe_upstream(target).await;
        let label = target.unwrap_or("<provider default>");

        if reachable {
            println!("  ok        {name} -> {label}");
        } else {
            println!("  UNREACH   {name} -> {label}");
            unreachable.push(name.clone());
        }
    }

    if unreachable.is_empty() {
        Ok(())
    } else {
        Err(RouterError::Internal {
            message: format!("unreachable upstreams: {}", unreachable.join(", ")),
        })
    }
}

/// TCP-probes a base URL. Providers without an explicit `base_url` use a
/// well-known public endpoint and are reported as reachable without a probe,
/// since a missing override is not an error.
async fn probe_upstream(base_url: Option<&str>) -> bool {
    let Some(raw) = base_url else {
        return true;
    };

    let Ok(url) = reqwest::Url::parse(raw) else {
        return false;
    };
    let Some(host) = url.host_str() else {
        return false;
    };
    let port = url.port_or_known_default().unwrap_or(443);

    matches!(
        tokio::time::timeout(PROBE_TIMEOUT, tokio::net::TcpStream::connect((host, port))).await,
        Ok(Ok(_))
    )
}

// --------------------------------------------------------------------------- dry-run

fn dry_run(
    path: PathBuf,
    model: &str,
    headers: &[String],
    prompt: &str,
) -> Result<(), RouterError> {
    let config = load_config_from_path(&path, InterpolationMode::Lenient)?;

    let mut header_map = HeaderMap::new();
    for entry in headers {
        let (key, value) = entry
            .split_once('=')
            .ok_or_else(|| RouterError::InvalidRequest {
                message: format!("header `{entry}` must be in KEY=VALUE form"),
            })?;

        let name = HeaderName::from_bytes(key.trim().as_bytes()).map_err(|error| {
            RouterError::InvalidRequest {
                message: format!("invalid header name `{key}`: {error}"),
            }
        })?;
        let value =
            HeaderValue::from_str(value.trim()).map_err(|error| RouterError::InvalidRequest {
                message: format!("invalid header value for `{key}`: {error}"),
            })?;

        header_map.insert(name, value);
    }

    let body = serde_json::json!({
        "model": model,
        "stream": true,
        "messages": [{"role": "user", "content": prompt}],
    });

    let request = inspect_request(header_map, bytes::Bytes::from(body.to_string()))?;

    // No traffic has flowed, so every EWMA is unknown.
    let health = dashmap::DashMap::new();
    let resolution = resolve_candidates(&config, &request, &health)?;
    let rule = matched_rule_name(&config, &request);

    println!("Routing trace");
    println!("=============");
    println!("config:      {}", path.display());
    println!("request id:  {}", request.request_id);
    println!("model:       {}", request.requested_model);
    println!("stream:      {}", request.is_stream);
    println!(
        "prompt:      ~{} tokens (estimated), complexity {:.2}",
        request.estimated_prompt_tokens, request.complexity_score
    );
    match rule {
        Some(name) => println!("matched:     rule `{name}`"),
        None => println!("matched:     no rule (virtual model)"),
    }
    println!(
        "retry on:    {}",
        resolution
            .retry_on_status
            .iter()
            .map(u16::to_string)
            .collect::<Vec<_>>()
            .join(", ")
    );
    println!();
    println!(
        "{:<3} {:<18} {:<32} {:<9} {:>10}",
        "#", "PROVIDER", "UPSTREAM MODEL", "HEALTH", "EWMA(µs)"
    );

    for (index, candidate) in resolution.candidates.iter().enumerate() {
        let ewma = health
            .get(&candidate.reference())
            .map(|entry| entry.value().ewma_ttft_us())
            .filter(|value| *value > 0)
            .map(|value| value.to_string())
            .unwrap_or_else(|| "-".to_string());

        println!(
            "{:<3} {:<18} {:<32} {:<9} {:>10}",
            index + 1,
            candidate.provider,
            candidate.model,
            "unknown",
            ewma
        );
    }

    println!();
    println!(
        "plan: try {} upstream(s) in order, then fail the request",
        resolution.candidates.len()
    );

    Ok(())
}

// --------------------------------------------------------------------------- init

fn init(output: PathBuf, force: bool) -> Result<(), RouterError> {
    if output.exists() && !force {
        return Err(RouterError::Config(ConfigError::InvalidValue {
            field: output.display().to_string(),
            detail: "file already exists; pass --force to overwrite".to_string(),
        }));
    }

    std::fs::write(&output, INITIAL_CONFIG).map_err(|source| {
        RouterError::Config(ConfigError::Io {
            path: output.display().to_string(),
            source,
        })
    })?;

    println!("wrote {}", output.display());
    Ok(())
}

// --------------------------------------------------------------------------- schema

fn schema() -> Result<(), RouterError> {
    let schema = schemars::schema_for!(RouterConfig);
    let rendered =
        serde_json::to_string_pretty(&schema).map_err(|error| RouterError::Internal {
            message: format!("failed to render the JSON schema: {error}"),
        })?;

    println!("{rendered}");
    Ok(())
}

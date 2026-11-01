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

use std::future::Future;
use std::path::PathBuf;
use std::pin::Pin;
use std::process::ExitCode;
use std::sync::Arc;
use std::time::Duration;

use clap::{Parser, Subcommand};
use http::{HeaderMap, HeaderName, HeaderValue};
use tokio::sync::watch;

use oxide_route::config::loader::{load_config_from_path, InterpolationMode};
use oxide_route::config::RouterConfig;
use oxide_route::error::{ConfigError, RouterError};
use oxide_route::proxy::inspector::inspect_request;
use oxide_route::proxy::router_engine::{matched_rule_name, resolve_candidates};
use oxide_route::proxy::{build_admin_router, build_router, build_router_with_metrics};
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
    let admin_listen = config.admin_listen.clone();
    let drain = Duration::from_millis(config.graceful_shutdown_timeout_ms);

    let state = Arc::new(AppState::new(config)?);
    let metrics = Metrics::install()?;

    // Seed the gauges so a fresh process reports idle rather than rendering an
    // empty exposition page.
    metrics.seed_idle(configured_upstreams(&state));

    // Hold the watcher for the process lifetime; dropping it stops the watch.
    let _watcher =
        oxide_route::config::watcher::spawn_file_watcher(Arc::clone(&state), path.clone())?;
    oxide_route::config::watcher::spawn_sighup_listener(Arc::clone(&state), path.clone())?;

    // Bind everything before serving anything, so a port conflict fails at
    // startup instead of leaving one listener half-live.
    let public_listener = bind(&listen).await?;
    let admin_listener = match admin_listen.as_deref() {
        Some(address) => Some(bind(address).await?),
        None => None,
    };

    let public_app = if admin_listener.is_some() {
        build_router(Arc::clone(&state))
    } else {
        // No admin port means the scrape endpoint has nowhere else to live.
        // Say so loudly: `/metrics` carries per-tenant and per-upstream traffic
        // shape and is now reachable by anything that can reach the ingress.
        tracing::warn!(
            "`admin_listen` is unset: /metrics and /healthz are served on the public listener"
        );
        build_router_with_metrics(Arc::clone(&state), metrics.clone())
    };

    // One signal watcher fans out to every listener. A `watch` channel is used
    // rather than a one-shot future because `with_graceful_shutdown` consumes
    // its future and each server needs its own view of the same signal.
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    tokio::spawn(watch_signals(shutdown_tx, drain));

    let public_server = axum::serve(public_listener, public_app)
        .with_graceful_shutdown(wait_for_shutdown(shutdown_rx.clone()));

    tracing::info!(address = %listen, config = %path.display(), "oxide serving");

    let servers: Pin<Box<dyn Future<Output = Result<(), RouterError>> + Send>> =
        match admin_listener {
            Some(admin_listener) => {
                let admin_app = build_admin_router(Arc::clone(&state), metrics.clone());
                let admin_server = axum::serve(admin_listener, admin_app)
                    .with_graceful_shutdown(wait_for_shutdown(shutdown_rx.clone()));

                tracing::info!(
                    address = %admin_listen.as_deref().unwrap_or_default(),
                    "admin listener serving /metrics and /healthz"
                );

                Box::pin(async move {
                    tokio::try_join!(public_server, admin_server).map_err(server_error)?;
                    Ok(())
                })
            }
            None => Box::pin(async move {
                public_server.await.map_err(server_error)?;
                Ok(())
            }),
        };

    let drained = await_drain(servers, shutdown_rx, drain, || {
        // The hard stop for streams that outlive the drain window. Without it a
        // single stalled SSE stream holds the process open until the orchestrator
        // gives up and SIGKILLs, which loses the same streams anyway — but
        // without the log line explaining why.
        tracing::warn!(
            drain_ms = drain.as_millis() as u64,
            "graceful shutdown deadline reached; dropping remaining connections"
        );
        std::process::exit(0);
    })
    .await;

    // `None` is only reachable from tests: in production the deadline handler
    // exits the process instead of returning.
    if let Some(result) = drained {
        result?;
    }

    telemetry.shutdown();
    tracing::info!("shutdown complete");
    Ok(())
}

fn server_error(error: std::io::Error) -> RouterError {
    RouterError::Internal {
        message: format!("server error: {error}"),
    }
}

/// Every `provider/model` reference the configuration can dispatch to.
///
/// Sorted and deduplicated so a model that reuses a fallback does not produce
/// duplicate gauge series.
fn configured_upstreams(state: &AppState) -> Vec<String> {
    let mut references: Vec<String> = state
        .config()
        .models
        .values()
        .flat_map(|model| model.upstream_references())
        .map(str::to_string)
        .collect();

    references.sort();
    references.dedup();
    references
}

async fn bind(address: &str) -> Result<tokio::net::TcpListener, RouterError> {
    tokio::net::TcpListener::bind(address)
        .await
        .map_err(|error| RouterError::Internal {
            message: format!("failed to bind `{address}`: {error}"),
        })
}

/// Runs the servers to completion, forcing `on_deadline` once the drain window
/// after a shutdown signal expires.
///
/// The deadline is measured from the signal, not from this call: a process that
/// has been serving for an hour still gets its full `graceful_shutdown_timeout_ms`.
///
/// Returns `None` when the deadline fired.
async fn await_drain<F>(
    servers: F,
    shutdown: watch::Receiver<bool>,
    drain: Duration,
    on_deadline: impl FnOnce(),
) -> Option<F::Output>
where
    F: Future,
{
    let deadline = async {
        wait_for_shutdown(shutdown).await;
        tokio::time::sleep(drain).await;
    };

    tokio::select! {
        output = servers => Some(output),
        () = deadline => {
            on_deadline();
            None
        }
    }
}

/// Resolves once the shutdown flag is raised, including when it already was.
async fn wait_for_shutdown(mut shutdown: watch::Receiver<bool>) {
    while !*shutdown.borrow_and_update() {
        if shutdown.changed().await.is_err() {
            return;
        }
    }
}

/// Waits for Ctrl-C or SIGTERM, then raises the shutdown flag for every server.
async fn watch_signals(shutdown: watch::Sender<bool>, drain: Duration) {
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

    tracing::info!(
        drain_ms = drain.as_millis() as u64,
        "shutting down; draining in-flight requests"
    );

    // Fails only when every server has already dropped its receiver, which means
    // the process is on its way down anyway.
    let _ = shutdown.send(true);
}

// --------------------------------------------------------------------------- validate

async fn validate(path: PathBuf, check_upstreams: bool) -> Result<(), RouterError> {
    let config = load_config_from_path(&path, InterpolationMode::Lenient)?;

    println!("configuration OK: {}", path.display());
    println!("  listen:               {}", config.listen);
    match &config.admin_listen {
        Some(admin) => println!("  admin_listen:         {admin}  (/metrics, /healthz)"),
        None => println!(
            "  admin_listen:         <unset>  -- /metrics will be served on the public listener"
        ),
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::time::Instant;

    /// Raises the shutdown flag after `after`.
    fn signal_after(after: Duration, shutdown: &watch::Sender<bool>) {
        let shutdown = shutdown.clone();
        tokio::spawn(async move {
            tokio::time::sleep(after).await;
            let _ = shutdown.send(true);
        });
    }

    #[tokio::test]
    async fn drain_deadline_forces_exit_when_a_stream_never_ends() {
        let (tx, rx) = watch::channel(false);
        signal_after(Duration::from_millis(10), &tx);

        let fired = Arc::new(AtomicBool::new(false));
        let flag = Arc::clone(&fired);
        let started = Instant::now();

        // A server that never finishes, like one stalled SSE stream.
        let outcome = await_drain(
            std::future::pending::<()>(),
            rx,
            Duration::from_millis(30),
            move || flag.store(true, Ordering::SeqCst),
        )
        .await;

        let elapsed = started.elapsed();
        assert!(outcome.is_none(), "a stalled drain must hit the deadline");
        assert!(
            fired.load(Ordering::SeqCst),
            "the deadline handler must run"
        );
        assert!(
            elapsed >= Duration::from_millis(35),
            "the deadline started before the signal fired: {elapsed:?}"
        );
    }

    #[tokio::test]
    async fn drain_returns_normally_when_the_servers_finish_first() {
        let (tx, rx) = watch::channel(false);
        signal_after(Duration::from_millis(10), &tx);

        let fired = Arc::new(AtomicBool::new(false));
        let flag = Arc::clone(&fired);

        let servers = async {
            tokio::time::sleep(Duration::from_millis(25)).await;
            "drained"
        };

        let outcome = await_drain(servers, rx, Duration::from_secs(30), move || {
            flag.store(true, Ordering::SeqCst);
        })
        .await;

        assert_eq!(outcome, Some("drained"), "the drain result must survive");
        assert!(
            !fired.load(Ordering::SeqCst),
            "a prompt drain must not trigger the deadline"
        );
    }

    #[tokio::test]
    async fn a_shutdown_already_raised_is_not_missed() {
        let (_tx, rx) = watch::channel(true);

        let started = Instant::now();
        wait_for_shutdown(rx).await;

        assert!(
            started.elapsed() < Duration::from_secs(1),
            "waiting on an already-set flag must return immediately"
        );
    }
}

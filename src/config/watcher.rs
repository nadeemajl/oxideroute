//! Zero-downtime configuration reload.
//!
//! Two triggers, one code path:
//!
//! * a `notify` file watcher (editor saves, `kubectl apply`-style config maps), and
//! * `SIGHUP` for operators with a shell.
//!
//! Both funnel through [`reload_from_path`], which parses with **strict**
//! interpolation — a config referencing an unset environment variable is
//! rejected rather than half-applied — and then hands the result to
//! [`AppState::apply_config`], whose swap is a single atomic store.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use notify::{RecursiveMode, Watcher};
use tracing::{info, warn};

use crate::config::loader::{load_config_from_path, InterpolationMode};
use crate::error::RouterError;
use crate::state::AppState;
use crate::telemetry::metrics;

/// How long to wait after a filesystem event before reloading.
///
/// Editors commonly emit several events per save (write, truncate, rename), and
/// reloading on each would rebuild every provider registry several times.
const DEBOUNCE: Duration = Duration::from_millis(50);

/// Loads, validates, and applies the configuration at `path`.
///
/// # Errors
///
/// Propagates parse/validation failures and provider-construction failures. On
/// error the previously active configuration remains in effect.
pub fn reload_from_path(state: &AppState, path: impl AsRef<Path>) -> Result<(), RouterError> {
    reload_with_source(state, path, "manual")
}

/// Reloads and counts the attempt, tagged with what triggered it.
///
/// A rejected reload is a *successful* safety mechanism — the old config keeps
/// serving — so it must be visible as a counter rather than only as a log line.
fn reload_with_source(
    state: &AppState,
    path: impl AsRef<Path>,
    source: &str,
) -> Result<(), RouterError> {
    let config = match load_config_from_path(path, InterpolationMode::Strict) {
        Ok(config) => config,
        Err(error) => {
            metrics::record_reload("rejected", source);
            return Err(error.into());
        }
    };

    match state.apply_config(config) {
        Ok(()) => {
            metrics::record_reload("success", source);
            Ok(())
        }
        Err(error) => {
            metrics::record_reload("rejected", source);
            Err(error)
        }
    }
}

/// Starts watching `path` and reloading the state when it changes.
///
/// The returned watcher must be kept alive: dropping it stops the watch.
///
/// # Errors
///
/// Fails if the platform watcher cannot be created or the path cannot be
/// watched.
pub fn spawn_file_watcher(
    state: Arc<AppState>,
    path: PathBuf,
) -> Result<notify::RecommendedWatcher, RouterError> {
    let target = path.clone();
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();

    let mut watcher = notify::recommended_watcher(move |event| {
        // The receiver lives as long as the spawned task below.
        let _ = tx.send(event);
    })
    .map_err(|error| RouterError::Internal {
        message: format!("failed to create config watcher: {error}"),
    })?;

    watcher
        .watch(&path, RecursiveMode::NonRecursive)
        .map_err(|error| RouterError::Internal {
            message: format!("failed to watch `{}`: {error}", path.display()),
        })?;

    tokio::spawn(async move {
        while let Some(event) = rx.recv().await {
            match event {
                Ok(event) if event.kind.is_modify() || event.kind.is_create() => {
                    tokio::time::sleep(DEBOUNCE).await;

                    match reload_with_source(&state, &target, "file") {
                        Ok(()) => info!(config = %target.display(), "configuration reloaded"),
                        Err(error) => warn!(
                            config = %target.display(),
                            %error,
                            "rejected invalid configuration; keeping the active one"
                        ),
                    }
                }
                Ok(_) => {}
                Err(error) => warn!(%error, "config watcher reported an error"),
            }
        }
    });

    Ok(watcher)
}

/// Reloads the configuration when the process receives `SIGHUP`.
///
/// # Errors
///
/// Fails if the signal handler cannot be installed (Unix only).
#[cfg(unix)]
pub fn spawn_sighup_listener(state: Arc<AppState>, path: PathBuf) -> Result<(), RouterError> {
    let mut signals = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::hangup())
        .map_err(|error| RouterError::Internal {
            message: format!("failed to install SIGHUP handler: {error}"),
        })?;

    tokio::spawn(async move {
        while signals.recv().await.is_some() {
            match reload_with_source(&state, &path, "signal") {
                Ok(()) => info!(config = %path.display(), "configuration reloaded on SIGHUP"),
                Err(error) => warn!(
                    config = %path.display(),
                    %error,
                    "SIGHUP reload rejected; keeping the active configuration"
                ),
            }
        }
    });

    Ok(())
}

/// No-op on platforms without Unix signals.
#[cfg(not(unix))]
pub fn spawn_sighup_listener(_state: Arc<AppState>, _path: PathBuf) -> Result<(), RouterError> {
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{ProviderConfig, ProviderKind, RouterConfig};
    use std::io::Write;

    fn config_toml(model: &str) -> String {
        format!(
            r#"
listen = "127.0.0.1:8080"

[providers.local]
type = "openai_compatible"
base_url = "http://127.0.0.1:11434/v1"

[models.{model}]
primary = "local/gpt-4o"
"#
        )
    }

    fn write_temp(name: &str, contents: &str) -> PathBuf {
        let path = std::env::temp_dir().join(format!(
            "oxide-watch-{}-{name}.toml",
            uuid::Uuid::new_v4().simple()
        ));
        let mut file = std::fs::File::create(&path).expect("create temp config");
        file.write_all(contents.as_bytes()).expect("write config");
        path
    }

    fn seed_state() -> AppState {
        let mut config = RouterConfig::default();
        config.providers.insert(
            "local".to_string(),
            ProviderConfig {
                kind: ProviderKind::OpenaiCompatible,
                api_key: None,
                base_url: Some("http://127.0.0.1:11434/v1".to_string()),
                timeout_ms: 1_000,
                ttft_timeout_ms: 500,
            },
        );
        AppState::new(config).expect("state builds")
    }

    #[test]
    fn reload_replaces_the_active_config() {
        let path = write_temp("valid", &config_toml("alpha"));
        let state = seed_state();

        reload_from_path(&state, &path).expect("reload succeeds");

        assert!(state.config().models.contains_key("alpha"));
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn invalid_config_leaves_the_active_config_untouched() {
        let state = seed_state();
        let before = state.config();

        let path = write_temp("broken", "listen = [\n");
        assert!(reload_from_path(&state, &path).is_err());

        assert_eq!(state.config().listen, before.listen);
        assert!(state.config().models.is_empty());
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn unset_env_var_rejects_the_reload() {
        let path = write_temp(
            "missing-env",
            r#"
listen = "127.0.0.1:8080"

[providers.p]
type = "openai"
api_key = "${OXIDE_TEST_DEFINITELY_UNSET_VAR}"

[models.m]
primary = "p/gpt-4o"
"#,
        );
        let state = seed_state();

        let error = reload_from_path(&state, &path).expect_err("unset var must reject");
        assert!(error
            .to_string()
            .contains("OXIDE_TEST_DEFINITELY_UNSET_VAR"));

        let _ = std::fs::remove_file(path);
    }
}

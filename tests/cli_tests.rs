//! End-to-end tests for the `oxide` binary's subcommands.
//!
//! These spawn the real executable via `CARGO_BIN_EXE_oxide`, so they cover
//! argument parsing, exit codes, and stdout formatting — the parts unit tests
//! cannot reach.

use std::io::{Read, Write};
use std::net::TcpStream;
use std::path::PathBuf;
use std::process::{Command, Stdio};
use std::time::Duration;

fn oxide() -> Command {
    Command::new(env!("CARGO_BIN_EXE_oxide"))
}

fn temp_path(label: &str) -> PathBuf {
    std::env::temp_dir().join(format!(
        "oxide-cli-{label}-{}.toml",
        uuid::Uuid::new_v4().simple()
    ))
}

#[test]
fn schema_emits_valid_json_for_the_root_config() {
    let output = oxide().arg("schema").output().expect("runs");

    assert!(
        output.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    let schema: serde_json::Value =
        serde_json::from_slice(&output.stdout).expect("schema is valid JSON");

    for field in ["listen", "providers", "models", "rules", "rate_limits"] {
        assert!(
            schema["properties"].get(field).is_some(),
            "schema is missing `{field}`"
        );
    }
}

#[test]
fn validate_accepts_the_shipped_examples() {
    for example in ["examples/minimal.toml", "examples/production.toml"] {
        let output = oxide()
            .args(["validate", "--config", example])
            .output()
            .expect("runs");

        assert!(
            output.status.success(),
            "{example} failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );

        let stdout = String::from_utf8_lossy(&output.stdout);
        assert!(stdout.contains("configuration OK"), "{stdout}");
    }
}

#[test]
fn validate_rejects_a_malformed_config() {
    let path = temp_path("broken");
    std::fs::write(&path, "listen = [\n").expect("write");

    let output = oxide()
        .args(["validate", "--config", path.to_str().expect("utf-8 path")])
        .output()
        .expect("runs");

    assert!(!output.status.success(), "malformed TOML must fail");
    assert!(String::from_utf8_lossy(&output.stderr).contains("oxide:"));

    let _ = std::fs::remove_file(path);
}

#[test]
fn dry_run_reports_the_matched_rule_and_candidate_plan() {
    let output = oxide()
        .args([
            "dry-run",
            "--config",
            "examples/production.toml",
            "--model",
            "auto",
            "--prompt",
            "Hello",
        ])
        .output()
        .expect("runs");

    assert!(
        output.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    let stdout = String::from_utf8_lossy(&output.stdout);

    assert!(stdout.contains("Routing trace"), "{stdout}");
    // The complexity rule in production.toml should match a trivial prompt.
    assert!(stdout.contains("cost-aware-complexity-tiering"), "{stdout}");
    assert!(stdout.contains("openai_us"), "{stdout}");
    assert!(stdout.contains("gpt-4o-mini"), "{stdout}");
    assert!(stdout.contains("PROVIDER"), "{stdout}");
}

#[test]
fn dry_run_rejects_a_malformed_header() {
    let output = oxide()
        .args([
            "dry-run",
            "--config",
            "examples/production.toml",
            "--model",
            "auto",
            "--prompt",
            "Hello",
            "--header",
            "not-a-pair",
        ])
        .output()
        .expect("runs");

    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr).contains("KEY=VALUE"));
}

#[test]
fn init_writes_a_loadable_config_and_refuses_to_clobber() {
    let path = temp_path("init");

    let created = oxide()
        .args(["init", "--output", path.to_str().expect("utf-8 path")])
        .output()
        .expect("runs");
    assert!(
        created.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&created.stderr)
    );

    // The template must pass our own validator.
    let validated = oxide()
        .args(["validate", "--config", path.to_str().expect("utf-8 path")])
        .output()
        .expect("runs");
    assert!(
        validated.status.success(),
        "generated config is invalid: {}",
        String::from_utf8_lossy(&validated.stderr)
    );

    // A second run must not silently destroy the file.
    let clobber = oxide()
        .args(["init", "--output", path.to_str().expect("utf-8 path")])
        .output()
        .expect("runs");
    assert!(!clobber.status.success());

    // `--force` is the explicit opt-in.
    let forced = oxide()
        .args([
            "init",
            "--output",
            path.to_str().expect("utf-8 path"),
            "--force",
        ])
        .output()
        .expect("runs");
    assert!(forced.status.success());

    let _ = std::fs::remove_file(path);
}

// --------------------------------------------------------------- serve: listeners

/// Kills the child on drop so a failing assertion cannot leak a server process.
struct ChildGuard(std::process::Child);

impl Drop for ChildGuard {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

/// Reserves a loopback port by binding and immediately releasing it.
fn free_port() -> u16 {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind ephemeral port");
    listener.local_addr().expect("local addr").port()
}

/// Polls until something accepts on `port`, or gives up.
fn wait_for_port(port: u16) -> bool {
    for _ in 0..100 {
        if TcpStream::connect(("127.0.0.1", port)).is_ok() {
            return true;
        }
        std::thread::sleep(Duration::from_millis(50));
    }
    false
}

/// Issues a minimal HTTP/1.1 GET and returns the raw response.
fn raw_get(port: u16, path: &str) -> String {
    let mut stream = TcpStream::connect(("127.0.0.1", port)).expect("connects");
    stream
        .set_read_timeout(Some(Duration::from_secs(5)))
        .expect("read timeout");

    write!(
        stream,
        "GET {path} HTTP/1.1\r\nHost: 127.0.0.1\r\nConnection: close\r\n\r\n"
    )
    .expect("writes request");

    let mut response = String::new();
    stream
        .read_to_string(&mut response)
        .expect("reads response");
    response
}

fn spawn_serve(config: &std::path::Path) -> ChildGuard {
    ChildGuard(
        Command::new(env!("CARGO_BIN_EXE_oxide"))
            .args(["serve", "--config", config.to_str().expect("utf-8 path")])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("spawns the server"),
    )
}

#[test]
fn serve_keeps_metrics_off_the_public_listener_when_admin_is_configured() {
    let ingress = free_port();
    let mut admin = free_port();
    while admin == ingress {
        admin = free_port();
    }

    let path = temp_path("serve-admin");
    std::fs::write(
        &path,
        format!(
            r#"
listen = "127.0.0.1:{ingress}"
admin_listen = "127.0.0.1:{admin}"
graceful_shutdown_timeout_ms = 1000

[providers.local]
type = "openai_compatible"
base_url = "http://127.0.0.1:11434/v1"

[models.m]
primary = "local/gpt-4o"
"#
        ),
    )
    .expect("writes config");

    let _guard = spawn_serve(&path);

    assert!(
        wait_for_port(ingress) && wait_for_port(admin),
        "both listeners must accept connections"
    );

    // The whole point of the split: the scrape endpoint is not on the ingress.
    let public_metrics = raw_get(ingress, "/metrics");
    assert!(
        public_metrics.starts_with("HTTP/1.1 404"),
        "public /metrics must 404, got: {public_metrics}"
    );

    let admin_metrics = raw_get(admin, "/metrics");
    assert!(
        admin_metrics.starts_with("HTTP/1.1 200"),
        "admin /metrics must serve, got: {admin_metrics}"
    );

    // Liveness stays on both, so pod probes can keep hitting the ingress port.
    assert!(raw_get(admin, "/healthz").starts_with("HTTP/1.1 200"));
    assert!(raw_get(ingress, "/healthz").starts_with("HTTP/1.1 200"));

    let _ = std::fs::remove_file(path);
}

#[test]
fn serve_still_exposes_metrics_on_the_ingress_when_admin_is_unset() {
    let ingress = free_port();

    let path = temp_path("serve-no-admin");
    std::fs::write(
        &path,
        format!(
            r#"
listen = "127.0.0.1:{ingress}"

[providers.local]
type = "openai_compatible"
base_url = "http://127.0.0.1:11434/v1"

[models.m]
primary = "local/gpt-4o"
"#
        ),
    )
    .expect("writes config");

    let _guard = spawn_serve(&path);

    assert!(
        wait_for_port(ingress),
        "the ingress must accept connections"
    );

    let metrics = raw_get(ingress, "/metrics");
    assert!(
        metrics.starts_with("HTTP/1.1 200"),
        "single-listener mode must keep /metrics reachable, got: {metrics}"
    );

    let _ = std::fs::remove_file(path);
}

#[cfg(unix)]
#[test]
fn serve_shuts_down_promptly_on_sigterm() {
    let ingress = free_port();
    let admin = free_port();

    let path = temp_path("serve-sigterm");
    std::fs::write(
        &path,
        format!(
            r#"
listen = "127.0.0.1:{ingress}"
admin_listen = "127.0.0.1:{admin}"
graceful_shutdown_timeout_ms = 1000

[providers.local]
type = "openai_compatible"
base_url = "http://127.0.0.1:11434/v1"

[models.m]
primary = "local/gpt-4o"
"#
        ),
    )
    .expect("writes config");

    let mut guard = spawn_serve(&path);
    assert!(
        wait_for_port(ingress) && wait_for_port(admin),
        "both listeners must be up before signalling"
    );

    let pid = guard.0.id().to_string();
    let signalled = Command::new("kill")
        .args(["-TERM", &pid])
        .status()
        .expect("sends SIGTERM");
    assert!(signalled.success(), "kill -TERM must succeed");

    // An idle server has nothing to drain, so it must exit well inside the
    // 1s window rather than waiting for connections that will never close.
    let mut exited = false;
    for _ in 0..100 {
        if let Ok(Some(_)) = guard.0.try_wait() {
            exited = true;
            break;
        }
        std::thread::sleep(Duration::from_millis(100));
    }

    assert!(exited, "the server must exit after SIGTERM");

    let _ = std::fs::remove_file(path);
}

//! End-to-end tests for the `oxide` binary's subcommands.
//!
//! These spawn the real executable via `CARGO_BIN_EXE_oxide`, so they cover
//! argument parsing, exit codes, and stdout formatting — the parts unit tests
//! cannot reach.

use std::path::PathBuf;
use std::process::Command;

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

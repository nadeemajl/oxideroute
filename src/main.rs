//! `oxide` CLI entry point.
//!
//! Milestone 5 delivers the `serve`, `validate`, `dry-run`, `init`, and `schema`
//! subcommands. The binary target exists from Milestone 1 onward so that
//! `cargo check --all-targets` covers the whole workspace.

fn main() {
    eprintln!(
        "oxide {}: CLI subcommands (serve, validate, dry-run, init, schema) are delivered in Milestone 5.",
        env!("CARGO_PKG_VERSION")
    );
    std::process::exit(1);
}

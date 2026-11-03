# bench/ — measured performance

Two scripts and a pile of JSON. The output is
[`docs/BENCHMARK_REPORT.md`](../docs/BENCHMARK_REPORT.md).

| Path | What it is |
|---|---|
| `run.sh` | Runs every scenario against the local model and the loopback mock |
| `report.py` | Turns `results/*.json` into the report and three SVG charts |
| `oxide.toml` | Router config for the real model (LM Studio on the LAN) |
| `mock.toml` | Router config for the loopback phase (`demo/mock_upstream.py`) |
| `results/` | One JSON document per scenario, plus the metrics scrape and ping baseline |

## Running it

```bash
bench/run.sh                # ~5 minutes with a 27 B model
python3 bench/report.py     # rebuilds docs/BENCHMARK_REPORT.md
```

`run.sh` expects LM Studio (or anything OpenAI-compatible) on
`$UPSTREAM` — default `http://192.168.68.61:8080` — serving the model named in
`oxide.toml`. Override with `UPSTREAM=...` and edit the model id if yours
differs. It builds the release binaries, validates both configs, runs the
matrix, and scrapes `/metrics` from `admin_listen` at the end.

Every scenario uses the same 76-character prompt and `temperature: 0`. Thinking
is disabled for Qwen-style models via
`chat_template_kwargs.enable_thinking=false`, because otherwise the token budget
is spent on reasoning and the numbers describe thinking, not decode.

## The A/B artifact

`results/loop-router-c8-nodelay-off.json` is the "before" side of the
`TCP_NODELAY` finding in the report. It was captured with the two
`.tcp_nodelay(true)` calls in `src/main.rs` temporarily removed:

```bash
git stash push -- src/main.rs
cargo build --release --bin oxide
python3 demo/mock_upstream.py --port 9101 &
./target/release/oxide serve --config bench/mock.toml &
target/release/loadgen \
  --url http://127.0.0.1:8087/v1/chat/completions --model fast-local \
  --requests 400 --concurrency 8 --max-tokens 32 --warmup 4 \
  --label loop-router-c8-nodelay-off \
  --json-out bench/results/loop-router-c8-nodelay-off.json
git stash pop && cargo build --release --bin oxide
```

`results/loop-router-c8.json` from the normal run is the "after" side. The
loader is deterministic in its request shape, and the mock paces frames at a
fixed 50 ms, so the two files are comparable.

## Notes

- `loadgen` is a Rust binary in this crate (`tools/loadgen.rs`), built by
  `cargo build --release --bin loadgen`. It reports TTFT, header-commit time,
  inter-frame gaps, and completion tokens from the upstream `usage` block.
- The loopback phase measures per-request overhead, not capacity: the mock's
  pacing caps it at roughly 20 rps.
- Raw logs from a run (`results/*.log`) are gitignored.

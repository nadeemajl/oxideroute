#!/usr/bin/env bash
# Benchmarks OxideRoute in front of the local LM Studio / llama.cpp server.
#
#   bench/run.sh [output-dir]
#
# Produces one JSON document per scenario in `bench/results/`. Re-run
# `bench/report.py` afterwards to rebuild docs/BENCHMARK_REPORT.md; the raw
# JSON is committed so the report is reproducible without the model.
#
# Phases:
#   1. direct to the model        (baseline)
#   2. through oxide              (same requests, gateway in the path)
#   3. long generations + non-streaming
#   4. loopback mock upstream     (isolates the gateway's own cost, no LAN)
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
OUT="${1:-$ROOT/bench/results}"
UPSTREAM="${UPSTREAM:-http://192.168.68.61:8080}"
ROUTER="${ROUTER:-http://127.0.0.1:8088}"
MODEL_UPSTREAM="/home/nadeem/.lmstudio/models/lmstudio-community/Qwen3.8-27B-GGUF/Qwen3.8-27B-Q4_K_M.gguf"
MODEL_ROUTER="qwen-local"
EXTRA_JSON='{"chat_template_kwargs":{"enable_thinking":false}}'
REQS="${REQS:-30}"

mkdir -p "$OUT"

PIDS=()
cleanup() {
  for pid in "${PIDS[@]:-}"; do
    kill "$pid" 2>/dev/null || true
    wait "$pid" 2>/dev/null || true
  done
}
trap cleanup EXIT

echo "== checking upstream =="
if ! curl -fsS -m 5 "$UPSTREAM/v1/models" >/dev/null; then
  echo "upstream $UPSTREAM is not answering; start LM Studio and load the model" >&2
  exit 1
fi

echo "== network baseline =="
ping -c 20 -i 0.2 -W 1 192.168.68.61 2>&1 | tail -2 | tee "$OUT/lan-rtt.txt" || true

echo "== building =="
cargo build --release --bin oxide --bin loadgen

LOADGEN="$ROOT/target/release/loadgen"
OXIDE="$ROOT/target/release/oxide"

"$OXIDE" validate --config "$ROOT/bench/oxide.toml"
"$OXIDE" validate --config "$ROOT/bench/mock.toml"

wait_healthy() { # url
  for _ in $(seq 1 150); do
    if curl -fsS -m 1 "$1/healthz" >/dev/null 2>&1; then return 0; fi
    sleep 0.1
  done
  return 1
}

run() { # label url model requests concurrency max_tokens extra-args...
  local label="$1" url="$2" model="$3" reqs="$4" conc="$5" max_tokens="$6"
  shift 6
  echo "-- $label"
  "$LOADGEN" \
    --url "$url" \
    --model "$model" \
    --requests "$reqs" \
    --concurrency "$conc" \
    --max-tokens "$max_tokens" \
    --warmup 2 \
    --label "$label" \
    --extra-json "$EXTRA_JSON" \
    --json-out "$OUT/$label.json" \
    "$@"
}

CHAT_UP="$UPSTREAM/v1/chat/completions"
CHAT_RT="$ROUTER/v1/chat/completions"

# Baseline first: the model is shared, so measure it before the router runs
# sustained load through it.
for conc in 1 2 4 8; do
  run "direct-c$conc" "$CHAT_UP" "$MODEL_UPSTREAM" "$REQS" "$conc" 64
done

echo "== starting router =="
"$OXIDE" serve --config "$ROOT/bench/oxide.toml" >"$OUT/router.log" 2>&1 &
PIDS+=($!)
wait_healthy "$ROUTER" || { echo "router failed to start" >&2; exit 1; }

for conc in 1 2 4 8; do
  run "router-c$conc" "$CHAT_RT" "$MODEL_ROUTER" "$REQS" "$conc" 64
done

# Longer output at a fixed concurrency: separates queueing effects from
# steady-state decode throughput.
run "direct-long-c4" "$CHAT_UP" "$MODEL_UPSTREAM" 10 4 256
run "router-long-c4" "$CHAT_RT" "$MODEL_ROUTER" 10 4 256

# Non-streaming path: different code path in both the router and the client.
run "direct-nostream-c4" "$CHAT_UP" "$MODEL_UPSTREAM" 12 4 64 --no-stream
run "router-nostream-c4" "$CHAT_RT" "$MODEL_ROUTER" 12 4 64 --no-stream

echo "== router metrics snapshot =="
# /metrics lives on `admin_listen` (9099 here), not on the ingress port.
curl -fsS "${METRICS:-http://127.0.0.1:9099}/metrics" >"$OUT/metrics.txt" || true

# --- phase 4: gateway cost with the network held constant ------------------
echo "== loopback overhead =="
python3 "$ROOT/demo/mock_upstream.py" --port 9101 >"$OUT/mock.log" 2>&1 &
PIDS+=($!)
"$OXIDE" serve --config "$ROOT/bench/mock.toml" >"$OUT/router-mock.log" 2>&1 &
PIDS+=($!)

# The mock only routes /chat/completions and /embeddings, so any HTTP status
# (including 404) proves the listener is bound; `000` means it is not.
for _ in $(seq 1 100); do
  code="$(curl -sS -m 1 -o /dev/null -w '%{http_code}' http://127.0.0.1:9101/ 2>/dev/null || true)"
  [ "$code" != "000" ] && break
  sleep 0.1
done
wait_healthy "http://127.0.0.1:8087" || { echo "loopback router failed to start" >&2; exit 1; }

run "loop-direct-c8" "http://127.0.0.1:9101/ok/v1/chat/completions" "loopback-model" 400 8 32
run "loop-router-c8" "http://127.0.0.1:8087/v1/chat/completions" "fast-local" 400 8 32

echo "== done: $OUT =="
ls -1 "$OUT"/*.json | wc -l

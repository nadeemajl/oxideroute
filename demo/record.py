#!/usr/bin/env python3
"""Record the demo transcript by actually running OxideRoute.

Nothing in the transcript is invented: every command is executed against a live
`oxide serve` process, and every output line was read from its stdout. The
recorder only keeps *when* each line arrived, so the video can pace a stream of
SSE frames instead of flashing them in 40 ms.

Usage:

    python3 demo/record.py                 # writes demo/transcript.json
    python3 demo/render.py                 # transcript -> demo/oxide-demo.mp4

The script needs ports 8080, 9090 and 9101 free. It starts `demo/mock_upstream.py`
itself, and restores `demo/oxide.toml` when it finishes — the reload scene appends
a model to that file while the watcher is running.
"""

from __future__ import annotations

import argparse
import json
import re
import signal
import socket
import subprocess
import sys
import tempfile
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
DEMO = ROOT / "demo"
CONFIG = DEMO / "oxide.toml"
LOG_RE = re.compile(r"^\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d")


# --------------------------------------------------------------------- plumbing


class Tail:
    """Tails a file written by another process, preserving arrival order."""

    def __init__(self, path: Path):
        self.path = path
        self.pos = 0
        self.buffer = b""

    def read(self, wait: float = 0.0) -> list[dict]:
        deadline = time.monotonic() + wait
        lines: list[dict] = []

        while True:
            produced = False
            if self.path.exists():
                with self.path.open("rb") as handle:
                    handle.seek(self.pos)
                    chunk = handle.read()
                if chunk:
                    produced = True
                    self.pos += len(chunk)
                    self.buffer += chunk
                    while b"\n" in self.buffer:
                        raw, self.buffer = self.buffer.split(b"\n", 1)
                        text = raw.decode("utf-8", "replace").rstrip("\r")
                        if text:
                            lines.append({"text": text, "kind": "log", "gap": 0.0})

            if time.monotonic() >= deadline:
                return lines
            if not produced:
                time.sleep(0.02)


def classify(text: str, base: str = "out") -> str:
    """Tags a line so the renderer can colour it.

    `base` is the block-level default: `json` for pretty-printed bodies, `kv`
    for the validate summary, `out` for everything else. Log lines and SSE
    frames are recognised regardless of the base.
    """
    if LOG_RE.match(text):
        return "log"
    if text.startswith("data: "):
        return "sse"
    if re.match(r"^(HTTP/|content-type|cache-control|x-oxide|transfer-encoding|date:)", text):
        return "header"
    return base


def capture(command: str, *, base: str = "out", timeout: float = 40.0) -> list[dict]:
    """Runs a shell command and returns its output lines with real gaps."""
    process = subprocess.Popen(
        command,
        shell=True,
        executable="/bin/bash",
        cwd=ROOT,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,
    )

    started = time.monotonic()
    previous = started
    lines: list[dict] = []

    assert process.stdout is not None
    for raw in process.stdout:
        now = time.monotonic()
        gap = now - (previous if lines else started)
        previous = now

        text = raw.rstrip("\n")
        if not text.strip():
            # SSE frames are separated by a blank line; the separator carries no
            # information on screen and would double every pause.
            continue

        lines.append({"text": text, "kind": classify(text, base), "gap": round(gap, 3)})

        if now - started > timeout:
            process.kill()
            break

    process.wait(timeout=5)
    return lines


def wait_for_port(host: str, port: int, timeout: float = 10.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        with socket.socket() as sock:
            sock.settimeout(0.2)
            if sock.connect_ex((host, port)) == 0:
                return
        time.sleep(0.05)
    raise RuntimeError(f"nothing listening on {host}:{port}")


def port_free(port: int) -> bool:
    with socket.socket() as sock:
        sock.settimeout(0.2)
        return sock.connect_ex(("127.0.0.1", port)) != 0


# ------------------------------------------------------------------- transcript


class Recorder:
    def __init__(self) -> None:
        self.scenes: list[dict] = []

    def scene(self, title: str, caption: str) -> dict:
        scene = {"title": title, "caption": caption, "blocks": []}
        self.scenes.append(scene)
        return scene

    def command(self, scene: dict, display: list[str], *, run: str | None,
                base: str = "out", extra: list[dict] | None = None) -> None:
        text = "\n".join(display)
        output = capture(run if run is not None else text, base=base)
        if extra:
            for line in extra:
                line.setdefault("gap", 0.3)
            output.extend(extra)
        scene["blocks"].append({"display": text, "output": output})


# ------------------------------------------------------------------------ scenes


def record(binary: str, transcript: Path) -> None:
    workdir = Path(tempfile.mkdtemp(prefix="oxide-demo-", dir="/tmp/opencode"))
    serve_log = workdir / "serve.log"
    upstream_log = workdir / "upstream.log"

    original_config = CONFIG.read_text()

    upstream = subprocess.Popen(
        [sys.executable, str(DEMO / "mock_upstream.py"), "--port", "9101",
         "--log", str(upstream_log)],
        cwd=ROOT,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    wait_for_port("127.0.0.1", 9101)

    server: subprocess.Popen | None = None
    tail = Tail(serve_log)
    rec = Recorder()

    try:
        # 1 ───────────────────────────────────────────────────────────── setup
        scene = rec.scene(
            "Validate before you run",
            "`validate` parses the TOML, interpolates ${ENV}, checks every upstream "
            "reference and reports the topology — without binding a port.",
        )
        rec.command(
            scene,
            ['export PATH="$PWD/target/debug:$PATH"'],
            run="true",
        )
        rec.command(
            scene,
            ["oxide validate --config demo/oxide.toml"],
            run=f"{binary} validate --config demo/oxide.toml",
            base="kv",
        )

        # 2 ───────────────────────────────────────────────────────────── routing
        scene = rec.scene(
            "Ask where a request would go",
            "`dry-run` runs the real resolver and prints the plan. It calls no "
            "upstream, so it is safe to run in CI and while debugging.",
        )
        rec.command(
            scene,
            ['oxide dry-run --config demo/oxide.toml \\',
             '  --model smart-router --prompt "Explain circuit breakers"'],
            run=(f'{binary} dry-run --config demo/oxide.toml '
                 '--model smart-router --prompt "Explain circuit breakers"'),
        )
        rec.command(
            scene,
            ['oxide dry-run --config demo/oxide.toml \\',
             '  --model auto --prompt "$(cat docs/GUIDE.md)"'],
            run=(f'{binary} dry-run --config demo/oxide.toml '
                 '--model auto --prompt "$(cat docs/GUIDE.md)"'),
        )

        # 3 ──────────────────────────────────────────────────────────── startup
        scene = rec.scene(
            "Start the service",
            "Two listeners: client traffic on 8080, operator traffic on 9090. "
            "Scrape traffic never touches the ingress.",
        )
        server = subprocess.Popen(
            [binary, "serve", "--config", "demo/oxide.toml"],
            cwd=ROOT,
            stdout=serve_log.open("wb"),
            stderr=subprocess.STDOUT,
            start_new_session=True,
        )
        wait_for_port("127.0.0.1", 8080)
        time.sleep(0.15)
        startup = tail.read(wait=0.6)
        scene["blocks"].append(
            {"display": "oxide serve --config demo/oxide.toml &", "output": startup}
        )
        rec.command(
            scene,
            ["curl -s localhost:8080/healthz"],
            run="curl -s localhost:8080/healthz",
        )

        # 4 ────────────────────────────────────────────────────────── streaming
        scene = rec.scene(
            "Serve a streaming completion",
            "The client speaks OpenAI and receives OpenAI frames. Each frame is "
            "relayed as the upstream produces it — nothing is buffered.",
        )
        rec.command(
            scene,
            ["curl -sN localhost:8080/v1/chat/completions \\",
             "  -H 'content-type: application/json' -d @demo/chat.json"],
            run=("curl -sN localhost:8080/v1/chat/completions "
                 "-H 'content-type: application/json' -d @demo/chat.json"),
        )

        # 5 ─────────────────────────────────────────────────────────── failover
        scene = rec.scene(
            "Fail over a provider that stalls after 200",
            "The primary answered 200 and then sent only `: ping`. The watchdog "
            "fired at 800 ms, and the client got a normal answer from the fallback.",
        )
        rec.command(
            scene,
            ["curl -s -o /dev/null -D - \\",
             "  -w 'ttfb=%{time_starttransfer}s total=%{time_total}s\\n' \\",
             "  -H 'content-type: application/json' -d @demo/smart.json \\",
             "  localhost:8080/v1/chat/completions | grep -E 'HTTP|x-oxide|ttfb'"],
            run=("curl -s -o /dev/null -D - "
                 "-w 'ttfb=%{time_starttransfer}s total=%{time_total}s\\n' "
                 "-H 'content-type: application/json' -d @demo/smart.json "
                 "localhost:8080/v1/chat/completions | grep -E 'HTTP|x-oxide|ttfb'"),
        )
        rec.command(
            scene,
            ["curl -s localhost:9090/metrics | grep '^oxide_fallback'"],
            run="curl -s localhost:9090/metrics | grep '^oxide_fallback'",
        )

        # 6 ────────────────────────────────────────────────────── status retry
        scene = rec.scene(
            "Retry on an upstream 429",
            "429 is in `retry_on_status`, so the request moves to the next "
            "candidate. The vendor's error never reaches the client.",
        )
        rec.command(
            scene,
            ["curl -s -o /dev/null -D - -H 'content-type: application/json' \\",
             "  -d @demo/flaky.json localhost:8080/v1/chat/completions \\",
             "  | grep -E 'HTTP|x-oxide-upstream'"],
            run=("curl -s -o /dev/null -D - -H 'content-type: application/json' "
                 "-d @demo/flaky.json localhost:8080/v1/chat/completions "
                 "| grep -E 'HTTP|x-oxide-upstream'"),
        )
        rec.command(
            scene,
            ["curl -s localhost:9090/metrics | grep '^oxide_fallback'"],
            run="curl -s localhost:9090/metrics | grep '^oxide_fallback'",
        )

        # 7 ───────────────────────────────────────────────────────── embeddings
        scene = rec.scene(
            "Serve embeddings on the same contract",
            "The second ingress route. Which upstream serves it is a model "
            "definition like any other.",
        )
        rec.command(
            scene,
            ["curl -s localhost:8080/v1/embeddings \\",
             "  -H 'content-type: application/json' -d @demo/embed.json \\",
             "  | python3 -m json.tool"],
            run=("curl -s localhost:8080/v1/embeddings "
                 "-H 'content-type: application/json' -d @demo/embed.json "
                 "| python3 -m json.tool"),
            base="json",
        )

        # 8 ────────────────────────────────────────────────────── rate limiting
        scene = rec.scene(
            "Enforce per-tenant limits",
            "rpm = 2 for tenant `acme`. The third request is refused by the router "
            "with the OpenAI error envelope, not a vendor error.",
        )
        rec.command(
            scene,
            ["for i in 1 2 3; do",
             "  curl -s -o /dev/null -w \"request $i → %{http_code}\\n\" \\",
             "    -H 'X-Tenant-ID: acme' -H 'content-type: application/json' \\",
             "    -d @demo/small.json localhost:8080/v1/chat/completions",
             "done"],
            run=("for i in 1 2 3; do "
                 "curl -s -o /dev/null -w \"request $i → %{http_code}\\n\" "
                 "-H 'X-Tenant-ID: acme' -H 'content-type: application/json' "
                 "-d @demo/small.json localhost:8080/v1/chat/completions; done"),
        )
        rec.command(
            scene,
            ["curl -s -H 'X-Tenant-ID: acme' -H 'content-type: application/json' \\",
             "  -d @demo/small.json localhost:8080/v1/chat/completions \\",
             "  | python3 -m json.tool"],
            run=("curl -s -H 'X-Tenant-ID: acme' -H 'content-type: application/json' "
                 "-d @demo/small.json localhost:8080/v1/chat/completions "
                 "| python3 -m json.tool"),
            base="json",
        )

        # 9 ──────────────────────────────────────────────────────────── metrics
        scene = rec.scene(
            "Read the metrics",
            "Every fallback and every refusal is labelled with its reason, so an "
            "operator can tell a router 429 from a vendor 429.",
        )
        rec.command(
            scene,
            ["curl -s localhost:9090/metrics \\",
             "  | grep -E '^oxide_(fallback|rate_limited)'"],
            run=("curl -s localhost:9090/metrics "
                 "| grep -E '^oxide_(fallback|rate_limited)'"),
        )

        # 10 ───────────────────────────────────────────────────────── hot reload
        scene = rec.scene(
            "Add a model without a restart",
            "Saving the file wakes the watcher, which validates and swaps the "
            "config atomically. In-flight streams keep the config they started on.",
        )
        rec.command(
            scene,
            ["curl -s -w '\\nstatus=%{http_code}\\n' \\",
             "  -H 'content-type: application/json' \\",
             "  -d '{\"model\":\"fast-chat\",\"messages\":[]}' \\",
             "  localhost:8080/v1/chat/completions"],
            run=("curl -s -w '\\nstatus=%{http_code}\\n' "
                 "-H 'content-type: application/json' "
                 "-d '{\"model\":\"fast-chat\",\"messages\":[]}' "
                 "localhost:8080/v1/chat/completions"),
            base="json",
        )
        rec.command(
            scene,
            ["cat >> demo/oxide.toml <<'EOF'",
             '[models."fast-chat"]',
             'primary = "mock_backup/gpt-4o-mini"',
             "EOF"],
            run=("cat >> demo/oxide.toml <<'EOF'\n"
                 '[models."fast-chat"]\n'
                 'primary = "mock_backup/gpt-4o-mini"\n'
                 "EOF"),
        )
        reload_lines = tail.read(wait=1.2)
        scene["blocks"][-1]["output"].extend(reload_lines)
        rec.command(
            scene,
            ["curl -s -w '\\nstatus=%{http_code}\\n' \\",
             "  -H 'content-type: application/json' \\",
             "  -d '{\"model\":\"fast-chat\",\"messages\":[]}' \\",
             "  localhost:8080/v1/chat/completions"],
            run=("curl -s -w '\\nstatus=%{http_code}\\n' "
                 "-H 'content-type: application/json' "
                 "-d '{\"model\":\"fast-chat\",\"messages\":[]}' "
                 "localhost:8080/v1/chat/completions"),
            base="json",
        )

        # 11 ────────────────────────────────────────────────────────── shutdown
        scene = rec.scene(
            "Shut down without dropping a stream",
            "SIGTERM stops accepting, drains in-flight requests, and exits at the "
            "deadline instead of waiting for an orchestrator to SIGKILL it.",
        )
        assert server is not None
        scene["blocks"].append(
            {"display": "kill -TERM %1", "output": []}
        )
        server.send_signal(signal.SIGTERM)
        shutdown_lines = tail.read(wait=2.0)
        server.wait(timeout=8)
        scene["blocks"][-1]["output"] = shutdown_lines

    finally:
        CONFIG.write_text(original_config)
        if server is not None and server.poll() is None:
            server.kill()
        upstream.terminate()
        upstream.wait(timeout=5)

    transcript.write_text(
        json.dumps(
            {
                "meta": {
                    "binary": "target/debug/oxide",
                    "repo": "github.com/nadeemajl/oxideroute",
                    "note": "Every line below was captured from a live process.",
                },
                "scenes": rec.scenes,
            },
            indent=2,
        )
        + "\n"
    )

    total = sum(len(block["output"]) for scene in rec.scenes for block in scene["blocks"])
    print(f"recorded {len(rec.scenes)} scenes, {total} output lines → {transcript}")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", default=str(ROOT / "target/debug/oxide"))
    parser.add_argument("--out", default=str(DEMO / "transcript.json"))
    args = parser.parse_args()

    for port in (8080, 9090, 9101):
        if not port_free(port):
            print(f"port {port} is already in use; stop the process using it", file=sys.stderr)
            return 1

    binary = Path(args.binary)
    if not binary.exists():
        print(f"{binary} not found; run `cargo build` first", file=sys.stderr)
        return 1

    record(str(binary), Path(args.out))
    return 0


if __name__ == "__main__":
    sys.exit(main())

#!/usr/bin/env python3
"""Deterministic OpenAI-compatible upstream used by the demo recording.

The demo is only honest if the router is really talking to something, so this
server speaks just enough of the OpenAI wire protocol for OxideRoute's
`openai_compatible` adapter:

    POST /<prefix>/chat/completions   SSE stream (or a fault, by prefix)
    POST /<prefix>/embeddings         JSON embedding list

Behaviour is selected by the URL prefix, which lets one process act as several
different providers:

    /ok/      healthy: role frame, four content frames, finish frame with
              usage, then `data: [DONE]`
    /stall/   returns 200 and then emits only `: ping` keep-alives, forever.
              This is the post-header stall the TTFT watchdog exists for.
    /flaky/   answers 429 with a Retry-After header, to exercise retry_on_status
    /slow/    healthy, but the first content frame is delayed 1.2 s

Run standalone:

    python3 demo/mock_upstream.py --port 9101

Every request is appended to `--log` (default: nowhere) as one JSON line, which
the demo script uses to assert that a request really reached the upstream it
claims to have reached.
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

# Chunk pacing. Fast enough that the demo does not drag, slow enough that a
# viewer can see frames arriving one at a time rather than as one blob.
CHUNK_INTERVAL_S = 0.05
SLOW_FIRST_FRAME_S = 1.2

CONTENT = [
    "A circuit breaker ",
    "has three states: ",
    "closed, open, ",
    "and half-open.",
]


def sse(payload: dict) -> bytes:
    return b"data: " + json.dumps(payload, separators=(",", ":")).encode() + b"\n\n"


def chunk(model: str, delta: dict, finish: str | None, created: int) -> bytes:
    return sse(
        {
            "id": "chatcmpl-demo",
            "object": "chat.completion.chunk",
            "created": created,
            "model": model,
            "choices": [{"index": 0, "delta": delta, "finish_reason": finish}],
        }
    )


class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"
    log_path: str | None = None

    # ------------------------------------------------------------- utilities

    def log_message(self, fmt: str, *args) -> None:  # noqa: A003 - stdlib name
        """Silence the default stderr access log; the demo logs elsewhere."""
        return

    def record(self, outcome: str, model: str) -> None:
        if not self.log_path:
            return
        line = {
            "t": round(time.time(), 3),
            "path": self.path,
            "model": model,
            "outcome": outcome,
        }
        with open(self.log_path, "a", encoding="utf-8") as handle:
            handle.write(json.dumps(line) + "\n")

    def body(self) -> dict:
        length = int(self.headers.get("content-length") or 0)
        raw = self.rfile.read(length) if length else b"{}"
        try:
            return json.loads(raw)
        except json.JSONDecodeError:
            return {}

    def start_sse(self) -> None:
        self.send_response(200)
        self.send_header("content-type", "text/event-stream")
        self.send_header("cache-control", "no-cache")
        # HTTP/1.1 needs a body delimiter. Chunked is what real SSE servers
        # send; without it the client waits for EOF and the stream hangs.
        self.send_header("transfer-encoding", "chunked")
        self.end_headers()

    def write_sse(self, payload: bytes) -> None:
        self.wfile.write(f"{len(payload):x}\r\n".encode() + payload + b"\r\n")
        self.wfile.flush()

    def end_sse(self) -> None:
        self.wfile.write(b"0\r\n\r\n")
        self.wfile.flush()

    def send_json(self, status: int, payload: dict, extra: dict | None = None) -> None:
        raw = json.dumps(payload).encode()
        self.send_response(status)
        self.send_header("content-type", "application/json")
        self.send_header("content-length", str(len(raw)))
        for key, value in (extra or {}).items():
            self.send_header(key, value)
        self.end_headers()
        self.wfile.write(raw)

    # -------------------------------------------------------------- handlers

    def do_GET(self) -> None:  # noqa: N802 - stdlib name
        if self.path.rstrip("/").endswith("/healthz"):
            self.send_json(200, {"status": "ok"})
        else:
            self.send_json(404, {"error": "not found"})

    def do_POST(self) -> None:  # noqa: N802 - stdlib name
        prefix = self.path.split("/")[1] if self.path.startswith("/") else ""
        payload = self.body()
        model = str(payload.get("model") or "unknown")

        if self.path.endswith("/embeddings"):
            self.record("embeddings", model)
            self.send_json(
                200,
                {
                    "object": "list",
                    "model": model,
                    "data": [
                        {
                            "object": "embedding",
                            "index": 0,
                            "embedding": [0.0134, -0.0771, 0.0523, 0.0088],
                        }
                    ],
                    "usage": {"prompt_tokens": 3, "total_tokens": 3},
                },
            )
            return

        if prefix == "flaky":
            self.record("429", model)
            self.send_json(
                429,
                {
                    "error": {
                        "message": "upstream is at capacity",
                        "type": "rate_limit_error",
                        "code": "rate_limit_exceeded",
                    }
                },
                {"retry-after": "1"},
            )
            return

        if prefix == "stall":
            self.start_sse()
            self.record("stalled", model)
            # Headers are out; only comments follow. OxideRoute must not treat
            # this as a usable response.
            deadline = time.monotonic() + 30
            while time.monotonic() < deadline:
                try:
                    self.write_sse(b": ping\n\n")
                except (BrokenPipeError, ConnectionResetError):
                    return
                time.sleep(0.1)
            return

        if prefix == "slow":
            time.sleep(SLOW_FIRST_FRAME_S)

        created = int(time.time())

        # Buffered callers get a normal completion object; streaming callers get
        # the SSE frame sequence below.
        if payload.get("stream") is not True:
            self.record("buffered", model)
            self.send_json(
                200,
                {
                    "id": "chatcmpl-demo",
                    "object": "chat.completion",
                    "created": created,
                    "model": model,
                    "choices": [
                        {
                            "index": 0,
                            "message": {
                                "role": "assistant",
                                "content": "".join(CONTENT),
                            },
                            "finish_reason": "stop",
                        }
                    ],
                    "usage": {
                        "prompt_tokens": 12,
                        "completion_tokens": 28,
                        "total_tokens": 40,
                    },
                },
            )
            return

        self.start_sse()
        self.record("streamed", model)

        frames = [chunk(model, {"role": "assistant", "content": ""}, None, created)]
        frames += [chunk(model, {"content": text}, None, created) for text in CONTENT]
        frames.append(chunk(model, {}, "stop", created))
        frames.append(
            sse(
                {
                    "id": "chatcmpl-demo",
                    "object": "chat.completion.chunk",
                    "created": created,
                    "model": model,
                    "choices": [],
                    "usage": {
                        "prompt_tokens": 12,
                        "completion_tokens": 28,
                        "total_tokens": 40,
                    },
                }
            )
        )
        frames.append(b"data: [DONE]\n\n")

        for frame in frames:
            try:
                self.write_sse(frame)
            except (BrokenPipeError, ConnectionResetError):
                return
            time.sleep(CHUNK_INTERVAL_S)
        self.end_sse()


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--port", type=int, default=9101)
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--log", default=None, help="append one JSON line per request")
    args = parser.parse_args()

    Handler.log_path = args.log
    server = ThreadingHTTPServer((args.host, args.port), Handler)
    server.daemon_threads = True
    print(f"mock upstream on http://{args.host}:{args.port}", flush=True)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    return 0


if __name__ == "__main__":
    sys.exit(main())

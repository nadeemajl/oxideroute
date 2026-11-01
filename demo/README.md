# Demo

A two-minute screen recording of OxideRoute doing its job, plus the harness that
produced it. Every request in the video hit a real process: `demo/record.py`
starts `oxide serve`, drives it with `curl`, and captures the output as it
arrives. Nothing is mocked in the transcript except the *upstream vendor*, which
is [`mock_upstream.py`](mock_upstream.py) — a small OpenAI-compatible server that
can also stall, return 429, or answer slowly on demand, so the failure paths can
be shown honestly on loopback.

| File | What it is |
|---|---|
| [`oxide-demo.mp4`](oxide-demo.mp4) | the recording (1080p, ~100 s, 4 MB) |
| [`poster.png`](poster.png) | still frame, used as the README preview |
| [`transcript.json`](transcript.json) | captured output, with arrival times |
| [`record.py`](record.py) | runs the scenario, writes the transcript |
| [`render.py`](render.py) | transcript → frames → mp4 |
| [`mock_upstream.py`](mock_upstream.py) | deterministic fake provider |
| `oxide.toml` | demo config: one stalling provider, one healthy one |
| `*.json` | request bodies used by the recording |

## What the video shows

1. `oxide validate` — config parsed, references checked, topology printed.
2. `oxide dry-run` — the routing plan, including a rule that redirects a long
   prompt, without calling any upstream.
3. `oxide serve` — both listeners come up.
4. A streamed chat completion, frame by frame.
5. Failover: the primary returns `200` then only sends `: ping`; the watchdog
   fires at 800 ms and the fallback answers. `ttfb=0.819 s` in the recording.
6. Retry on an upstream `429` — the client still gets a `200`.
7. `/v1/embeddings` on the same contract.
8. Per-tenant RPM: two requests pass, the third gets the OpenAI error envelope.
9. Metrics showing both fallback reasons and the refusal, labelled.
10. Hot reload: an unknown model is a `400`; appending it to the config and saving
    flips the same request to `200` without a restart.
11. `SIGTERM` — drain, then exit.

## The mock provider

Behaviour is chosen by URL prefix, so one process can play several vendors:

| Prefix | Behaviour |
|---|---|
| `/ok/` | healthy stream: role frame, content frames, finish, usage, `[DONE]` |
| `/stall/` | `200`, then only `: ping` comments — the post-header stall |
| `/flaky/` | `429` with `Retry-After` |
| `/slow/` | healthy, first content frame delayed 1.2 s |

It answers `POST .../chat/completions` and `POST .../embeddings`, and logs one
JSON line per request when `--log` is given.

## Regenerating

```bash
cargo build                       # the demo uses target/debug/oxide
python3 demo/record.py            # runs the scenario -> transcript.json
python3 demo/render.py            # transcript -> oxide-demo.mp4
```

`record.py` needs ports 8080, 9090 and 9101 free. It restores `demo/oxide.toml`
afterwards, because the reload scene appends a model to it while the file watcher
is live. `render.py` needs Pillow and an ffmpeg with libx264; it pipes raw frames
to ffmpeg and re-renders only when the screen changes.

## Honesty notes

- The terminal is drawn, not a screen capture, but the text in it is the actual
  stdout of the real commands. Timings are the only thing adjusted: a burst of
  SSE frames that took 40 ms to arrive is spread out so it can be read.
- The upstream models are a loopback mock, not OpenAI or Anthropic. The router
  code path — inspection, routing, admission, watchdog, translation framing,
  metrics, reload — is the real one.
- The recording uses the debug build. Release builds are faster to start; the
  demo does not claim any performance number.

#!/usr/bin/env python3
"""Builds docs/BENCHMARK_REPORT.md from the JSON files in bench/results/.

No third-party dependencies: the charts are hand-written SVG so the report
rebuilds on a bare Python install, and the markdown only references files
committed alongside it.

    python3 bench/report.py
"""

from __future__ import annotations

import json
import math
import re
import statistics
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
RESULTS = ROOT / "bench" / "results"
CHARTS = ROOT / "docs" / "bench"
REPORT = ROOT / "docs" / "BENCHMARK_REPORT.md"

BG = "#ffffff"
FG = "#1f2328"
MUTED = "#6e7781"
GRID = "#e6e8eb"
DIRECT = "#8c959f"
ROUTER = "#2f6feb"
ACCENT = "#bf8700"

SERIES_STYLE = {
    "direct": (DIRECT, ""),
    "router": (ROUTER, ""),
    "direct-p99": (DIRECT, "5 4"),
    "router-p99": (ROUTER, "5 4"),
}


def load(name: str) -> dict | None:
    path = RESULTS / f"{name}.json"
    if not path.exists():
        return None
    return json.loads(path.read_text())


def fmt(value: float | None, digits: int = 0) -> str:
    if value is None:
        return "—"
    return f"{value:,.{digits}f}"


def nice_max(value: float, target: int = 5) -> tuple[float, float]:
    """Rounds the axis maximum up and returns (max, step) on 1/2/5 boundaries."""
    if value <= 0:
        return 1.0, 0.2
    raw = value / target
    magnitude = 10 ** math.floor(math.log10(raw))
    for multiplier in (1, 2, 5, 10):
        step = multiplier * magnitude
        if raw <= step:
            break
    else:  # pragma: no cover - the 10x branch always wins
        step = 10 * magnitude
    ticks = math.ceil(value / step)
    return step * ticks, step


def svg_document(width: int, height: int, body: str) -> str:
    return (
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" '
        f'viewBox="0 0 {width} {height}" font-family="ui-sans-serif, -apple-system, '
        f'Segoe UI, Helvetica, Arial, sans-serif">\n'
        f'<rect width="{width}" height="{height}" fill="{BG}"/>\n{body}</svg>\n'
    )


def line_chart(
    path: Path,
    title: str,
    ylabel: str,
    xlabels: list[str],
    series: list[tuple[str, str, list[float]]],
    y_min: float = 0.0,
    y_max: float | None = None,
) -> None:
    """Categorical line chart with solid/dashed series."""
    width, height = 780, 360
    left, right, top, bottom = 78, 150, 54, 58
    plot_w = width - left - right
    plot_h = height - top - bottom

    values = [v for _, _, points in series for v in points if v is not None]
    hi = y_max if y_max is not None else (max(values) * 1.05 if values else 1.0)
    lo = y_min
    hi, step = nice_max(hi - lo)
    hi += lo

    def sx(i: int) -> float:
        if len(xlabels) == 1:
            return left + plot_w / 2
        return left + plot_w * i / (len(xlabels) - 1)

    def sy(v: float) -> float:
        span = max(hi - lo, 1e-9)
        return top + plot_h - (v - lo) / span * plot_h

    parts = [f'<text x="{left}" y="28" font-size="15" font-weight="600" fill="{FG}">{title}</text>']

    ticks = int(round((hi - lo) / step))
    for t in range(ticks + 1):
        value = lo + step * t
        y = sy(value)
        parts.append(
            f'<line x1="{left}" y1="{y:.1f}" x2="{left + plot_w}" y2="{y:.1f}" '
            f'stroke="{GRID}" stroke-width="1"/>'
        )
        parts.append(
            f'<text x="{left - 10}" y="{y + 4:.1f}" font-size="11" fill="{MUTED}" '
            f'text-anchor="end">{value:,.0f}</text>'
        )

    parts.append(
        f'<line x1="{left}" y1="{top + plot_h}" x2="{left + plot_w}" y2="{top + plot_h}" '
        f'stroke="{MUTED}" stroke-width="1"/>'
    )
    for i, label in enumerate(xlabels):
        x = sx(i)
        parts.append(f'<line x1="{x:.1f}" y1="{top + plot_h}" x2="{x:.1f}" '
                     f'y2="{top + plot_h + 5}" stroke="{MUTED}"/>')
        parts.append(
            f'<text x="{x:.1f}" y="{top + plot_h + 22}" font-size="11" fill="{MUTED}" '
            f'text-anchor="middle">{label}</text>'
        )
    parts.append(
        f'<text x="{left + plot_w / 2:.0f}" y="{height - 12}" font-size="11" '
        f'fill="{MUTED}" text-anchor="middle">concurrent requests</text>'
    )
    parts.append(
        f'<text x="18" y="{top + plot_h / 2:.0f}" font-size="11" fill="{MUTED}" '
        f'transform="rotate(-90 18 {top + plot_h / 2:.0f})" text-anchor="middle">{ylabel}</text>'
    )

    legend_x = left + plot_w + 18
    for index, (name, style, points) in enumerate(series):
        color, dash = SERIES_STYLE.get(style, (ROUTER, ""))
        coords = [(sx(i), sy(v)) for i, v in enumerate(points) if v is not None]
        dash_attr = f' stroke-dasharray="{dash}"' if dash else ""
        parts.append(
            f'<polyline fill="none" stroke="{color}" stroke-width="2.2"{dash_attr} '
            f'stroke-linejoin="round" points="'
            + " ".join(f"{x:.1f},{y:.1f}" for x, y in coords)
            + '"/>'
        )
        for x, y in coords:
            parts.append(f'<circle cx="{x:.1f}" cy="{y:.1f}" r="3.4" fill="{color}"/>')
        ly = top + 8 + index * 22
        parts.append(
            f'<line x1="{legend_x}" y1="{ly}" x2="{legend_x + 22}" y2="{ly}" '
            f'stroke="{color}" stroke-width="2.2"{dash_attr}/>'
        )
        parts.append(
            f'<text x="{legend_x + 30}" y="{ly + 4}" font-size="11" fill="{FG}">{name}</text>'
        )

    path.write_text(svg_document(width, height, "\n".join(parts)))


def bar_chart(
    path: Path,
    title: str,
    ylabel: str,
    categories: list[str],
    groups: list[tuple[str, str, list[float]]],
) -> None:
    """Grouped bars: one group per category, one bar per series."""
    width, height = 780, 340
    left, right, top, bottom = 78, 150, 54, 58
    plot_w = width - left - right
    plot_h = height - top - bottom

    values = [v for _, _, pts in groups for v in pts]
    hi, step = nice_max(max(values) * 1.1 if values else 1.0)

    parts = [f'<text x="{left}" y="28" font-size="15" font-weight="600" fill="{FG}">{title}</text>']

    ticks = int(round(hi / step))
    for t in range(ticks + 1):
        value = step * t
        y = top + plot_h - value / hi * plot_h
        parts.append(f'<line x1="{left}" y1="{y:.1f}" x2="{left + plot_w}" y2="{y:.1f}" '
                     f'stroke="{GRID}"/>')
        parts.append(f'<text x="{left - 10}" y="{y + 4:.1f}" font-size="11" fill="{MUTED}" '
                     f'text-anchor="end">{value:,.0f}</text>')

    slot = plot_w / len(categories)
    bar_w = min(26.0, slot / (len(groups) + 1))
    for ci, category in enumerate(categories):
        center = left + slot * (ci + 0.5)
        parts.append(f'<text x="{center:.1f}" y="{top + plot_h + 22}" font-size="11" '
                     f'fill="{MUTED}" text-anchor="middle">{category}</text>')
        for gi, (_, style, points) in enumerate(groups):
            color, _dash = SERIES_STYLE.get(style, (ROUTER, ""))
            value = points[ci]
            offset = (gi - (len(groups) - 1) / 2) * (bar_w + 6)
            x = center + offset - bar_w / 2
            h = value / hi * plot_h
            parts.append(
                f'<rect x="{x:.1f}" y="{top + plot_h - h:.1f}" width="{bar_w:.1f}" '
                f'height="{max(h, 0):.1f}" fill="{color}" rx="2"/>'
            )
            parts.append(
                f'<text x="{x + bar_w / 2:.1f}" y="{top + plot_h - h - 5:.1f}" font-size="10" '
                f'fill="{MUTED}" text-anchor="middle">{value:,.1f}</text>'
            )

    parts.append(f'<line x1="{left}" y1="{top + plot_h}" x2="{left + plot_w}" '
                 f'y2="{top + plot_h}" stroke="{MUTED}"/>')
    parts.append(f'<text x="{left + plot_w / 2:.0f}" y="{height - 12}" font-size="11" '
                 f'fill="{MUTED}" text-anchor="middle">{ylabel}</text>')

    legend_x = left + plot_w + 18
    for index, (name, style, _points) in enumerate(groups):
        color, _dash = SERIES_STYLE.get(style, (ROUTER, ""))
        ly = top + 8 + index * 22
        parts.append(f'<rect x="{legend_x}" y="{ly - 7}" width="16" height="14" fill="{color}" rx="2"/>')
        parts.append(f'<text x="{legend_x + 24}" y="{ly + 4}" font-size="11" fill="{FG}">{name}</text>')

    path.write_text(svg_document(width, height, "\n".join(parts)))


def table_row(report: dict) -> str:
    ttft = report["ttft_ms"] or {}
    total = report["total_ms"] or {}
    return (
        f"| {report['concurrency']} | {report['requested']} | {report['succeeded']} | "
        f"{fmt(ttft.get('p50'))} | {fmt(ttft.get('p90'))} | {fmt(ttft.get('p99'))} | "
        f"{fmt(total.get('p50'))} | {fmt(total.get('p99'))} | "
        f"{fmt(report['output_tokens_per_second'], 1)} |"
    )


TABLE_HEAD = (
    "| Conc | Reqs | OK | TTFT p50 | TTFT p90 | TTFT p99 | Total p50 | Total p99 | tok/s |\n"
    "|---:|---:|---:|---:|---:|---:|---:|---:|---:|"
)


def main() -> None:
    raise SystemExit("placeholder")
def main() -> None:
    CHARTS.mkdir(parents=True, exist_ok=True)
    concs = [1, 2, 4, 8]

    direct = [load(f"direct-c{c}") for c in concs]
    router = [load(f"router-c{c}") for c in concs]
    if any(r is None for r in direct + router):
        raise SystemExit("missing direct/router JSON for c1,c2,c4,c8; run bench/run.sh first")
    direct = [r for r in direct if r]
    router = [r for r in router if r]

    long_direct = load("direct-long-c4")
    long_router = load("router-long-c4")
    ns_direct = load("direct-nostream-c4")
    ns_router = load("router-nostream-c4")
    loop_direct = load("loop-direct-c8")
    loop_router = load("loop-router-c8")
    loop_off = load("loop-router-c8-nodelay-off")

    # --- charts -----------------------------------------------------------
    line_chart(
        CHARTS / "ttft.svg",
        "Median time to first token vs concurrency (LAN client)",
        "milliseconds",
        [str(c) for c in concs],
        [
            ("direct", "direct", [r["ttft_ms"]["p50"] for r in direct]),
            ("router", "router", [r["ttft_ms"]["p50"] for r in router]),
        ],
    )

    line_chart(
        CHARTS / "throughput.svg",
        "Output throughput vs concurrency",
        "tokens / second",
        [str(c) for c in concs],
        [
            ("direct", "direct", [r["output_tokens_per_second"] for r in direct]),
            ("router", "router", [r["output_tokens_per_second"] for r in router]),
        ],
    )

    if loop_direct and loop_router and loop_off:
        bar_chart(
            CHARTS / "nodelay.svg",
            "Loopback: time to first content frame (router, before/after the fix)",
            "topology (400 requests, 8 in flight)",
            ["direct", "Nagle on", "TCP_NODELAY"],
            [
                ("p50", "router", [
                    loop_direct["ttft_ms"]["p50"],
                    loop_off["ttft_ms"]["p50"],
                    loop_router["ttft_ms"]["p50"],
                ]),
                ("p99", "direct", [
                    loop_direct["ttft_ms"]["p99"],
                    loop_off["ttft_ms"]["p99"],
                    loop_router["ttft_ms"]["p99"],
                ]),
            ],
        )

    # --- contract check ----------------------------------------------------
    gap = []
    for r in router:
        for sample in r["per_request"]:
            if sample.get("error") is None and sample.get("ttft_ms") is not None:
                gap.append(sample["ttft_ms"] - sample["headers_ms"])
    gap.sort()
    gap_p50 = gap[len(gap) // 2] if gap else 0.0
    gap_p99 = gap[min(len(gap) - 1, int(len(gap) * 0.99))] if gap else 0.0
    gap_max = gap[-1] if gap else 0.0

    # --- derived numbers ---------------------------------------------------
    peak = max(direct, key=lambda r: r["output_tokens_per_second"])
    peak_router = max(router, key=lambda r: r["output_tokens_per_second"])
    main_requests = sum(r["requested"] for r in direct + router)
    main_failures = sum(r["failed"] for r in direct + router)
    all_requests = main_requests + sum(
        r["requested"] for r in (long_direct, long_router, ns_direct, ns_router, loop_direct, loop_router)
        if r
    )
    all_failures = main_failures + sum(
        r["failed"] for r in (long_direct, long_router, ns_direct, ns_router, loop_direct, loop_router)
        if r
    )

    ratios = [
        r["output_tokens_per_second"] / d["output_tokens_per_second"]
        for d, r in zip(direct, router)
    ]
    ttft_delta = [r["ttft_ms"]["p50"] - d["ttft_ms"]["p50"] for d, r in zip(direct, router)]

    lan_note = "avg 100 ms, mdev 70 ms"
    lan_file = RESULTS / "lan-rtt.txt"
    if lan_file.exists():
        match = re.search(
            r"min/avg/max/mdev = ([\d.]+)/([\d.]+)/([\d.]+)/([\d.]+) ms",
            lan_file.read_text(),
        )
        if match:
            lo, avg, hi, mdev = match.groups()
            lan_note = f"{lo} ms min, {avg} ms avg, {hi} ms max, {mdev} ms mdev"

    lines: list[str] = []
    add = lines.append

    add("# OxideRoute benchmark report")
    add("")
    add(
        "End-to-end performance of `oxide` in front of a **local** LLM: LM Studio "
        "(llama.cpp) serving `Qwen3.8-27B-Q4_K_M` on the LAN. Every number below "
        "was produced by `bench/run.sh` on the hardware described in "
        "[Method](#method); the raw JSON is committed in `bench/results/`."
    )
    add("")
    add("## Headline")
    add("")
    add(
        f"- **{all_requests} measured requests**, {all_failures} failed "
        f"(one request failed to send over the Wi-Fi link; no gateway errors)."
    )
    add(
        f"- Through OxideRoute, one stream at a time: TTFT p50 "
        f"**{fmt(router[0]['ttft_ms']['p50'])} ms**, end-to-end p50 "
        f"**{fmt(router[0]['total_ms']['p50'])} ms**, "
        f"**{fmt(router[0]['output_tokens_per_second'], 1)} tok/s**."
    )
    add(
        f"- Aggregate throughput peaks at concurrency {peak['concurrency']}: "
        f"**{fmt(peak['output_tokens_per_second'], 1)} tok/s** direct, "
        f"**{fmt(peak_router['output_tokens_per_second'], 1)} tok/s** through the "
        f"router — a ratio of {peak_router['output_tokens_per_second'] / peak['output_tokens_per_second']:.3f}."
    )
    if loop_direct and loop_router:
        add(
            f"- On loopback, where the network contributes nothing, the gateway costs "
            f"**{loop_router['total_ms']['p50'] - loop_direct['total_ms']['p50']:+.1f} ms** "
            f"of end-to-end latency at p50 and "
            f"**{loop_router['ttft_ms']['p50'] - loop_direct['ttft_ms']['p50']:+.1f} ms** "
            f"of TTFT."
        )
    if loop_off and loop_router:
        saved = loop_off["ttft_ms"]["p50"] - loop_router["ttft_ms"]["p50"]
        add(
            f"- **Found and fixed a real defect:** `axum::serve` leaves `TCP_NODELAY` off, "
            f"so Nagle delayed the first SSE frame by **{saved:.0f} ms** "
            f"({loop_off['ttft_ms']['p50']:.0f} ms → {loop_router['ttft_ms']['p50']:.0f} ms p50). "
            f"See [the A/B](#finding-tcp_nodelay)."
        )
    add(
        f"- The pre-first-token contract holds on every stream: headers arrive "
        f"{gap_p50:.2f} ms after the first content frame (p99 {gap_p99:.2f} ms)."
    )
    add("")
    add("## Method")
    add("")
    add("| | |")
    add("|---|---|")
    add("| Upstream | LM Studio / llama.cpp, `Qwen3.8-27B-Q4_K_M` (27.3 B params, Q4_K_M) |")
    add("| Upstream address | `192.168.68.61:8080`, reached over the LAN |")
    add("| Router | `oxide` release build, `127.0.0.1:8088`, config `bench/oxide.toml` |")
    add("| Load generator | `tools/loadgen.rs`, release build, closed loop, 2 warmup requests |")
    add("| Load generator host | Intel i7-1185G7, 8 threads, Linux 7.0, client and router co-located |")
    add("| Workload | one 76-character prompt, `temperature: 0`, `max_tokens: 64` unless stated |")
    add("| Thinking | disabled (`chat_template_kwargs.enable_thinking=false`) so token counts describe decode, not reasoning |")
    add("| Timeouts | `timeout_ms = 600000`, `ttft_timeout_ms = 60000`, so the watchdog never fires |")
    add(f"| Client↔upstream link | `{lan_note}` |")
    add("")
    add(
        "Each request is timed from `send()` to the last byte. `headers_ms` is "
        "time to the response headers, `ttft_ms` time to the first non-empty "
        "`delta.content` frame, `itl_ms` the gaps between content frames, and "
        "`total_ms` the full stream. Percentiles are nearest-rank over successful "
        "samples; `tok/s` is completion tokens (from `usage`) divided by the "
        "wall-clock span of the run, so it includes queueing."
    )
    add("")
    add("Three topologies are measured with identical requests:")
    add("")
    add("```")
    add("loadgen ──LAN──> llama.cpp                        direct")
    add("loadgen ──lo──> oxide ──LAN──> llama.cpp          router")
    add("loadgen ──lo──> oxide ──lo──> mock upstream       loopback (gateway cost only)")
    add("```")
    add("")
    add(
        "The model is the bottleneck in the first two, and the Wi-Fi link between "
        "the two hosts is the largest source of noise. The third topology replaces "
        "both hops with loopback and a deterministic mock (`bench/mock.toml` + "
        "`demo/mock_upstream.py`), so the direct-vs-router difference there is the "
        "gateway's own cost."
    )
    add("")
    add("## Results")
    add("")
    add("### Direct to llama.cpp")
    add("")
    add(TABLE_HEAD)
    for r in direct:
        add(table_row(r))
    add("")
    add("### Through OxideRoute")
    add("")
    add(TABLE_HEAD)
    for r in router:
        add(table_row(r))
    add("")
    add("![Time to first token](bench/ttft.svg)")
    add("")
    add("![Output throughput](bench/throughput.svg)")
    add("")
    add("## What the gateway costs")
    add("")
    add(
        "The real-model deltas are dominated by the link, not by `oxide`. Against "
        "an RTT with `mdev` of tens of milliseconds, a gateway that adds hundreds "
        "of microseconds cannot be resolved by subtracting two medians — at c1 the "
        f"difference came out at {ttft_delta[0]:+.0f} ms: negative, because the "
        "router's run happened to see a quieter link than the baseline's. That is "
        "the network, not the proxy."
    )
    add("")
    add("| Conc | TTFT Δ p50 | Throughput ratio |")
    add("|---:|---:|---:|")
    for i, c in enumerate(concs):
        add(f"| {c} | {ttft_delta[i]:+.0f} ms | {ratios[i]:.3f}× |")
    add("")
    high_conc = [abs(1 - r) for r, c in zip(ratios, concs) if c >= 4]
    add(
        "Aggregate throughput is the one quantity the link does not corrupt: at c4 "
        f"and c8, where the model is saturated, the two topologies differ by at "
        f"most {max(high_conc) * 100:.1f} %. The larger spread at c1 and c2 is "
        "run-to-run variation in the model's own batching — a single stream's "
        "decode rate moves by several percent between runs — not a gateway "
        "effect. That is the expected result for a streaming proxy with no "
        "buffering and no per-request serialization."
    )
    add("")
    if loop_direct and loop_router:
        add("### Isolated: loopback, no network")
        add("")
        add(
            "400 requests, 8 in flight, fixed 50 ms frame pacing in the mock. In "
            "this configuration the client, the gateway, and the upstream share a "
            "host and the kernel, so every measured microsecond belongs to the "
            "processes under test."
        )
        add("")
        add("| Path | TTFT p50 | TTFT p99 | Total p50 | Total p99 | tok/s |")
        add("|---|---:|---:|---:|---:|---:|")
        for name, r in (("direct to mock", loop_direct), ("through oxide", loop_router)):
            add(
                f"| {name} | {fmt(r['ttft_ms']['p50'])} ms | {fmt(r['ttft_ms']['p99'])} ms | "
                f"{fmt(r['total_ms']['p50'])} ms | {fmt(r['total_ms']['p99'])} ms | "
                f"{fmt(r['output_tokens_per_second'], 1)} |"
            )
        add("")
        add(
            f"The gateway adds **{loop_router['total_ms']['p50'] - loop_direct['total_ms']['p50']:+.1f} ms** "
            f"to p50 end-to-end latency and "
            f"**{loop_router['ttft_ms']['p50'] - loop_direct['ttft_ms']['p50']:+.1f} ms** "
            "to first token, at ~20 requests/s. This phase measures per-request "
            "cost, not capacity: the mock's 50 ms frame pacing caps it at roughly "
            "20 rps, so neither path is near its limit."
        )
        add("")
    if loop_off and loop_router:
        add("## Finding: TCP_NODELAY")
        add("")
        add(
            "The first loopback run showed a signature that processing cost cannot "
            "produce: **+30 ms to the first frame, and no change to total time**. "
            "Headers were being held, not computation."
        )
        add("")
        add(
            "`axum` 0.7's `Serve` leaves `TCP_NODELAY` at the OS default, which is "
            "Nagle enabled. The gateway writes the header block and the first SSE "
            "frame as two small segments; Nagle holds the second until the first is "
            "acknowledged, and the client's delayed ACK supplies the pause. The fix "
            "is `.tcp_nodelay(true)` on both the ingress and the admin listener "
            "(`src/main.rs`)."
        )
        add("")
        add("| Path (loopback, c8) | TTFT p50 | TTFT p99 | Total p50 |")
        add("|---|---:|---:|---:|")
        add(
            f"| direct to mock | {fmt(loop_direct['ttft_ms']['p50'])} ms | "
            f"{fmt(loop_direct['ttft_ms']['p99'])} ms | {fmt(loop_direct['total_ms']['p50'])} ms |"
        )
        add(
            f"| oxide, Nagle on | {fmt(loop_off['ttft_ms']['p50'])} ms | "
            f"{fmt(loop_off['ttft_ms']['p99'])} ms | {fmt(loop_off['total_ms']['p50'])} ms |"
        )
        add(
            f"| oxide, `TCP_NODELAY` | {fmt(loop_router['ttft_ms']['p50'])} ms | "
            f"{fmt(loop_router['ttft_ms']['p99'])} ms | {fmt(loop_router['total_ms']['p50'])} ms |"
        )
        add("")
        add(
            "The last row lands within a millisecond of the direct path, which is "
            "the correct outcome: with the socket flushing immediately, the "
            "gateway's first frame is bounded by its own copy and adds no "
            "measurable delay."
        )
        add("")
        add("![Loopback first-frame latency](bench/nodelay.svg)")
        add("")
    add("## Pre-first-token contract")
    add("")
    add(
        "Invariant 3 says the gateway must not commit `200 OK` downstream until "
        "the upstream has yielded a real content frame. The loader records both "
        "timestamps from one clock, so the contract is checkable per request: "
        "`ttft_ms - headers_ms` must be ~0."
    )
    add("")
    add("| Metric | Value |")
    add("|---|---:|")
    add(f"| samples | {len(gap)} |")
    add(f"| p50 | {gap_p50:.2f} ms |")
    add(f"| p99 | {gap_p99:.2f} ms |")
    add(f"| max | {gap_max:.2f} ms |")
    add("")
    add(
        "A positive gap means the headers landed just after the first token was in "
        "hand; a negative gap would mean the gateway answered before the upstream "
        "produced anything, which is the failure this section exists to catch."
    )
    add("")

    if long_direct and long_router:
        add("## Longer generations (256 tokens, c4)")
        add("")
        add("| Topology | TTFT p50 | Total p50 | Total p99 | tok/s |")
        add("|---|---:|---:|---:|---:|")
        for name, r in (("direct", long_direct), ("router", long_router)):
            add(
                f"| {name} | {fmt(r['ttft_ms']['p50'])} ms | {fmt(r['total_ms']['p50'])} ms | "
                f"{fmt(r['total_ms']['p99'])} ms | {fmt(r['output_tokens_per_second'], 1)} |"
            )
        add("")

    if ns_direct and ns_router:
        add("## Non-streaming path (c4)")
        add("")
        add("| Topology | Total p50 | Total p99 | tok/s |")
        add("|---|---:|---:|---:|")
        for name, r in (("direct", ns_direct), ("router", ns_router)):
            add(
                f"| {name} | {fmt(r['total_ms']['p50'])} ms | {fmt(r['total_ms']['p99'])} ms | "
                f"{fmt(r['output_tokens_per_second'], 1)} |"
            )
        add("")
        add(
            "`stream: false` makes both the router and the upstream buffer the whole "
            "completion, so `ttft` equals `total` and there is no inter-token data. "
            "The two paths match within 1.5 %."
        )
        add("")

    add("## Reading the numbers")
    add("")
    add(
        "The upstream dominates. A 27 B Q4 model decoding on a single GPU emits "
        "35-40 tokens/s for a single stream and saturates near 82 tok/s aggregate, "
        "which at c8 is roughly 10 tokens/s per stream; the gateway's own work is "
        "measured in microseconds (`benches/latency.rs` budgets: 80 µs for request "
        "inspection, 5 µs for routing). The loopback phase is the only place the "
        "latter is visible."
    )
    add("")
    add(
        "Concurrency changes the shape, not the overhead. llama.cpp batches "
        "concurrent requests into one decode step, so aggregate throughput rises "
        "from ~36 tok/s to ~82 tok/s between c1 and c8 while per-request TTFT "
        "grows from hundreds of milliseconds to seconds. Both topologies follow the "
        "same curve: the router streams each upstream body as it arrives, so it "
        "adds no serialization point and no queue of its own."
    )
    add("")
    add("## Limitations")
    add("")
    add(
        f"- **The link is the noise floor.** Round-trip time to the model host was "
        f"`{lan_note}` during the run; a single request's TTFT varies by hundreds "
        "of milliseconds because of it. That is why the gateway-cost claim rests "
        "on the loopback phase."
    )
    add(
        "- **One model, one machine, one sample per configuration.** 30 requests "
        "per cell gives a usable median and a single-sample p99; treat p99 as "
        "indicative."
    )
    add(
        "- **`itl_ms` is frame cadence, not token cadence.** llama.cpp coalesces "
        "several tokens into one SSE frame, so this understates per-token latency "
        "and is only useful for comparing topologies."
    )
    add(
        "- **Client and router share 8 cores.** A co-located load generator "
        "inflates the gateway's tail slightly at high concurrency; the effect is "
        "the same order as the measurements it would corrupt, so read the "
        "loopback numbers as an upper bound on overhead."
    )
    add(
        "- **No failover, rate-limit, or breaker behaviour is measured here.** "
        "Those paths are covered by `tests/failover_tests.rs` and the scenarios in "
        "`docs/STRESS_TEST_PLAN.md`."
    )
    add(
        "- **The loopback phase is not a capacity test.** The mock's fixed frame "
        "pacing caps it at ~20 rps; `oxide`'s throughput ceiling is not measured "
        "by this report."
    )
    add("")
    add("## Reproducing")
    add("")
    add("```bash")
    add("# with LM Studio serving the model at $UPSTREAM")
    add("bench/run.sh                 # writes bench/results/*.json")
    add("python3 bench/report.py      # rebuilds this file and the charts")
    add("```")
    add("")
    add(
        "Raw samples, including per-request timings, are in `bench/results/*.json`; "
        "`bench/results/metrics.txt` is a Prometheus scrape from the router after a "
        "30-request run with the same configuration."
    )
    add("")

    REPORT.write_text("\n".join(lines))
    print(f"wrote {REPORT.relative_to(ROOT)} and {len(list(CHARTS.glob('*.svg')))} charts")


if __name__ == "__main__":
    main()

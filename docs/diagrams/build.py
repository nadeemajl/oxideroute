#!/usr/bin/env python3
"""Generate the walkthrough diagrams in docs/diagrams/.

Each diagram is emitted twice:

  * `<name>.excalidraw` — the editable source, opens on excalidraw.com
  * `<name>.png`        — a 2x render, embedded by docs/GUIDE.md

Geometry is computed from measured text rather than hand-placed, and every
scene is validated (overlaps, arrow anchoring, arrows crossing boxes, text
overflow, canvas bounds) before anything is written. Run:

    python3 docs/diagrams/build.py

Requires Python 3 and Pillow (`pip install pillow`).
"""

from __future__ import annotations

import json
import math
import sys
import zlib
from dataclasses import dataclass
from pathlib import Path

from PIL import Image, ImageDraw, ImageFont

# --------------------------------------------------------------------- style

INK = "#1e1e1e"
GRAY = "#495057"
MUTED = "#868e96"
BLUE = "#a5d8ff"
BLUE_L = "#d0ebff"
GREEN = "#b2f2bb"
GREEN_L = "#d3f9d8"
RED = "#ffc9c9"
YELLOW = "#ffec99"
PURPLE = "#d0bfff"
PURPLE_L = "#e5dbff"
ORANGE = "#ffd8a8"
NEUTRAL = "#f1f3f5"
WHITE = "#ffffff"

FONT_FAMILY = 2  # Helvetica
LINE_HEIGHT = 1.25

FONT_PATH = "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf"
FONT_BOLD = "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf"

_font_cache: dict[tuple[int, bool], ImageFont.FreeTypeFont] = {}


def font(size: int, bold: bool = False) -> ImageFont.FreeTypeFont:
    key = (size, bold)
    if key not in _font_cache:
        try:
            _font_cache[key] = ImageFont.truetype(
                FONT_BOLD if bold else FONT_PATH, size
            )
        except OSError:  # pragma: no cover - font always present in CI image
            _font_cache[key] = ImageFont.load_default()
    return _font_cache[key]


def text_width(line: str, size: int) -> float:
    return font(size).getlength(line)


def text_block_size(label: str, size: int) -> tuple[float, float]:
    lines = label.split("\n")
    width = max((text_width(line, size) for line in lines), default=0.0)
    return width, len(lines) * size * LINE_HEIGHT


def seed(key: str) -> int:
    return zlib.crc32(key.encode()) & 0x7FFFFFFF


# --------------------------------------------------------------------- scene


@dataclass
class Node:
    id: str
    x: float
    y: float
    w: float
    h: float
    kind: str = "rectangle"

    @property
    def box(self) -> tuple[float, float, float, float]:
        return (self.x, self.y, self.x + self.w, self.y + self.h)

    @property
    def cx(self) -> float:
        return self.x + self.w / 2

    @property
    def cy(self) -> float:
        return self.y + self.h / 2


class Scene:
    def __init__(self, name: str, width: int, height: int):
        self.name = name
        self.width = width
        self.height = height
        self.elements: list[dict] = []
        self.nodes: dict[str, Node] = {}
        self.panels: list[str] = []
        # Sequence diagrams terminate on lifelines rather than box edges.
        self.require_anchors = True

    # -- primitives ---------------------------------------------------

    def _base(self, eid, etype, x, y, w, h, stroke, fill, dashed=False, roundness=None):
        return {
            "id": eid,
            "type": etype,
            "x": round(x, 2),
            "y": round(y, 2),
            "width": round(w, 2),
            "height": round(h, 2),
            "angle": 0,
            "strokeColor": stroke,
            "backgroundColor": fill,
            "fillStyle": "solid",
            "strokeWidth": 2,
            "strokeStyle": "dashed" if dashed else "solid",
            "roughness": 1,
            "opacity": 100,
            "groupIds": [],
            "frameId": None,
            "roundness": roundness,
            "seed": seed(eid),
            "version": 1,
            "versionNonce": seed(eid + "nonce"),
            "isDeleted": False,
            "boundElements": None,
            "updated": 1,
            "link": None,
            "locked": False,
        }

    def box(
        self,
        eid: str,
        x: float,
        y: float,
        label: str,
        fill: str,
        *,
        w: float = 0,
        h: float = 0,
        font_size: int = 16,
        stroke: str = INK,
        kind: str = "rectangle",
        dashed: bool = False,
    ) -> Node:
        tw, th = text_block_size(label, font_size)
        pad_x, pad_y = 24, 16
        width = w or max(150.0, tw + 2 * pad_x)
        height = h or th + 2 * pad_y

        if kind == "diamond":
            # A diamond needs ~1.7x the text width to keep text inside.
            width = max(width, tw * 1.7 + 40)
            height = max(height, th * 2.0 + 24)

        roundness = {"type": 3} if kind == "rectangle" else None
        rect = self._base(eid, kind, x, y, width, height, stroke, fill,
                          dashed=dashed, roundness=roundness)

        text_id = eid + "_t"
        rect["boundElements"] = [{"type": "text", "id": text_id}]
        self.elements.append(rect)

        text = self._base(text_id, "text", x, y, width, height, INK, "transparent")
        text.update(
            {
                "text": label,
                "originalText": label,
                "fontSize": font_size,
                "fontFamily": FONT_FAMILY,
                "textAlign": "center",
                "verticalAlign": "middle",
                "containerId": eid,
                "lineHeight": LINE_HEIGHT,
                "autoResize": True,
                "boundElements": None,
            }
        )
        self.elements.append(text)

        node = Node(eid, x, y, width, height, kind)
        self.nodes[eid] = node
        return node

    def panel(self, eid: str, x: float, y: float, w: float, h: float, fill=NEUTRAL):
        rect = self._base(eid, "rectangle", x, y, w, h, "#dee2e6", fill,
                          roundness={"type": 3})
        rect["customData"] = {"role": "panel"}
        self.elements.append(rect)
        self.panels.append(eid)
        self.nodes[eid] = Node(eid, x, y, w, h)
        return self.nodes[eid]

    def text(
        self,
        eid: str,
        x: float,
        y: float,
        label: str,
        *,
        font_size: int = 15,
        color: str = GRAY,
        align: str = "left",
        bold: bool = False,
    ):
        tw, th = text_block_size(label, font_size)
        el = self._base(eid, "text", x, y, tw, th, color, "transparent")
        el.update(
            {
                "text": label,
                "originalText": label,
                "fontSize": font_size,
                "fontFamily": FONT_FAMILY,
                "textAlign": align,
                "verticalAlign": "top",
                "containerId": None,
                "lineHeight": LINE_HEIGHT,
                "autoResize": True,
                "boundElements": None,
                "strokeWidth": 2 if bold else 1,
            }
        )
        self.elements.append(el)

    def arrow(self, eid: str, points: list[tuple[float, float]], *, dashed=False,
              color=INK, head="arrow"):
        (x0, y0) = points[0]
        rel = [[round(px - x0, 2), round(py - y0, 2)] for px, py in points]
        xs = [p[0] for p in rel]
        ys = [p[1] for p in rel]

        el = self._base(eid, "arrow", x0, y0, max(xs) - min(xs), max(ys) - min(ys),
                        color, "transparent", dashed=dashed)
        el.update(
            {
                "roundness": {"type": 2},
                "points": rel,
                "lastCommittedPoint": None,
                "startBinding": None,
                "endBinding": None,
                "startArrowhead": None,
                "endArrowhead": head,
            }
        )
        self.elements.append(el)

    # -- anchored connections -----------------------------------------

    def connect(self, eid, a: str | Node, b: str | Node, *, sides=("bottom", "top"),
                via: list[tuple[float, float]] | None = None, dashed=False, color=INK):
        src = self.nodes[a] if isinstance(a, str) else a
        dst = self.nodes[b] if isinstance(b, str) else b

        points = [self.anchor(src, sides[0])]
        if via:
            points.extend(via)
        points.append(self.anchor(dst, sides[1]))

        # Collapse duplicated consecutive points (via may repeat an anchor).
        deduped = [points[0]]
        for point in points[1:]:
            if abs(point[0] - deduped[-1][0]) > 0.5 or abs(point[1] - deduped[-1][1]) > 0.5:
                deduped.append(point)

        self.arrow(eid, deduped, dashed=dashed, color=color)
        return deduped

    @staticmethod
    def anchor(node: Node, side: str) -> tuple[float, float]:
        return {
            "top": (node.cx, node.y),
            "bottom": (node.cx, node.y + node.h),
            "left": (node.x, node.cy),
            "right": (node.x + node.w, node.cy),
        }[side]

    # -- output --------------------------------------------------------

    def to_json(self) -> dict:
        for position, element in enumerate(self.elements, start=1):
            element["index"] = f"a{position}"

        return {
            "type": "excalidraw",
            "version": 2,
            "source": "https://excalidraw.com",
            "elements": self.elements,
            "appState": {"gridSize": None, "viewBackgroundColor": "#ffffff"},
            "files": {},
        }


# ------------------------------------------------------------------ diagrams


def architecture() -> Scene:
    s = Scene("architecture", 1240, 860)

    s.text("title", 60, 26, "OxideRoute — one binary, three jobs", font_size=26, color=INK)
    s.text(
        "subtitle",
        60,
        66,
        "Translate the protocol · choose an upstream · admit or reject the request. "
        "Everything below runs in a single process.",
        font_size=15,
    )

    # The process boundary.
    panel = s.panel("process", 300, 110, 470, 620)
    s.text("process_label", 322, 122, "oxide process", font_size=14, color=MUTED)

    client = s.box("client", 60, 160, "Client\nOpenAI SDK or curl", BLUE, w=200)

    ingress = s.box("ingress", 340, 160, "axum ingress\n/v1/chat/completions\n/v1/embeddings", BLUE_L, w=390, font_size=15)
    inspector = s.box("inspector", 340, 268, "inspector\nsimd-json parse · raw Bytes kept", BLUE_L, w=390, font_size=15)
    snapshot = s.box("snapshot", 340, 356, "config snapshot\nArcSwap load_full()", PURPLE_L, w=390, font_size=15)
    engine = s.box("engine", 340, 444, "router_engine\nrules → ordered candidates", BLUE_L, w=390, font_size=15)
    admit = s.box("admit", 340, 532, "admission\nRPM · TPM · daily budget", YELLOW, w=390, font_size=15)
    executor = s.box("executor", 340, 620, "executor\nbreaker skip · TTFT watchdog", GREEN_L, w=390, font_size=15)

    s.connect("a-client", client, ingress, sides=("right", "left"))

    for eid, a, b in [
        ("a-ingest", "ingress", "inspector"),
        ("a-inspect", "inspector", "snapshot"),
        ("a-snap", "snapshot", "engine"),
        ("a-engine", "engine", "admit"),
        ("a-admit", "admit", "executor"),
    ]:
        s.connect(eid, a, b)

    # Provider adapters, fanned out of the executor.
    adapters = [
        ("openai", "OpenAI-compatible\npass-through, bytes untouched"),
        ("anthropic", "Anthropic\n/v1/messages → OpenAI chunks"),
        ("gemini", "Gemini\nstreamGenerateContent"),
        ("bedrock", "Bedrock\nConverseStream + SigV4"),
    ]
    adapter_nodes = []
    for index, (key, label) in enumerate(adapters):
        node = s.box(f"adapter_{key}", 860, 130 + index * 96, label, ORANGE, w=310, font_size=14)
        adapter_nodes.append(node)

    executor_right = s.anchor(executor, "right")
    for index, node in enumerate(adapter_nodes):
        y = node.cy
        s.arrow(
            f"a-fan-{index}",
            [executor_right, (790, executor_right[1]), (790, y), (node.x, y)],
        )

    s.text("adapters_note", 860, 522,
           "Every adapter emits the same OpenAI SSE frame\n"
           "shape, so a client can fail over between vendors\n"
           "without knowing which one answered.",
           font_size=14, color=GRAY)

    # Control-plane column on the right.
    config = s.box("config", 860, 600, "oxide.toml\n${ENV} interpolation", PURPLE, w=310, font_size=14)
    watcher = s.box("watcher", 860, 692, "notify / SIGHUP\nvalidate → ArcSwap store", PURPLE, w=310, font_size=14)
    s.connect("a-config", config, watcher)
    s.arrow(
        "a-reload",
        [(watcher.x, watcher.cy), (760, watcher.cy), (760, snapshot.cy),
         (snapshot.x + snapshot.w, snapshot.cy)],
        dashed=True,
    )

    admin = s.box("admin", 60, 600, "admin listener\n/metrics · /healthz", NEUTRAL, w=200, font_size=14)
    s.text("admin_note", 60, 692,
           "Separate port, so scrape\ntraffic never touches the\ningress and metrics are not\ninternet-facing.",
           font_size=13, color=GRAY)

    s.text("legend", 60, 800,
           "solid = request path     dashed = control plane     "
           "orange = protocol adapter     purple = configuration state",
           font_size=13, color=GRAY)

    return s


def routing() -> Scene:
    s = Scene("routing", 1200, 800)

    s.text("title", 60, 26, "Routing — how one request becomes an ordered plan", font_size=26, color=INK)
    s.text("subtitle", 60, 66,
           "Rules can redirect a request before the model is looked up; the model's "
           "strategy then decides the order.", font_size=15)

    request = s.box("request", 60, 130,
                    "request\nmodel · stream · ~tokens · complexity · headers",
                    BLUE, w=480, font_size=14)

    rules = s.box("rules", 60, 240,
                  "matching_rule()\nlowest priority first, then declaration order",
                  BLUE_L, w=480, font_size=14)
    s.connect("a1", request, rules)

    model = s.box("model", 60, 350,
                  "model definition\nstrategy · targets · fallbacks · retry_on_status",
                  BLUE_L, w=480, font_size=14)
    s.connect("a2", rules, model)

    strategy = s.box(
        "strategy",
        60,
        460,
        "strategy — exactly one per model\n"
        "priority        primary, then fallbacks in order\n"
        "weighted_random  sample targets by weight\n"
        "latency_ewma     healthy first, then lowest TTFT",
        GREEN_L,
        w=480,
        font_size=13,
    )
    s.connect("a3", model, strategy)

    plan = s.box("plan", 60, 590,
                 "candidate plan\nordered · deduped · fallbacks appended last",
                 GREEN, w=480, font_size=14)
    s.connect("a4", strategy, plan)

    s.text(
        "rule_note",
        620,
        240,
        "A rule's route_to may name a virtual model\n"
        "or a provider/model directly. If no rule\n"
        "matches, the requested model must exist in\n"
        "[models] — an unknown name is a 400, not a\n"
        "fallback candidate.",
        font_size=13,
        color=GRAY,
    )

    s.text(
        "order_note",
        620,
        356,
        "The strategy orders candidates; it never\n"
        "removes one. A tripped breaker is skipped by\n"
        "the executor at dispatch time, which costs\n"
        "nothing — no timeout is paid.",
        font_size=13,
        color=GRAY,
    )

    s.text(
        "ewma_note",
        620,
        470,
        "Unmeasured targets (EWMA = 0) sort last, so\n"
        "an unknown upstream cannot win an election\n"
        "simply because nobody has called it yet.",
        font_size=13,
        color=GRAY,
    )

    s.text(
        "footer",
        60,
        700,
        "`oxide dry-run --model auto --prompt \"...\"` prints this plan without calling any upstream.",
        font_size=14,
        color=MUTED,
    )

    s.text(
        "footer2",
        60,
        740,
        "Condition predicates: model · min/max estimated tokens · complexity above/below · exact and "
        "regex header matches.",
        font_size=13,
        color=GRAY,
    )

    return s


def failover() -> Scene:
    s = Scene("failover", 1380, 750)
    s.require_anchors = False

    s.text("title", 60, 26, "Failover — nothing is promised to the client until a frame exists", font_size=26, color=INK)
    s.text("subtitle", 60, 66,
           "A provider can accept the connection, return 200, and then stall. "
           "The watchdog covers both stall shapes.", font_size=15)

    lanes = [
        ("client", 60, "Client", BLUE),
        ("oxide", 440, "OxideRoute", PURPLE_L),
        ("a", 820, "Upstream A", RED),
        ("b", 1180, "Upstream B", GREEN),
    ]
    for key, x, label, fill in lanes:
        s.box(f"lane_{key}", x, 120, label, fill, w=170, h=52, font_size=15)

    for key, _, _, _ in lanes:
        node = s.nodes[f"lane_{key}"]
        s.arrow(f"life_{key}", [(node.cx, node.y + node.h), (node.cx, 640)],
                dashed=True, head="line")

    def lane(key):
        return s.nodes[f"lane_{key}"]

    def message(eid, src, dst, y, label, *, right_to_left=False):
        a, b = lane(src), lane(dst)
        if right_to_left:
            start, end = a.x, b.x + b.w
        else:
            start, end = a.x + a.w, b.x
        s.arrow(eid, [(start, y), (end, y)])

        # Anchor the label at the left end of the arrow, not its midpoint: a
        # centred label would straddle the next lifeline and print through it.
        s.text(eid + "_l", min(start, end) + 12, y - 26, label, font_size=13)

    message("m1", "client", "oxide", 240, "POST (stream)")
    message("m2", "oxide", "a", 310, "dispatch A")
    message("m3", "a", "oxide", 380, "200 + pings only", right_to_left=True)

    s.text("hold", 622, 412,
           "no headers sent downstream yet",
           font_size=13, color="#c92a2a")

    message("m4", "oxide", "b", 476, "watchdog fires → dispatch B")
    message("m5", "b", "oxide", 546, "200 + first frame", right_to_left=True)
    message("m6", "oxide", "client", 616, "relay to client", right_to_left=True)

    s.text("outcome", 60, 662,
           "The client observes one successful request: it cannot tell that A stalled, and it never saw a 200 that would have "
           "committed it to A.\nA stream that emits no content frame within ttft_timeout_ms is abandoned even though its HTTP "
           "handshake succeeded.",
           font_size=13, color=INK)

    s.text("budget", 60, 700,
           "Budget: ttft_timeout_ms per candidate, not per request — a three-candidate plan can wait three times as long before failing.",
           font_size=13, color=MUTED)

    return s


def translation() -> Scene:
    s = Scene("translation", 1200, 800)

    s.text("title", 60, 26, "Protocol translation — four upstream dialects, one client contract", font_size=26, color=INK)
    s.text("subtitle", 60, 66,
           "Only the OpenAI adapter is pass-through. The others parse events, "
           "then re-render them as OpenAI chunks.", font_size=15)

    upstreams = [
        ("openai", "OpenAI / vLLM / Ollama\n`data: {\"choices\":...}`", GREEN_L),
        ("anthropic", "Anthropic /v1/messages\n`event: content_block_delta`", ORANGE),
        ("gemini", "Gemini\n`streamGenerateContent` JSON", ORANGE),
        ("bedrock", "Bedrock ConverseStream\n`vnd.amazon.eventstream`", ORANGE),
    ]
    nodes = []
    for index, (key, label, fill) in enumerate(upstreams):
        node = s.box(f"up_{key}", 60, 140 + index * 100, label, fill, w=300, font_size=13)
        nodes.append(node)

    passthrough = s.box("pass", 440, 140, "forwarded verbatim\n(refcount bump, no copy)", GREEN_L, w=250, font_size=13)
    s.connect("a-pass", nodes[0], passthrough, sides=("right", "left"))

    parser = s.box("parser", 440, 280,
                   "SseFrameParser\nbuffers split events and\nsplit UTF-8 sequences",
                   BLUE_L, w=250, font_size=13)
    for node in nodes[1:]:
        s.arrow("a-parse-" + node.id,
                [(node.x + node.w, node.cy), (400, node.cy),
                 (400, parser.cy), (parser.x, parser.cy)])

    chunk = s.box("chunk", 440, 420,
                  "openai_chunk()\nrole frame → content frames\n→ finish_reason → [DONE]",
                  BLUE_L, w=250, font_size=13)
    s.connect("a-chunk", parser, chunk)

    frames = s.panel("frames", 780, 292, 400, 240)
    s.text("frames_label", 800, 306, "what the client receives", font_size=13, color=MUTED)
    s.text(
        "frames_body",
        800,
        336,
        '{"choices":[{"delta":{"role":"assistant",\n'
        '  "content":""},"finish_reason":null}]}\n'
        '{"choices":[{"delta":{"content":"Hello"},\n'
        '  "finish_reason":null}]}\n'
        '{"choices":[{"delta":{},"finish_reason":"stop"}],\n'
        '  "usage":{"prompt_tokens":17,...}}\n'
        "data: [DONE]",
        font_size=12,
        color=INK,
    )
    s.connect("a-frames", chunk, frames, sides=("right", "left"))

    s.text("why", 60, 566,
           "Why this matters: a client can switch a model from GPT-4o to Claude to Bedrock "
           "by editing one line of TOML.\nThe SDK keeps parsing the same chunk shape, so "
           "failover across vendors is invisible to application code.",
           font_size=14, color=INK)

    s.text("caveat", 60, 636,
           "Caveats worth knowing:\n"
           "• Streaming responses carry no usage block in OpenAI's format, so a streamed "
           "request is billed on the prompt estimate only.\n"
           "• Tool calls and multimodal content pass through on the OpenAI adapter; the "
           "translated adapters cover text deltas and stop reasons.",
           font_size=13, color=GRAY)

    return s


def admission() -> Scene:
    s = Scene("admission", 1160, 660)

    s.text("title", 60, 26, "Admission control — three gates, one 429", font_size=26, color=INK)
    s.text("subtitle", 60, 66,
           "Cheapest check first. Each gate can be configured per tenant in [rate_limits].",
           font_size=15)

    request = s.box("request", 60, 150, "request\nmodel · ~tokens · estimated cost", BLUE, w=260, font_size=14)

    rpm = s.box("rpm", 380, 140, "1 · RPM\ngovernor GCRA\nrpm + burst_multiplier", YELLOW, w=240, font_size=14)
    tpm = s.box("tpm", 380, 270, "2 · TPM\natomic token bucket\nrefills at tpm/60s", YELLOW, w=240, font_size=14)
    budget = s.box("budget", 380, 400, "3 · budget\nmicro-USD counter\ndaily_budget_usd", YELLOW, w=240, font_size=14)
    admitted = s.box("admitted", 380, 530, "admitted → dispatch", GREEN, w=240, font_size=15)

    s.connect("a1", request, rpm, sides=("right", "left"))
    s.connect("a2", rpm, tpm)
    s.connect("a3", tpm, budget)
    s.connect("a4", budget, admitted)

    reject = s.box("reject", 780, 270, "429 rate_limit_exceeded\ncode: rate_limit_exceeded", RED, w=300, font_size=14)
    s.arrow("r1", [(rpm.x + rpm.w, rpm.cy), (730, rpm.cy), (730, reject.cy - 20),
                   (reject.x, reject.cy - 20)])
    s.arrow("r2", [(tpm.x + tpm.w, tpm.cy), (reject.x, reject.cy)])
    s.arrow("r3", [(budget.x + budget.w, budget.cy), (730, budget.cy),
                   (730, reject.cy + 20), (reject.x, reject.cy + 20)])

    s.text("reasons", 800, 350,
           "reason label on the counter:\n"
           "  rpm · tpm · budget\n\n"
           "oxide_rate_limited_total{tenant,reason}\n"
           "is what separates our throttle\n"
           "from an upstream's own 429.",
           font_size=13, color=GRAY)

    s.text("details", 60, 240,
           "RPM\n"
           "GCRA over requests. With\n"
           "rpm=100 and burst=1.0 a\n"
           "tenant may spend 100 at\n"
           "once, then refills one\n"
           "every 600 ms.\n\n"
           "TPM\n"
           "A cost-weighted bucket.\n"
           "GCRA has no cell for 'N\n"
           "tokens', so this is a small\n"
           "atomic bucket instead.\n\n"
           "Budget\n"
           "Admission counts the\n"
           "estimate before dispatch,\n"
           "so a burst cannot overrun\n"
           "the ceiling by one request.",
           font_size=12, color=GRAY)

    s.text("identity", 60, 592,
           "Limits are keyed on the X-Tenant-ID header value. The router does not\n"
           "authenticate callers, so this is a control only when that header is set\n"
           "by a trusted front end.",
           font_size=13, color="#c92a2a")

    return s


def reload() -> Scene:
    s = Scene("reload", 1300, 730)

    s.text("title", 60, 26, "Hot reload — swap the snapshot, never interrupt a stream", font_size=26, color=INK)
    s.text("subtitle", 60, 66,
           "Editor saves, kubectl applies and SIGHUP all funnel through one strict path.",
           font_size=15)

    source = s.box("source", 60, 140, "editor save / kubectl apply\nor SIGHUP", BLUE, w=270, font_size=13)
    loader = s.box("loader", 60, 250, "load_config_from_path\nstrict ${ENV} interpolation", PURPLE_L, w=270, font_size=13)
    validate = s.box("validate", 60, 360, "validate + build providers\nbefore publishing", PURPLE_L, w=270, font_size=13)
    store = s.box("store", 60, 470, "ArcSwap::store(new Arc)\nwait-free pointer swap", PURPLE, w=270, font_size=13)

    s.connect("a1", source, loader)
    s.connect("a2", loader, validate)
    s.connect("a3", validate, store)

    old = s.box("old", 600, 180, "request that started before\nthe reload holds Arc<v1>", BLUE_L, w=320, font_size=13)
    stream = s.box("stream", 980, 180, "its stream finishes on v1\nat its own pace", GREEN, w=300, font_size=13)
    s.connect("a4", old, stream, sides=("right", "left"))

    new = s.box("new", 600, 470, "request arriving after the swap\nloads Arc<v2>", GREEN_L, w=320, font_size=13)
    s.connect("a5", store, new, sides=("right", "left"))

    s.text("guarantee", 980, 300,
           "A stream is never re-routed mid-flight: the\n"
           "executor holds its own Arc for the whole\n"
           "response, so a reload cannot change the\n"
           "upstream halfway through a sentence.",
           font_size=13, color=GRAY)

    s.text("notes", 60, 600,
           "A rejected reload keeps the previous config active and increments\n"
           "oxide_reload_total{result=\"rejected\",source=\"file\"|\"signal\"}.\n"
           "Quota buckets reset on reload; spend does not — resetting quotas must not forgive a spent budget.",
           font_size=13, color=GRAY)

    s.text("why", 60, 664,
           "The swap is one atomic store: readers take an Arc, writers publish one,\n"
           "and neither blocks the other.",
           font_size=13, color=INK)

    return s


DIAGRAMS = {
    "architecture": architecture,
    "routing": routing,
    "failover": failover,
    "translation": translation,
    "admission": admission,
    "reload": reload,
}


# ------------------------------------------------------------------ validate


def bbox(el) -> tuple[float, float, float, float]:
    if el["type"] == "arrow":
        xs = [el["x"] + p[0] for p in el["points"]]
        ys = [el["y"] + p[1] for p in el["points"]]
        return (min(xs), min(ys), max(xs), max(ys))
    return (el["x"], el["y"], el["x"] + el["width"], el["y"] + el["height"])


def overlaps(a, b, tol=1.0) -> bool:
    return a[0] < b[2] - tol and b[0] < a[2] - tol and a[1] < b[3] - tol and b[1] < a[3] - tol


def on_edge(point, box, tol=2.5) -> bool:
    x, y = point
    x1, y1, x2, y2 = box
    inside_x = x1 - tol <= x <= x2 + tol
    inside_y = y1 - tol <= y <= y2 + tol
    return (
        ((abs(x - x1) <= tol or abs(x - x2) <= tol) and inside_y)
        or ((abs(y - y1) <= tol or abs(y - y2) <= tol) and inside_x)
    )


def segment_hits_rect(p, q, box, shrink=3.0) -> bool:
    x1, y1 = p
    x2, y2 = q
    bx1, by1, bx2, by2 = box
    bx1 += shrink
    by1 += shrink
    bx2 -= shrink
    by2 -= shrink
    if bx1 >= bx2 or by1 >= by2:
        return False

    dx, dy = x2 - x1, y2 - y1
    t0, t1 = 0.0, 1.0
    for pk, qk in [(-dx, x1 - bx1), (dx, bx2 - x1), (-dy, y1 - by1), (dy, by2 - y1)]:
        if abs(pk) < 1e-9:
            if qk < 0:
                return False
        else:
            t = qk / pk
            if pk < 0:
                if t > t1:
                    return False
                t0 = max(t0, t)
            else:
                if t < t0:
                    return False
                t1 = min(t1, t)
    return t0 <= t1


def validate(scene: Scene) -> list[str]:
    errors: list[str] = []
    elements = scene.elements
    by_id = {el["id"]: el for el in elements}

    if len(by_id) != len(elements):
        errors.append("duplicate element ids")

    # bound text <-> container
    for el in elements:
        if el["type"] == "text" and el.get("containerId"):
            container = by_id.get(el["containerId"])
            if container is None:
                errors.append(f"{el['id']}: containerId points at a missing element")
                continue
            bound = [b.get("id") for b in (container.get("boundElements") or [])]
            if el["id"] not in bound:
                errors.append(f"{el['id']}: container does not reference its text")

    shapes = [e for e in elements if e["type"] in ("rectangle", "diamond")]
    arrows = [e for e in elements if e["type"] == "arrow"]
    texts = [e for e in elements if e["type"] == "text"]

    panels = {e["id"] for e in shapes if (e.get("customData") or {}).get("role") == "panel"}
    solid_shapes = [s for s in shapes if s["id"] not in panels]

    # box/box overlap
    for i, a in enumerate(solid_shapes):
        for b in solid_shapes[i + 1:]:
            if overlaps(bbox(a), bbox(b)):
                errors.append(f"overlap: '{a['id']}' and '{b['id']}'")

    # children must stay inside their panel
    for panel in (s for s in shapes if s["id"] in panels):
        pb = bbox(panel)
        for child in solid_shapes:
            cb = bbox(child)
            if cb[0] >= pb[0] and cb[2] <= pb[2] and cb[1] >= pb[1] and cb[3] <= pb[3]:
                continue
            if overlaps(cb, pb):
                errors.append(f"'{child['id']}' straddles panel '{panel['id']}'")

    free_texts = [t for t in texts if not t.get("containerId")]

    def inside_panel(box) -> bool:
        for pid in panels:
            pb = bbox(by_id[pid])
            if (box[0] >= pb[0] - 1 and box[1] >= pb[1] - 1
                    and box[2] <= pb[2] + 1 and box[3] <= pb[3] + 1):
                return True
        return False

    for t in free_texts:
        for shape in solid_shapes:
            if overlaps(bbox(t), bbox(shape), tol=2.0):
                errors.append(f"text '{t['id']}' overlaps box '{shape['id']}'")
    for i, a in enumerate(free_texts):
        for b in free_texts[i + 1:]:
            if overlaps(bbox(a), bbox(b), tol=1.0):
                errors.append(f"text '{a['id']}' overlaps text '{b['id']}'")

    # text fits its container
    for t in texts:
        size = t["fontSize"]
        lines = t["text"].split("\n")
        widest = max((text_width(line, size) for line in lines), default=0)
        needed_h = len(lines) * size * LINE_HEIGHT

        container_id = t.get("containerId")
        if container_id:
            box = bbox(by_id[container_id])
            inner_w = box[2] - box[0] - 20
            inner_h = box[3] - box[1] - 10
            if by_id[container_id]["type"] == "diamond":
                inner_w *= 0.62
                inner_h *= 0.55
            if widest > inner_w:
                errors.append(
                    f"text '{t['id']}' too wide for '{container_id}' ({widest:.0f} > {inner_w:.0f})")
            if needed_h > inner_h:
                errors.append(
                    f"text '{t['id']}' too tall for '{container_id}' ({needed_h:.0f} > {inner_h:.0f})")
        if t["width"] and not container_id and widest > t["width"] + 1:
            errors.append(f"text '{t['id']}' clipped by its own width ({widest:.0f} > {t['width']:.0f})")

    # arrows
    for arrow in arrows:
        points = [(arrow["x"] + p[0], arrow["y"] + p[1]) for p in arrow["points"]]

        if scene.require_anchors and arrow.get("endArrowhead") == "arrow":
            # Panels count as anchoring targets: an arrow may terminate on a
            # panel edge when it points into a detail box.
            if not any(on_edge(points[-1], bbox(s)) for s in shapes):
                errors.append(f"{arrow['id']}: end {points[-1]} is not anchored on a box edge")

        for p, q in zip(points, points[1:]):
            for shape in solid_shapes:
                box = bbox(shape)
                if on_edge(p, box) or on_edge(q, box):
                    continue
                if segment_hits_rect(p, q, box):
                    errors.append(f"{arrow['id']}: crosses '{shape['id']}'")

    # canvas bounds
    for el in elements:
        x1, y1, x2, y2 = bbox(el)
        if x1 < 0 or y1 < 0 or x2 > scene.width or y2 > scene.height:
            errors.append(f"'{el['id']}' outside canvas: ({x1:.0f},{y1:.0f},{x2:.0f},{y2:.0f})")

    # empty text
    for t in texts:
        if not t["text"].strip():
            errors.append(f"text '{t['id']}' is empty")

    return errors


# -------------------------------------------------------------------- render

SCALE = 2


def render(scene: Scene, path: Path):
    img = Image.new("RGB", (scene.width * SCALE, scene.height * SCALE), "white")
    draw = ImageDraw.Draw(img)

    def sx(v):
        return v * SCALE

    for el in scene.elements:
        if el["type"] == "rectangle":
            x1, y1, x2, y2 = bbox(el)
            dash = el["strokeStyle"] == "dashed"
            draw.rounded_rectangle(
                [sx(x1), sx(y1), sx(x2), sx(y2)],
                radius=8 * SCALE,
                fill=None if el["backgroundColor"] == "transparent" else el["backgroundColor"],
                outline=el["strokeColor"],
                width=2 * SCALE,
            )
            if dash:
                # Redraw with a dashed outline approximation on top of white fill.
                draw.rounded_rectangle(
                    [sx(x1), sx(y1), sx(x2), sx(y2)],
                    radius=8 * SCALE,
                    fill="white",
                    outline="white",
                    width=2 * SCALE,
                )
                draw.rounded_rectangle(
                    [sx(x1), sx(y1), sx(x2), sx(y2)],
                    radius=8 * SCALE,
                    outline=el["strokeColor"],
                    width=2 * SCALE,
                )
        elif el["type"] == "diamond":
            x1, y1, x2, y2 = bbox(el)
            cx, cy = (x1 + x2) / 2, (y1 + y2) / 2
            draw.polygon(
                [(sx(cx), sx(y1)), (sx(x2), sx(cy)), (sx(cx), sx(y2)), (sx(x1), sx(cy))],
                fill=el["backgroundColor"] if el["backgroundColor"] != "transparent" else "white",
                outline=el["strokeColor"],
            )

    for el in scene.elements:
        if el["type"] != "arrow":
            continue
        points = [(el["x"] + p[0], el["y"] + p[1]) for p in el["points"]]
        color = el["strokeColor"]

        for start, end in zip(points, points[1:]):
            if el["strokeStyle"] == "dashed":
                dx, dy = end[0] - start[0], end[1] - start[1]
                length = math.hypot(dx, dy) or 1
                steps = max(int(length // 8), 1)
                for i in range(0, steps, 2):
                    a = (start[0] + dx * i / steps, start[1] + dy * i / steps)
                    b = (start[0] + dx * (i + 1) / steps, start[1] + dy * (i + 1) / steps)
                    draw.line([(sx(a[0]), sx(a[1])), (sx(b[0]), sx(b[1]))], fill=color, width=2 * SCALE)
            else:
                draw.line([(sx(start[0]), sx(start[1])), (sx(end[0]), sx(end[1]))],
                          fill=color, width=2 * SCALE)

        if el.get("endArrowhead") == "arrow":
            start, end = points[-2], points[-1]
            ang = math.atan2(end[1] - start[1], end[0] - start[0])
            size = 10
            draw.polygon(
                [
                    (sx(end[0]), sx(end[1])),
                    (sx(end[0] - size * math.cos(ang - 0.38)), sx(end[1] - size * math.sin(ang - 0.38))),
                    (sx(end[0] - size * math.cos(ang + 0.38)), sx(end[1] - size * math.sin(ang + 0.38))),
                ],
                fill=color,
            )

    for el in scene.elements:
        if el["type"] != "text":
            continue
        f = font(el["fontSize"] * SCALE, bold=el["fontSize"] >= 24)
        lines = el["text"].split("\n")
        lh = el["fontSize"] * LINE_HEIGHT * SCALE
        x1, y1, x2, y2 = bbox(el)
        total_h = len(lines) * lh
        container = el.get("containerId")

        if container:
            cy = (sx(y1) + sx(y2)) / 2 - total_h / 2
            align = "center"
        else:
            cy = sx(y1)
            align = el["textAlign"]

        for i, line in enumerate(lines):
            if align == "center":
                w = draw.textlength(line, font=f)
                draw.text((sx((x1 + x2) / 2) - w / 2, cy + i * lh), line, font=f, fill=el["strokeColor"])
            else:
                draw.text((sx(x1), cy + i * lh), line, font=f, fill=el["strokeColor"])

    img.save(path)


# ---------------------------------------------------------------------- main


def main() -> int:
    out_dir = Path(__file__).resolve().parent
    failures = 0

    for name, build in DIAGRAMS.items():
        scene = build()
        errors = validate(scene)

        excal = out_dir / f"{name}.excalidraw"
        png = out_dir / f"{name}.png"

        excal.write_text(json.dumps(scene.to_json(), indent=2) + "\n")
        render(scene, png)

        if errors:
            failures += 1
            print(f"FAIL {name} ({len(errors)} error(s))")
            for error in errors:
                print(f"   {error}")
        else:
            print(f"ok   {name}  ({len(scene.elements)} elements → {png.name})")

    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())

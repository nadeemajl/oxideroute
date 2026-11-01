#!/usr/bin/env python3
"""Render demo/transcript.json into demo/oxide-demo.mp4.

The transcript holds real command output captured by `demo/record.py`; this
script decides how long each line stays on screen, draws the frames, and hands
them to ffmpeg. Pacing is the only thing invented here: a burst of SSE frames
that took 40 ms to arrive is spread over a few seconds so it can be read.

    python3 demo/render.py                  # -> demo/oxide-demo.mp4
    python3 demo/render.py --keep-frames    # leave PNGs for inspection

Requires Pillow and ffmpeg (libx264).
"""

from __future__ import annotations

import argparse
import json
import re
import shutil
import subprocess
import sys
import tempfile
from dataclasses import dataclass, field
from pathlib import Path

from PIL import Image, ImageDraw, ImageFont

ROOT = Path(__file__).resolve().parent.parent
DEMO = ROOT / "demo"

# ------------------------------------------------------------------ appearance

W, H = 1920, 1080
PANEL = (72, 54, 1848, 926)
CAPTION_BOX = (72, 940, 1848, 1042)
BODY_X = 116
BODY_Y = 136
BODY_W = 1696
LINE_H = 28
FONT_SIZE = 17
MAX_LINES = (900 - BODY_Y) // LINE_H

BG_TOP = (13, 17, 23)
BG_BOTTOM = (8, 10, 14)
SURFACE = (13, 17, 23)
BORDER = (31, 41, 55)
FG = (216, 222, 233)
DIM = (139, 148, 158)
FAINT = (104, 114, 126)
ACCENT = (88, 166, 255)
GREEN = (63, 185, 80)
AMBER = (210, 153, 34)
RED = (248, 81, 73)
PURPLE = (188, 140, 255)
CYAN = (86, 182, 194)
YELLOW = (227, 179, 65)

MONO = "/usr/share/fonts/truetype/dejavu/DejaVuSansMono.ttf"
MONO_BOLD = "/usr/share/fonts/truetype/dejavu/DejaVuSansMono-Bold.ttf"
UI = "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf"
UI_BOLD = "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf"

TYPE_CPS = 48.0
MIN_GAP = {"out": 0.085, "kv": 0.10, "json": 0.075, "header": 0.18,
           "sse": 0.34, "log": 0.50, "metric": 0.30}
MAX_GAP = 1.25

_fonts: dict[tuple[str, int], ImageFont.FreeTypeFont] = {}


def font(path: str, size: int) -> ImageFont.FreeTypeFont:
    key = (path, size)
    if key not in _fonts:
        _fonts[key] = ImageFont.truetype(path, size)
    return _fonts[key]


MONO_FONT = font(MONO, FONT_SIZE)
CHAR_W = MONO_FONT.getlength("0")
MAX_CHARS = int(BODY_W / CHAR_W)

Span = tuple[str, tuple[int, int, int], bool]


# ------------------------------------------------------------------- colouring

FIELD_RE = re.compile(r"(\S+=\S+)")
LOG_RE = re.compile(r"^(\S+Z)\s+(INFO|WARN|ERROR|DEBUG|TRACE)\s+([\w:]+):\s?(.*)$")
HEADER_RE = re.compile(r"^([\w-]+):\s?(.*)$")
METRIC_RE = re.compile(r"^([a-zA-Z_:][\w:]*)(\{[^}]*\})?\s+(\S+)$")
STATUS_TAIL_RE = re.compile(r"(?:→ |status=)([1-5]\d\d)\s*$")
JSON_RE = re.compile(
    r'("(?:\\.|[^"\\])*")(\s*:)?'      # strings, optionally a key
    r"|(-?\d+(?:\.\d+)?(?:[eE][+-]?\d+)?)"  # numbers
    r"|(true|false|null)"              # literals
    r"|([{}\[\],:])"                   # structure
)


def status_colour(code: str) -> tuple[int, int, int]:
    if code.startswith("2"):
        return GREEN
    if code.startswith("4"):
        return RED
    if code.startswith("5"):
        return RED
    return FG


def json_spans(text: str, base=DIM) -> list[Span]:
    spans: list[Span] = []
    cursor = 0
    for match in JSON_RE.finditer(text):
        if match.start() > cursor:
            spans.append((text[cursor:match.start()], base, False))

        string, colon, number, literal, structure = match.groups()
        if string is not None:
            if colon:
                spans.append((string, ACCENT, False))
                spans.append((colon, DIM, False))
            else:
                spans.append((string, GREEN, False))
        elif number is not None:
            spans.append((number, AMBER, False))
        elif literal is not None:
            spans.append((literal, PURPLE, False))
        else:
            spans.append((structure, DIM, False))
        cursor = match.end()

    if cursor < len(text):
        spans.append((text[cursor:], base, False))
    return spans


def spans_for(text: str, kind: str) -> list[Span]:
    if kind == "log":
        match = LOG_RE.match(text)
        if match:
            timestamp, level, target, message = match.groups()
            level_colour = {"INFO": CYAN, "WARN": AMBER, "DEBUG": DIM,
                            "TRACE": DIM}.get(level, RED)
            spans: list[Span] = [
                (timestamp, FAINT, False),
                ("  ", FAINT, False),
                (level, level_colour, True),
                (" ", FAINT, False),
                (target + ":", FAINT, False),
                (" ", FAINT, False),
            ]
            for piece in FIELD_RE.split(message):
                if not piece:
                    continue
                field_match = re.fullmatch(r"(\S+?)=(.*)", piece)
                if field_match:
                    key, value = field_match.groups()
                    spans.append((key, DIM, False))
                    spans.append(("=", FAINT, False))
                    spans.append((value, CYAN, False))
                else:
                    spans.append((piece, FG, False))
            return spans
        return [(text, DIM, False)]

    if kind == "sse":
        prefix = "data: "
        rest = text[len(prefix):] if text.startswith(prefix) else text
        spans = [(prefix, FAINT, False)]
        if rest.strip() == "[DONE]":
            spans.append((rest, GREEN, True))
        else:
            spans.extend(json_spans(rest))
        return spans

    if kind == "json":
        stripped = text.lstrip()
        # A JSON block can also contain a status line from `curl -w`; only lines
        # that start like JSON get the tokenizer.
        if stripped[:1] in ("{", "[", "]", "}", '"') or re.match(r"^-?\d", stripped):
            return json_spans(text)
        kind = "out"

    if kind == "header":
        if text.startswith("HTTP/"):
            parts = text.split(" ", 2)
            spans = [(parts[0], DIM, False), (" ", DIM, False)]
            if len(parts) > 1:
                spans.append((parts[1], status_colour(parts[1]), True))
            if len(parts) > 2:
                spans.append((" " + parts[2], DIM, False))
            return spans
        match = HEADER_RE.match(text)
        if match:
            name, value = match.groups()
            colour = CYAN if name.lower().startswith("x-oxide") else FG
            return [(name + ":", DIM, False), (" " + value, colour, False)]
        return [(text, FG, False)]

    if kind == "metric":
        match = METRIC_RE.match(text)
        if match:
            name, labels, value = match.groups()
            return [
                (name, ACCENT, False),
                (labels or "", DIM, False),
                (" " + value, AMBER, True),
            ]

    if kind == "kv":
        if ":" in text:
            label, value = text.split(":", 1)
            return [(label + ":", DIM, False), (value, FG, False)]

    # Colour a status only where it is unambiguous: `→ 429` or `status=200`.
    match = STATUS_TAIL_RE.search(text)
    if match:
        start = match.start(1)
        return [
            (text[:start], FG, False),
            (match.group(1), status_colour(match.group(1)), True),
            (text[match.end(1):], FG, False),
        ]
    return [(text, FG, False)]


# --------------------------------------------------------------------- wrapping


def wrap(spans: list[Span], max_chars: int = MAX_CHARS) -> list[list[Span]]:
    """Splits one logical line into visual lines, preserving span styles."""
    lines: list[list[Span]] = []
    current: list[Span] = []
    column = 0

    for text, colour, bold in spans:
        # Split the span at the wrap column instead of per character.
        remaining = text
        while remaining:
            room = max_chars - column
            if room <= 0:
                lines.append(current)
                current = [("  ", FAINT, False)]
                column = 2
                room = max_chars - column
            take = remaining[:room]
            current.append((take, colour, bold))
            column += len(take)
            remaining = remaining[room:]
            if remaining:
                lines.append(current)
                current = [("  ", FAINT, False)]
                column = 2

    lines.append(current)
    return lines


# ---------------------------------------------------------------- frame content


@dataclass
class Terminal:
    lines: list[list[Span]] = field(default_factory=list)
    typed: str | None = None
    scene: int = 0
    caption: str = ""
    title: str = ""
    index: int = 0
    total_scenes: int = 1


def command_spans(text: str) -> list[list[Span]]:
    parts = text.split("\n")
    logical = [[("$ ", GREEN, True), (parts[0], FG, True)]]
    for part in parts[1:]:
        logical.append([("  ", FAINT, False), (part, FG, True)])
    return logical


def gradient_background() -> Image.Image:
    image = Image.new("RGB", (W, H), BG_BOTTOM)
    draw = ImageDraw.Draw(image)
    for y in range(H):
        ratio = y / H
        colour = tuple(
            round(top + (bottom - top) * ratio)
            for top, bottom in zip(BG_TOP, BG_BOTTOM)
        )
        draw.line([(0, y), (W, y)], fill=colour)
    return image


BACKGROUND = gradient_background()

_line_cache: dict[tuple, Image.Image] = {}


def line_image(spans: list[Span]) -> Image.Image:
    key = tuple(spans)
    cached = _line_cache.get(key)
    if cached is not None:
        return cached

    image = Image.new("RGBA", (BODY_W, LINE_H), (0, 0, 0, 0))
    draw = ImageDraw.Draw(image)
    x = 0.0
    for text, colour, bold in spans:
        draw.text((x, 0), text, font=font(MONO_BOLD if bold else MONO, FONT_SIZE),
                  fill=colour)
        x += len(text) * CHAR_W
    _line_cache[key] = image
    return image


def frame_image(state: Terminal, progress: float) -> Image.Image:
    image = BACKGROUND.copy()
    draw = ImageDraw.Draw(image)

    # Window chrome -------------------------------------------------------
    draw.rounded_rectangle(PANEL, radius=16, fill=SURFACE, outline=BORDER, width=2)
    for index, colour in enumerate([(255, 95, 86), (255, 189, 46), (39, 201, 63)]):
        cx = PANEL[0] + 26 + index * 21
        draw.ellipse([cx - 7, PANEL[1] + 21, cx + 7, PANEL[1] + 35], fill=colour)

    draw.text((PANEL[0] + 104, PANEL[1] + 18), "oxide — live demo",
              font=font(UI, 15), fill=DIM)

    # Scene dots, right-aligned in the title bar.
    dot_x = PANEL[2] - 26
    for index in range(state.total_scenes - 1, -1, -1):
        colour = ACCENT if index == state.scene else (48, 54, 61)
        radius = 4 if index == state.scene else 3
        draw.ellipse([dot_x - radius, PANEL[1] + 26 - radius,
                      dot_x + radius, PANEL[1] + 26 + radius], fill=colour)
        dot_x -= 18

    draw.line([(PANEL[0], PANEL[1] + 55), (PANEL[2], PANEL[1] + 55)],
              fill=BORDER, width=1)

    # Body ---------------------------------------------------------------
    visual: list[list[Span]] = []
    for logical in state.lines:
        visual.extend(wrap(logical))
    if state.typed is not None:
        for logical in command_spans(state.typed):
            visual.extend(wrap(logical))

    visible = visual[-MAX_LINES:]
    y = BODY_Y
    for spans in visible:
        image.paste(line_image(spans), (BODY_X, y), line_image(spans))
        y += LINE_H

    # Cursor after the last typed character.
    if state.typed is not None and visual:
        last = visual[-1]
        columns = sum(len(text) for text, _, _ in last)
        if columns >= MAX_CHARS:
            cursor_x = BODY_X
            cursor_y = BODY_Y + (len(visible)) * LINE_H
        else:
            cursor_x = BODY_X + columns * CHAR_W
            cursor_y = BODY_Y + (len(visible) - 1) * LINE_H
        draw.rectangle([cursor_x + 1, cursor_y + 3, cursor_x + CHAR_W - 1,
                        cursor_y + LINE_H - 6], fill=ACCENT)

    # Caption bar --------------------------------------------------------
    draw.rectangle(CAPTION_BOX, fill=(11, 14, 19))
    draw.line([(CAPTION_BOX[0], CAPTION_BOX[1]), (CAPTION_BOX[2], CAPTION_BOX[1])],
              fill=BORDER, width=1)

    number = f"{state.index + 1:02d}"
    draw.text((CAPTION_BOX[0] + 28, CAPTION_BOX[1] + 18), number,
              font=font(MONO_BOLD, 15), fill=ACCENT)
    draw.text((CAPTION_BOX[0] + 62, CAPTION_BOX[1] + 12), state.title,
              font=font(UI_BOLD, 22), fill=FG)

    caption_font = font(UI, 15)
    words = state.caption.split()
    lines: list[str] = []
    current = ""
    for word in words:
        candidate = f"{current} {word}".strip()
        if draw.textlength(candidate, font=caption_font) > CAPTION_BOX[2] - CAPTION_BOX[0] - 220:
            lines.append(current)
            current = word
        else:
            current = candidate
    lines.append(current)

    caption_y = CAPTION_BOX[1] + 44
    for line in lines[:2]:
        draw.text((CAPTION_BOX[0] + 62, caption_y), line, font=caption_font,
                  fill=DIM)
        caption_y += 20

    draw.text((CAPTION_BOX[2] - 28, CAPTION_BOX[1] + 20),
              "github.com/nadeemajl/oxideroute", font=font(MONO, 12),
              fill=FAINT, anchor="ra")

    # Progress -----------------------------------------------------------
    draw.rounded_rectangle([PANEL[0], 1062, PANEL[2], 1066], radius=2,
                           fill=(30, 36, 44))
    filled = PANEL[0] + (PANEL[2] - PANEL[0]) * max(0.0, min(1.0, progress))
    if filled > PANEL[0] + 4:
        draw.rounded_rectangle([PANEL[0], 1062, filled, 1066], radius=2, fill=ACCENT)

    return image


def title_image(commit: str) -> Image.Image:
    image = BACKGROUND.copy()
    draw = ImageDraw.Draw(image)

    draw.text((150, 286), "OxideRoute", font=font(UI_BOLD, 78), fill=FG)
    draw.rectangle([154, 392, 274, 397], fill=ACCENT)

    draw.text((154, 434), "One OpenAI-compatible API in front of every model vendor.",
              font=font(UI, 27), fill=(201, 209, 217))
    draw.text((154, 480), "A short tour, recorded against a live process.",
              font=font(UI, 18), fill=DIM)

    chips = ["streaming", "failover", "rate limits", "hot reload", "metrics"]
    x = 154
    for chip in chips:
        width = draw.textlength(chip, font=font(UI, 16)) + 34
        draw.rounded_rectangle([x, 552, x + width, 592], radius=20,
                               outline=BORDER, fill=(15, 20, 27))
        draw.text((x + 17, 563), chip, font=font(UI, 16), fill=(170, 180, 192))
        x += width + 12

    draw.text((154, 936), f"build {commit} · rust · single binary",
              font=font(MONO, 14), fill=FAINT)
    draw.text((W - 154, 936), "github.com/nadeemajl/oxideroute",
              font=font(MONO, 14), fill=FAINT, anchor="ra")

    return image


def outro_image() -> Image.Image:
    image = BACKGROUND.copy()
    draw = ImageDraw.Draw(image)

    draw.text((150, 150), "Where to read more", font=font(UI_BOLD, 40), fill=FG)
    draw.rectangle([154, 214, 234, 218], fill=ACCENT)

    entries = [
        ("docs/overview.html", "one page: what it is and what it is not"),
        ("docs/GUIDE.md", "feature-by-feature walkthrough, with diagrams"),
        ("docs/ARCHITECTURE.md", "crate layout and concurrency invariants"),
        ("docs/STRESS_TEST_PLAN.md", "the capacity campaign still to run"),
    ]
    y = 280
    for name, description in entries:
        draw.text((154, y), name, font=font(MONO_BOLD, 20), fill=ACCENT)
        draw.text((560, y + 2), description, font=font(UI, 19), fill=DIM)
        y += 58

    draw.line([(154, 560), (W - 154, 560)], fill=BORDER, width=1)
    draw.text((154, 600), "Run it yourself:", font=font(UI_BOLD, 20), fill=FG)
    draw.text((154, 638), "cargo build --release && ./target/release/oxide init --output oxide.toml",
              font=font(MONO, 17), fill=(201, 209, 217))

    draw.text((154, 720),
              "The demo config and mock upstream are in demo/ — every request in this video hit that process.",
              font=font(UI, 17), fill=FAINT)

    return image


# ------------------------------------------------------------------------ build


def build_steps(scenes: list[dict]) -> list[dict]:
    steps: list[dict] = []
    clock = 0.0

    for index, scene in enumerate(scenes):
        if index:
            clock += 1.35
        steps.append({"t": clock, "op": "caption", "scene": index})

        for block in scene["blocks"]:
            clock += 0.55
            text = block["display"]
            steps.append({"t": clock, "op": "begin", "text": text})

            for chars in range(4, len(text), 4):
                steps.append({"t": clock + chars / TYPE_CPS, "op": "typed",
                              "text": text[:chars]})
            clock += len(text) / TYPE_CPS
            steps.append({"t": clock, "op": "typed", "text": text})
            clock += 0.42

            for line in block["output"]:
                gap = min(max(line["gap"], MIN_GAP.get(line["kind"], 0.1)), MAX_GAP)
                clock += gap
                steps.append({"t": clock, "op": "line", "line": line})
            clock += 0.30

    return steps


def apply_step(state: Terminal, step: dict) -> None:
    op = step["op"]
    if op == "caption":
        state.scene = step["scene"]
    elif op == "begin":
        if state.typed is not None:
            state.lines.extend(command_spans(state.typed))
        if state.lines:
            state.lines.append([("", FG, False)])
        state.typed = ""
    elif op == "typed":
        state.typed = step["text"]
    elif op == "line":
        if state.typed is not None:
            state.lines.extend(command_spans(state.typed))
            state.typed = None
        state.lines.append(spans_for(step["line"]["text"], step["line"]["kind"]))


def render(scenes: list[dict], mp4: Path, fps: int, keep_frames: bool,
           frame_dir: Path) -> tuple[float, dict, int]:
    steps = build_steps(scenes)
    if not steps:
        raise SystemExit("transcript has no steps")

    # Hold the final terminal state long enough to read the shutdown lines.
    total = steps[-1]["t"] + 3.2
    commit = subprocess.run(["git", "-C", str(ROOT), "rev-parse", "--short", "HEAD"],
                            capture_output=True, text=True, check=True).stdout.strip()

    encoder = subprocess.Popen(
        [
            "ffmpeg", "-hide_banner", "-loglevel", "error", "-y",
            "-f", "rawvideo", "-pix_fmt", "rgb24", "-s", f"{W}x{H}",
            "-r", str(fps), "-i", "-",
            "-vf", "format=yuv420p",
            "-c:v", "libx264", "-preset", "veryfast", "-crf", "21",
            "-movflags", "+faststart", str(mp4),
        ],
        stdin=subprocess.PIPE,
    )
    assert encoder.stdin is not None

    frames_written = 0
    carry = 0.0

    def emit(image: Image.Image, duration: float, name: str | None = None) -> None:
        nonlocal frames_written, carry
        # Round each state to a whole number of frames, carrying the residual so
        # the total duration stays faithful over hundreds of short typing steps.
        exact = duration * fps + carry
        count = max(1, round(exact))
        carry = exact - count

        raw = image.tobytes()
        for _ in range(count):
            encoder.stdin.write(raw)
            frames_written += 1
        if name and keep_frames:
            image.save(frame_dir / f"{name}.png", compress_level=1)

    emit(title_image(commit), 4.2, "intro")

    state = Terminal(total_scenes=len(scenes))
    state.index = 0
    state.title = scenes[0]["title"]
    state.caption = scenes[0]["caption"]

    poster: Image.Image | None = None
    poster_armed: int | None = None

    for index, step in enumerate(steps):
        apply_step(state, step)

        if step["op"] == "caption":
            state.index = step["scene"]
            state.title = scenes[step["scene"]]["title"]
            state.caption = scenes[step["scene"]]["caption"]
            if scenes[step["scene"]]["title"].startswith("Fail over"):
                poster_armed = 0
        if step["op"] == "line" and poster_armed is not None:
            poster_armed += 1

        end = steps[index + 1]["t"] if index + 1 < len(steps) else total
        duration = max(end - step["t"], 1.0 / fps)

        image = frame_image(state, step["t"] / total)
        emit(image, duration, f"state_{index:05d}")

        if poster_armed is not None and poster_armed >= 5 and poster is None:
            poster = image

    emit(outro_image(), 6.0, "outro")

    encoder.stdin.close()
    if encoder.wait(timeout=120) != 0:
        raise SystemExit("ffmpeg failed")

    if poster is not None:
        poster.save(DEMO / "poster.png", compress_level=1)

    return frames_written / fps, {"poster": poster is not None, "commit": commit}, len(steps)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--transcript", default=str(DEMO / "transcript.json"))
    parser.add_argument("--out", default=str(DEMO / "oxide-demo.mp4"))
    parser.add_argument("--fps", type=int, default=30)
    parser.add_argument("--keep-frames", action="store_true")
    args = parser.parse_args()

    if shutil.which("ffmpeg") is None:
        print("ffmpeg is required", file=sys.stderr)
        return 1

    data = json.loads(Path(args.transcript).read_text())
    frame_dir = Path(tempfile.mkdtemp(prefix="oxide-frames-", dir="/tmp/opencode"))

    try:
        duration, meta, states = render(data["scenes"], Path(args.out), args.fps,
                                        args.keep_frames, frame_dir)
        size_mb = Path(args.out).stat().st_size / (1024 * 1024)
        print(f"{args.out}: {duration:.1f}s, {size_mb:.1f} MB, "
              f"{states} states, build {meta['commit']}")
    except BrokenPipeError:
        print("ffmpeg stopped reading frames", file=sys.stderr)
        return 1
    finally:
        if args.keep_frames:
            print(f"frames kept in {frame_dir}")
        else:
            shutil.rmtree(frame_dir, ignore_errors=True)

    return 0


if __name__ == "__main__":
    sys.exit(main())

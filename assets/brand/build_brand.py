"""Build the LongHorizonOS brand assets.

Generates every logo lockup as SVG (with the wordmark converted to outlines, so
the files render identically without any font installed) plus PNG exports.

Usage:
    python assets/brand/build_brand.py

The mark is a loop that closes into a checkmark:

    an open cycle (work can come around again) + a check (it came back proven)

The loop is deliberately left open and the check exits through the gap: a Goal
reopens when the world changes, and only current, applicable Evidence closes it.
"""

from __future__ import annotations

import pathlib

from fontTools.pens.svgPathPen import SVGPathPen
from fontTools.ttLib import TTFont

ROOT = pathlib.Path(__file__).resolve().parents[2]
BRAND = ROOT / "assets" / "brand"

_FONT_DIRS = [
    pathlib.Path(
        r"C:\Users\yangjiashu\.cache\codex-runtimes\codex-primary-runtime"
        r"\dependencies\native\poppler\Library\share\fonts"
    ),
    BRAND / "fonts",
]


def _find_font(filename: str) -> pathlib.Path:
    for directory in _FONT_DIRS:
        candidate = directory / filename
        if candidate.is_file():
            return candidate
    raise FileNotFoundError(
        f"{filename} not found. Searched: "
        + ", ".join(str(d) for d in _FONT_DIRS)
    )


# ----------------------------------------------------------------- palette
INK = "#141414"           # near-black: the loop, and the wordmark on light
PAPER = "#F7F5F0"         # warm off-white: light background / icon tile
ACCENT = "#FF5A1F"        # signal orange: the proof stroke
SURFACE_DARK = "#0F0F12"  # dark-surface background
LOOP_ON_DARK = "#FFFFFF"  # the loop, on dark surfaces

# ------------------------------------------------------------ mark geometry
# Drawn on a 256x256 grid centred at (128, 128), then placed by `mark_group`.
RING_R = 76.0
RING_STROKE = 21.0
CHECK_STROKE = 25.0
GAP_DEG = 96.0  # the opening in the loop that the check exits through
_CHECK = ((96.0, 130.0), (124.0, 158.0), (192.0, 82.0))

MARK_X0 = 128.0 - RING_R - RING_STROKE / 2
MARK_X1 = max(128.0 + RING_R + RING_STROKE / 2, _CHECK[2][0] + CHECK_STROKE / 2)
MARK_Y0 = min(128.0 - RING_R - RING_STROKE / 2, _CHECK[2][1] - CHECK_STROKE / 2)
MARK_Y1 = 128.0 + RING_R + RING_STROKE / 2
MARK_W = MARK_X1 - MARK_X0
MARK_H = MARK_Y1 - MARK_Y0


def _ring(stroke: str, opacity: str = "") -> str:
    """The open loop, as a dashed circle so the gap is exact."""
    import math

    circ = 2 * math.pi * RING_R
    on = circ * (1 - GAP_DEG / 360.0)
    rot = -90.0 + GAP_DEG / 2.0
    op = f' stroke-opacity="{opacity}"' if opacity else ""
    return (
        f'    <circle cx="128" cy="128" r="{RING_R:g}" fill="none" '
        f'stroke="{stroke}"{op}\n'
        f'            stroke-width="{RING_STROKE:g}" stroke-linecap="round"\n'
        f'            stroke-dasharray="{on:.2f} {circ - on:.2f}"\n'
        f'            transform="rotate({rot:.2f} 128 128)"/>'
    )


def _check(stroke: str) -> str:
    (x1, y1), (x2, y2), (x3, y3) = _CHECK
    return (
        f'    <path d="M{x1:g} {y1:g} L{x2:g} {y2:g} L{x3:g} {y3:g}" '
        f'fill="none" stroke="{stroke}"\n'
        f'          stroke-width="{CHECK_STROKE:g}" stroke-linecap="round" '
        'stroke-linejoin="round"/>'
    )


def mark_group(
    target_w: float,
    cx: float,
    cy: float,
    loop: str = INK,
    accent: str = ACCENT,
) -> str:
    """Place the mark scaled to `target_w`, centred on (cx, cy)."""
    scale = target_w / MARK_W
    tx = cx - scale * (MARK_X0 + MARK_W / 2)
    ty = cy - scale * (MARK_Y0 + MARK_H / 2)
    return (
        f'  <g transform="translate({tx:.3f} {ty:.3f}) scale({scale:.5f})">\n'
        f"{_ring(loop)}\n{_check(accent)}\n  </g>"
    )


# --------------------------------------------------------------- wordmark
UPEM = 1000.0
CAP = 693.0  # Ubuntu cap height, in font units


class Wordmark:
    """Converts wordmark text into SVG outline paths (no font dependency)."""

    def __init__(self) -> None:
        self._fonts: dict[pathlib.Path, TTFont] = {}

    def _font(self, path: pathlib.Path) -> TTFont:
        if path not in self._fonts:
            self._fonts[path] = TTFont(str(path))
        return self._fonts[path]

    def run(
        self,
        text: str,
        font_path: pathlib.Path,
        tracking: float = 0.0,
        x: float = 0.0,
    ) -> tuple[str, float]:
        """Return (svg path data, advance) in font units."""
        font = self._font(font_path)
        glyphs = font.getGlyphSet()
        cmap = font.getBestCmap()
        out: list[str] = []
        cursor = x
        for ch in text:
            glyph = glyphs[cmap[ord(ch)]]
            pen = SVGPathPen(glyphs)
            glyph.draw(pen)
            commands = pen.getCommands()
            if commands:
                out.append(
                    f'<path transform="translate({cursor:.2f} 0)" '
                    f'd="{commands}"/>'
                )
            cursor += glyph.width + tracking
        return "".join(out), cursor - x - tracking


WM = Wordmark()
TRACKING = -8.0
WORD_GAP = 24.0


def wordmark_metrics(cap_px: float) -> float:
    """Width in px of the full wordmark at the given cap height."""
    _, adv1 = WM.run("LongHorizon", _find_font("Ubuntu-M.ttf"), TRACKING)
    _, adv2 = WM.run("OS", _find_font("Ubuntu-B.ttf"), TRACKING)
    return (adv1 + WORD_GAP + adv2) * (cap_px / CAP)


def wordmark_group(
    cap_px: float,
    x: float,
    cap_top_y: float,
    light_bg: bool,
) -> str:
    """"LongHorizon" in medium + "OS" in bold accent, as outlines."""
    medium = _find_font("Ubuntu-M.ttf")
    bold = _find_font("Ubuntu-B.ttf")
    d1, adv1 = WM.run("LongHorizon", medium, TRACKING)
    d2, _ = WM.run("OS", bold, TRACKING, x=adv1 + WORD_GAP)

    scale = cap_px / CAP
    baseline = cap_top_y + cap_px  # font is y-up, SVG is y-down
    primary = INK if light_bg else "#FFFFFF"
    return (
        f'  <g transform="translate({x:.3f} {baseline:.3f}) '
        f'scale({scale:.6f} {-scale:.6f})">\n'
        f'    <g fill="{primary}">{d1}</g>\n'
        f'    <g fill="{ACCENT}">{d2}</g>\n'
        "  </g>"
    )


# ------------------------------------------------------------------ files
def svg_doc(width: float, height: float, body: str, defs: str = "") -> str:
    return (
        '<svg xmlns="http://www.w3.org/2000/svg" '
        f'width="{width:g}" height="{height:g}" '
        f'viewBox="0 0 {width:g} {height:g}" '
        'role="img" aria-label="LongHorizonOS">\n'
        f"{defs}{body}\n</svg>\n"
    )


def build_icon(dark: bool = False) -> str:
    """Rounded-tile app icon."""
    bg = SURFACE_DARK if dark else PAPER
    loop = LOOP_ON_DARK if dark else INK
    body = (
        f'  <rect width="256" height="256" rx="58" fill="{bg}"/>\n'
        + mark_group(target_w=152, cx=128, cy=128, loop=loop)
    )
    return svg_doc(256, 256, body)


def build_mark(light_bg: bool) -> str:
    w, h = 208.0, 196.0
    loop = INK if light_bg else LOOP_ON_DARK
    return svg_doc(w, h, mark_group(172, w / 2, h / 2, loop=loop))


def build_mark_mono(color: str) -> str:
    """Single-colour mark: the loop drops back, the check keeps full weight."""
    w, h = 208.0, 196.0
    scale = 172 / MARK_W
    tx = w / 2 - scale * (MARK_X0 + MARK_W / 2)
    ty = h / 2 - scale * (MARK_Y0 + MARK_H / 2)
    body = (
        f'  <g transform="translate({tx:.3f} {ty:.3f}) scale({scale:.5f})">\n'
        f'{_ring(color, opacity="0.45")}\n{_check(color)}\n'
        "  </g>"
    )
    return svg_doc(w, h, body)


def build_horizontal(light_bg: bool) -> str:
    h = 128.0
    pad = 24.0
    mark_w = 100.0
    cap_px = 56.0
    gap = 26.0
    loop = INK if light_bg else LOOP_ON_DARK
    mark = mark_group(mark_w, pad + mark_w / 2, h / 2, loop=loop)

    text_x = pad + mark_w + gap
    word = wordmark_group(cap_px, text_x, h / 2 - cap_px / 2, light_bg)
    total_w = round(text_x + wordmark_metrics(cap_px) + pad, 1)
    return svg_doc(total_w, h, mark + "\n" + word)


def build_stacked(light_bg: bool) -> str:
    cap_px = 64.0
    mark_w = 148.0
    pad = 40.0
    # Size the artboard from the measured wordmark so nothing ever clips.
    word_w = wordmark_metrics(cap_px)
    w = round(max(word_w, mark_w) + pad * 2, 1)
    mark_cy = 122.0
    cap_top = 226.0
    h = round(cap_top + cap_px + pad, 1)
    loop = INK if light_bg else LOOP_ON_DARK
    mark = mark_group(mark_w, w / 2, mark_cy, loop=loop)
    word = wordmark_group(cap_px, (w - word_w) / 2, cap_top, light_bg)
    return svg_doc(w, h, mark + "\n" + word)


def build_banner() -> str:
    w, h = 1280.0, 400.0
    mark_w = 118.0
    cap_px = 74.0
    gap = 34.0
    center_y = h / 2 - 24.0

    block = mark_w + gap
    total = block + wordmark_metrics(cap_px)
    start = (w - total) / 2

    mark = mark_group(mark_w, start + mark_w / 2, center_y, loop=LOOP_ON_DARK)
    word = wordmark_group(cap_px, start + block, center_y - cap_px / 2, False)

    tag = "An evidence-backed operating runtime for long-horizon agents"
    tag_cap = 24.0
    d, adv = WM.run(tag, _find_font("Ubuntu-M.ttf"), tracking=3.0)
    tscale = tag_cap / CAP
    tagline = (
        f'  <g transform="translate({(w - adv * tscale) / 2:.2f} '
        f'{center_y + 108.0:.2f}) scale({tscale:.6f} {-tscale:.6f})" '
        'fill="#FFFFFF" fill-opacity="0.62">'
        f"{d}</g>"
    )

    body = (
        f'  <rect width="{w:g}" height="{h:g}" fill="{SURFACE_DARK}"/>\n'
        + mark + "\n" + word + "\n" + tagline
    )
    return svg_doc(w, h, body)


FILES: dict[str, object] = {
    "icon.svg": lambda: build_icon(dark=False),
    "icon-dark.svg": lambda: build_icon(dark=True),
    "mark-on-dark.svg": lambda: build_mark(light_bg=False),
    "mark-on-light.svg": lambda: build_mark(light_bg=True),
    "mark-mono-white.svg": lambda: build_mark_mono("#FFFFFF"),
    "mark-mono-black.svg": lambda: build_mark_mono(INK),
    "logo-horizontal-on-light.svg": lambda: build_horizontal(True),
    "logo-horizontal-on-dark.svg": lambda: build_horizontal(False),
    "logo-stacked-on-light.svg": lambda: build_stacked(True),
    "logo-stacked-on-dark.svg": lambda: build_stacked(False),
    "banner.svg": build_banner,
}


def main() -> None:
    BRAND.mkdir(parents=True, exist_ok=True)
    for name, fn in FILES.items():
        (BRAND / name).write_text(fn(), encoding="utf-8")  # type: ignore[operator]
        print("wrote", name)
    print()
    print("SVG written. To refresh the PNG/ICO exports run:")
    print("    node assets/brand/rasterize.js")


if __name__ == "__main__":
    main()

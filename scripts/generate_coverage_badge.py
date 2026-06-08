"""Generate a coverage badge SVG from coverage.py JSON output."""

from __future__ import annotations

import json
import sys
from pathlib import Path
from xml.sax.saxutils import escape


def coverage_color(percent: float) -> str:
    """Return a shields-style color for a coverage percentage."""
    if percent >= 90:
        return "#4c1"
    if percent >= 80:
        return "#97ca00"
    if percent >= 70:
        return "#a4a61d"
    if percent >= 60:
        return "#dfb317"
    if percent >= 50:
        return "#fe7d37"
    return "#e05d44"


def text_width(text: str) -> int:
    """Approximate text width for the badge's Verdana 11px font."""
    return (len(text) * 7) + 10


def build_badge(label: str, message: str, color: str) -> str:
    """Build a compact SVG badge."""
    label_width = text_width(label)
    message_width = text_width(message)
    width = label_width + message_width
    label_center = label_width / 2
    message_center = label_width + (message_width / 2)
    safe_label = escape(label)
    safe_message = escape(message)
    safe_title = escape(f"{label}: {message}")

    lines = [
        '<svg xmlns="http://www.w3.org/2000/svg"',
        f'     width="{width}" height="20" role="img"',
        f'     aria-label="{safe_title}">',
        f"  <title>{safe_title}</title>",
        '  <linearGradient id="s" x2="0" y2="100%">',
        '    <stop offset="0" stop-color="#bbb" stop-opacity=".1"/>',
        '    <stop offset="1" stop-opacity=".1"/>',
        "  </linearGradient>",
        '  <clipPath id="r">',
        f'    <rect width="{width}" height="20" rx="3" fill="#fff"/>',
        "  </clipPath>",
        '  <g clip-path="url(#r)">',
        f'    <rect width="{label_width}" height="20" fill="#555"/>',
        f'    <rect x="{label_width}" width="{message_width}"',
        f'          height="20" fill="{color}"/>',
        f'    <rect width="{width}" height="20" fill="url(#s)"/>',
        "  </g>",
        '  <g fill="#fff" text-anchor="middle"',
        '     font-family="Verdana,Geneva,sans-serif" font-size="11">',
        f'    <text x="{label_center}" y="15" fill="#010101"',
        f'          fill-opacity=".3">{safe_label}</text>',
        f'    <text x="{label_center}" y="14">{safe_label}</text>',
        f'    <text x="{message_center}" y="15" fill="#010101"',
        f'          fill-opacity=".3">{safe_message}</text>',
        f'    <text x="{message_center}" y="14">{safe_message}</text>',
        "  </g>",
        "</svg>",
    ]
    return "\n".join(lines) + "\n"


def read_coverage_percent(input_path: Path) -> float:
    """Read total coverage percentage from coverage.py JSON output."""
    data = json.loads(input_path.read_text(encoding="utf-8"))
    try:
        return float(data["totals"]["percent_covered"])
    except KeyError as exc:
        raise SystemExit(f"Invalid coverage JSON, missing key: {exc}") from exc


def main(argv: list[str]) -> int:
    """CLI entry point."""
    if len(argv) not in {2, 3}:
        print(
            "Usage: generate_coverage_badge.py <coverage.json> [coverage.svg]",
            file=sys.stderr,
        )
        return 2

    input_path = Path(argv[1])
    output_path = Path(argv[2]) if len(argv) == 3 else Path("coverage.svg")
    percent = read_coverage_percent(input_path)
    message = f"{round(percent):.0f}%"
    badge = build_badge("coverage", message, coverage_color(percent))
    output_path.write_text(badge, encoding="utf-8", newline="\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))

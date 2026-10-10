#!/usr/bin/env python3
"""Render the SF10 README chart for the four deletion-vector cases."""

import argparse
import csv
import math
from pathlib import Path
import statistics

from render_selective_s3_chart import README_THEMES


ROOT = Path(__file__).resolve().parents[1]
READERS = ("delta-arrow-reader", "delta-rs", "duckdb", "polars", "spark")
LABELS = ("Delta Arrow Reader", "delta-rs", "DuckDB", "Polars", "Spark (single machine)")
LAYOUTS = ("q2.localized", "q2.scattered", "q4.localized", "q4.scattered")
TABLE_COLUMNS = {"q2": 416, "q4": 90}
TIME_TICKS = {"q2": (0, 20, 40, 60, 80), "q4": (0, 50, 100, 150, 200)}


def load_medians():
    source = ROOT / "docs/public/benchmarks/selective-read-current-timings.csv"
    with source.open(newline="") as stream:
        rows = list(csv.DictReader(stream))
    samples = {}
    for row in rows:
        assert row["status"] == "success" and row["execution_mode"] == "reuse"
        value = int(row["query_0_completion_ns"]) / 1e9
        assert math.isfinite(value) and value > 0
        samples.setdefault((row["case_id"], row["reader_id"]), []).append(
            (int(row["repetition"]), value)
        )
    expected = {
        (f"production.{layout}{dv}", reader)
        for layout in LAYOUTS for dv in ("", ".dv") for reader in READERS
    }
    assert samples.keys() == expected
    assert len(rows) == len({row["run_id"] for row in rows}) == 200
    values = {}
    for key, runs in samples.items():
        assert sorted(repetition for repetition, _ in runs) == list(range(5)), key
        values[key] = statistics.median(value for _, value in runs)
    for case, reader in expected:
        if reader != READERS[0]:
            assert values[case, READERS[0]] < values[case, reader], f"README headline does not match {case}"
    return values


def render(theme_name, values):
    theme = README_THEMES[theme_name]
    plot_left, plot_width = 240, 700
    colors = (theme["engines"]["delta_arrow_reader"],) + {
        "light": ("#e1e5ea", "#b9c0c8", "#96a1af", "#738091"),
        "dark": ("#53606e", "#697786", "#83909e", "#a5afba"),
    }[theme_name]
    parts = [f'''<svg xmlns="http://www.w3.org/2000/svg" width="1200" height="760" viewBox="0 0 1200 760" role="img" aria-labelledby="title description">
<title id="title">Faster Delta Lake reads</title>
<desc id="description">Five readers query 416-column and 90-column Delta tables with about 60 million rows derived from TPC-H lineitem at SF10. All four cases use deletion vectors to mark deleted rows. Matching rows are grouped together or spread out within row groups. Each group follows legend order. Bars show median seconds for the first complete query after table initialization over five independent runs. Linear axes start at zero; shorter bars are faster. The 416-column panels span 0 to 80 seconds, and the 90-column panels span 0 to 200 seconds. Compare bar lengths within the same axis range. Labels give seconds and, for other readers, their time divided by Delta Arrow Reader's time in the same case. Startup and table initialization are excluded. The full report also includes initialization costs and four cases without deletion vectors.</desc>
<style>text{{font-family:Inter,ui-sans-serif,-apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif;font-variant-numeric:tabular-nums}}</style>
<defs><linearGradient id="page" x1="0" y1="0" x2="1" y2="1"><stop offset="0" stop-color="{theme['background']}"/><stop offset="1" stop-color="{theme['background_end']}"/></linearGradient></defs>
<rect width="1200" height="760" fill="url(#page)"/>
<text x="44" y="32" fill="{theme['muted']}" font-size="12" font-weight="700" letter-spacing="2">TPC-H-DERIVED DATA | ABOUT 60 MILLION ROWS PER TABLE</text>
<text x="44" y="70" fill="{theme['text']}" font-size="32" font-weight="700">Faster Delta Lake reads</text>
<text x="44" y="99" fill="{theme['muted']}" font-size="16">Query time in seconds. Shorter bars are faster; axes differ by table width.</text>
<text x="1156" y="99" text-anchor="end" fill="{theme['muted']}" font-size="13">Labels: seconds / time vs. Delta Arrow Reader</text>''']
    for x, label, color in zip((44, 300, 465, 625, 780), LABELS, colors, strict=True):
        parts.append(f'''<rect x="{x}" y="121" width="10" height="10" rx="3" fill="{color}"/>
<text x="{x + 18}" y="131" fill="{theme['text']}" font-size="14">{label}</text>''')

    parts.append(f'<text x="44" y="159" fill="{theme["muted"]}" font-size="13">Table / matching rows</text>')
    for row, layout in enumerate(LAYOUTS):
        case = f"production.{layout}.dv"
        top = 184 + row * 136
        query, arrangement = layout.split(".")
        ticks = TIME_TICKS[query]
        matches = "Matches grouped together" if arrangement == "localized" else "Matches spread out"
        case_label = f"{TABLE_COLUMNS[query]}-column table, {matches.lower()}, with deletion vectors"
        baseline = values[case, READERS[0]]
        if row:
            parts.append(f'<line x1="44" y1="{top - 28}" x2="1156" y2="{top - 28}" stroke="{theme["grid"]}" stroke-dasharray="4 7"/>')
        for tick in ticks:
            x = plot_left + plot_width * tick / ticks[-1]
            parts.append(f'''<line x1="{x}" y1="{top - 2}" x2="{x}" y2="{top + 100}" stroke="{theme['grid']}" stroke-dasharray="3 6"/>
<text x="{x}" y="{top - 12}" text-anchor="middle" fill="{theme['muted']}" font-size="13">{tick} s</text>''')
        parts.append(f'''<text x="44" y="{top + 39}" fill="{theme['text']}" font-size="18" font-weight="500">{TABLE_COLUMNS[query]} columns</text>
<text x="44" y="{top + 61}" fill="{theme['muted']}" font-size="13">{matches}</text>''')
        for index, (reader, label, color) in enumerate(zip(READERS, LABELS, colors, strict=True)):
            value = values[case, reader]
            assert 0 < value <= ticks[-1], (case, reader, value)
            width = plot_width * value / ticks[-1]
            y = top + index * 20 + 7
            label_color = theme["text"] if index == 0 else theme["muted"]
            annotation = f"{value:.2f} s" + (f" / {value / baseline:.2f}x" if index else "")
            parts.append(f'''<rect x="{plot_left}" y="{y - 5}" width="{width:.2f}" height="10" rx="2" fill="{color}"><title>{case_label}: {label}, {value:.3f} s, {value / baseline:.2f} times Delta Arrow Reader's time</title></rect>
<text x="{plot_left + width + 12:.2f}" y="{y + 4}" fill="{label_color}" font-size="13">{annotation}</text>''')
    parts.append(f'''<text x="44" y="740" fill="{theme['muted']}" font-size="13">Four cases with deletion vectors. Median of 5 complete first queries; startup and table initialization excluded.</text>
</svg>''')
    return "\n".join(parts) + "\n"


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--check", action="store_true", help="verify the CSV and generated SVGs")
    args = parser.parse_args()
    values = load_medians()
    for theme in README_THEMES:
        target = ROOT / f"docs/public/assets/selective-read-readme-{theme}.svg"
        rendered = render(theme, values)
        if args.check:
            assert target.read_text() == rendered, target
        else:
            target.write_text(rendered)
        print(target.relative_to(ROOT))


if __name__ == "__main__":
    main()

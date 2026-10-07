#!/usr/bin/env python3
"""Render the SF10 README chart for the four deletion-vector cases."""

import argparse
import csv
import math
from pathlib import Path

from render_selective_s3_chart import README_THEMES, x_position


ROOT = Path(__file__).resolve().parents[1]
READERS = ("delta-arrow-reader", "delta-rs", "duckdb", "polars", "spark")
LABELS = ("Delta Arrow Reader", "delta-rs", "DuckDB", "Polars", "Spark (single machine)")
LAYOUTS = ("q2.localized", "q2.scattered", "q4.localized", "q4.scattered")
TABLE_COLUMNS = {"q2": 416, "q4": 90}
TIME_TICKS = (5, 10, 20, 50, 100, 200)


def load_medians():
    source = ROOT / "docs/public/benchmarks/selective-read-summary.csv"
    with source.open(newline="") as stream:
        rows = [
            row for row in csv.DictReader(stream)
            if row["execution_mode"] == "open" and row["case_id"].endswith(".dv")
        ]
    values = {}
    for row in rows:
        assert (row["status"], row["eligible"], row["samples"]) == ("success", "True", "5")
        value = float(row["open_query_ns_median"]) / 1e9
        assert math.isfinite(value) and value > 0
        values[row["case_id"], row["reader_id"]] = value
    expected = {
        (f"production.{layout}.dv", reader)
        for layout in LAYOUTS for reader in READERS
    }
    assert len(rows) == len(values) == 20 and values.keys() == expected
    return values


def render(theme_name, values):
    theme = README_THEMES[theme_name]
    plot_left, plot_width = 240, 700
    domain = (TIME_TICKS[0], TIME_TICKS[-1])
    colors = (theme["engines"]["delta_arrow_reader"],) + {
        "light": ("#e1e5ea", "#b9c0c8", "#96a1af", "#738091"),
        "dark": ("#53606e", "#697786", "#83909e", "#a5afba"),
    }[theme_name]
    parts = [f'''<svg xmlns="http://www.w3.org/2000/svg" width="1200" height="700" viewBox="0 0 1200 700" role="img" aria-labelledby="title description">
<title id="title">Faster Delta Lake reads</title>
<desc id="description">Five readers query 416-column and 90-column Delta tables with about 60 million rows derived from TPC-H lineitem at SF10. All four cases use deletion vectors to mark deleted rows. Matching rows are grouped together or spread out within row groups. Each group follows legend order. Dots show median seconds to open a table and consume the full result over five runs. Lower is faster. All cases share a logarithmic time axis from 5 to 200 seconds, so equal time ratios span equal distances. Labels give seconds and, for other readers, their time divided by Delta Arrow Reader's time in the same case. Startup is excluded. The full report also includes four cases without deletion vectors.</desc>
<style>text{{font-family:Inter,ui-sans-serif,-apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif;font-variant-numeric:tabular-nums}}</style>
<defs><linearGradient id="page" x1="0" y1="0" x2="1" y2="1"><stop offset="0" stop-color="{theme['background']}"/><stop offset="1" stop-color="{theme['background_end']}"/></linearGradient></defs>
<rect width="1200" height="700" fill="url(#page)"/>
<text x="44" y="32" fill="{theme['muted']}" font-size="12" font-weight="700" letter-spacing="2">TPC-H-DERIVED DATA | ABOUT 60 MILLION ROWS PER TABLE</text>
<text x="44" y="70" fill="{theme['text']}" font-size="32" font-weight="700">Faster Delta Lake reads</text>
<text x="44" y="99" fill="{theme['muted']}" font-size="16">Query time in seconds (median of 5 runs). Log scale; lower is faster.</text>
<text x="1156" y="99" text-anchor="end" fill="{theme['muted']}" font-size="13">Labels: seconds / time vs. Delta Arrow Reader</text>''']
    for x, label, color in zip((44, 300, 465, 625, 780), LABELS, colors, strict=True):
        parts.append(f'''<rect x="{x}" y="121" width="10" height="10" rx="3" fill="{color}"/>
<text x="{x + 18}" y="131" fill="{theme['text']}" font-size="14">{label}</text>''')

    parts.append(f'<text x="44" y="159" fill="{theme["muted"]}" font-size="13">Table / matching rows</text>')
    for tick in TIME_TICKS:
        x = x_position(tick, plot_left, plot_width, domain)
        parts.append(f'''<line x1="{x}" y1="170" x2="{x}" y2="650" stroke="{theme['grid']}" stroke-dasharray="3 6"/>
<text x="{x}" y="159" text-anchor="middle" fill="{theme['muted']}" font-size="13">{tick} s</text>''')
    for row, layout in enumerate(LAYOUTS):
        case = f"production.{layout}.dv"
        top = 184 + row * 120
        query, arrangement = layout.split(".")
        matches = "Matches grouped together" if arrangement == "localized" else "Matches spread out"
        case_label = f"{TABLE_COLUMNS[query]}-column table, {matches.lower()}, with deletion vectors"
        baseline = values[case, READERS[0]]
        assert all(baseline < values[case, reader] for reader in READERS[1:]), f"Headline does not match {case}"
        if row:
            parts.append(f'<line x1="44" y1="{top - 15}" x2="1156" y2="{top - 15}" stroke="{theme["grid"]}" stroke-dasharray="4 7"/>')
        parts.append(f'''<text x="44" y="{top + 39}" fill="{theme['text']}" font-size="18" font-weight="500">{TABLE_COLUMNS[query]} columns</text>
<text x="44" y="{top + 61}" fill="{theme['muted']}" font-size="13">{matches}</text>''')
        for index, (reader, label, color) in enumerate(zip(READERS, LABELS, colors, strict=True)):
            value = values[case, reader]
            assert domain[0] <= value <= domain[1], (case, reader, value)
            x = x_position(value, plot_left, plot_width, domain)
            y = top + index * 20 + 7
            label_color = theme["text"] if index == 0 else theme["muted"]
            annotation = f"{value:.2f} s" + (f" / {value / baseline:.2f}x" if index else "")
            parts.append(f'''<line x1="{plot_left}" y1="{y}" x2="{x:.2f}" y2="{y}" stroke="{color}" stroke-width="2"/>
<circle cx="{x:.2f}" cy="{y}" r="6" fill="{color}"><title>{case_label}: {label}, {value:.3f} s, {value / baseline:.2f} times Delta Arrow Reader's time</title></circle>
<text x="{x + 12:.2f}" y="{y + 4}" fill="{label_color}" font-size="13">{annotation}</text>''')
    parts.append(f'''<text x="44" y="680" fill="{theme['muted']}" font-size="13">Four cases with deleted rows (deletion vectors). Includes opening the table and reading all results; process startup excluded.</text>
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

#!/usr/bin/env python3
"""Render the SF10 README chart for the four deletion-vector cases."""

import argparse
import csv
import math
from pathlib import Path

from render_selective_s3_chart import README_THEMES


ROOT = Path(__file__).resolve().parents[1]
READERS = ("delta-arrow-reader", "delta-rs", "duckdb", "polars", "spark")
LABELS = ("Delta Arrow Reader", "delta-rs", "DuckDB", "Polars", "Spark (single machine)")
LAYOUTS = ("q2.localized", "q2.scattered", "q4.localized", "q4.scattered")
TABLE_COLUMNS = {"q2": 416, "q4": 90}


def load_medians():
    source = ROOT / "docs/content/benchmarks/selective-read-summary.csv"
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
    plot_left, plot_width, maximum = 210, 870, 200
    colors = (theme["engines"]["delta_arrow_reader"],) + {
        "light": ("#e1e5ea", "#b9c0c8", "#96a1af", "#738091"),
        "dark": ("#53606e", "#697786", "#83909e", "#a5afba"),
    }[theme_name]
    parts = [f'''<svg xmlns="http://www.w3.org/2000/svg" width="1200" height="700" viewBox="0 0 1200 700" role="img" aria-labelledby="title description">
<title id="title">Selective Delta reads with deletion vectors</title>
<desc id="description">Four deletion-vector cases compare five readers on 416-column and 90-column Delta tables derived from TPC-H lineitem at SF10. Row labels describe stored table columns and matching-row layout. Each group follows legend order. Bars show median seconds to open a table and consume the full result over five runs. Lower is faster. All cases share a linear axis from zero to 200 seconds. Startup is excluded. The full report also includes four cases without deletion vectors.</desc>
<style>text{{font-family:Inter,ui-sans-serif,-apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif;font-variant-numeric:tabular-nums}}</style>
<defs><linearGradient id="page" x1="0" y1="0" x2="1" y2="1"><stop offset="0" stop-color="{theme['background']}"/><stop offset="1" stop-color="{theme['background_end']}"/></linearGradient></defs>
<rect width="1200" height="700" fill="url(#page)"/>
<text x="44" y="32" fill="{theme['muted']}" font-size="12" font-weight="700" letter-spacing="2">TPC-H-DERIVED DATA (SF10) | SELECTIVE DELTA LAKE READS</text>
<text x="44" y="70" fill="{theme['text']}" font-size="32" font-weight="700">Fastest with deletion vectors</text>
<text x="44" y="99" fill="{theme['muted']}" font-size="16">Median query time over five runs, seconds. Lower is faster.</text>''']
    for x, label, color in zip((44, 300, 465, 625, 780), LABELS, colors, strict=True):
        parts.append(f'''<rect x="{x}" y="121" width="10" height="10" rx="3" fill="{color}"/>
<text x="{x + 18}" y="131" fill="{theme['text']}" font-size="14">{label}</text>''')

    parts.append(f'<text x="44" y="159" fill="{theme["muted"]}" font-size="13">Table / matches</text>')
    for tick in range(5):
        x = plot_left + plot_width * tick / 4
        parts.append(f'''<line x1="{x}" y1="170" x2="{x}" y2="650" stroke="{theme['grid']}" stroke-dasharray="3 6"/>
<text x="{x}" y="159" text-anchor="middle" fill="{theme['muted']}" font-size="13">{maximum * tick // 4} s</text>''')
    for row, layout in enumerate(LAYOUTS):
        case = f"production.{layout}.dv"
        top = 184 + row * 120
        query, arrangement = layout.split(".")
        case_label = f"{TABLE_COLUMNS[query]}-column table, {arrangement}, with deletion vectors"
        assert all(values[case, READERS[0]] < values[case, reader] for reader in READERS[1:]), f"Headline does not match {case}"
        if row:
            parts.append(f'<line x1="44" y1="{top - 15}" x2="1156" y2="{top - 15}" stroke="{theme["grid"]}" stroke-dasharray="4 7"/>')
        parts.append(f'''<text x="44" y="{top + 39}" fill="{theme['text']}" font-size="18" font-weight="500">{TABLE_COLUMNS[query]} columns</text>
<text x="44" y="{top + 61}" fill="{theme['muted']}" font-size="14">{arrangement.capitalize()}</text>''')
        for index, (reader, label, color) in enumerate(zip(READERS, LABELS, colors, strict=True)):
            value = values[case, reader]
            assert value <= maximum, (case, reader, value)
            width = value / maximum * plot_width
            y = top + index * 20
            label_color = theme["text"] if index == 0 else theme["muted"]
            parts.append(f'''<rect x="{plot_left}" y="{y}" width="{width:.2f}" height="14" rx="5" fill="{color}"><title>{case_label}: {label}, {value:.3f} s</title></rect>
<text x="{plot_left + width + 8:.2f}" y="{y + 12}" fill="{label_color}" font-size="13">{value:.3f} s</text>''')
    parts.append(f'''<text x="44" y="680" fill="{theme['muted']}" font-size="13">Four deletion-vector cases shown. Open table + full result; process startup excluded.</text>
</svg>''')
    return "\n".join(parts) + "\n"


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--check", action="store_true", help="verify the CSV and generated SVGs")
    args = parser.parse_args()
    values = load_medians()
    for theme in README_THEMES:
        target = ROOT / f"docs/content/assets/selective-read-readme-{theme}.svg"
        rendered = render(theme, values)
        if args.check:
            assert target.read_text() == rendered, target
        else:
            target.write_text(rendered)
        print(target.relative_to(ROOT))


if __name__ == "__main__":
    main()

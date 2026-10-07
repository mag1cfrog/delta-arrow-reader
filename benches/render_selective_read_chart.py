#!/usr/bin/env python3
"""Render the SF10 README chart from the published open-query medians."""

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
PANELS = (("", "Without deletion vectors", 40), (".dv", "With deletion vectors", 200))


def load_medians():
    source = ROOT / "docs/content/benchmarks/selective-read-summary.csv"
    with source.open(newline="") as stream:
        rows = [row for row in csv.DictReader(stream) if row["execution_mode"] == "open"]
    values = {}
    for row in rows:
        assert (row["status"], row["eligible"], row["samples"]) == ("success", "True", "5")
        value = float(row["open_query_ns_median"]) / 1e9
        assert math.isfinite(value) and value > 0
        values[row["case_id"], row["reader_id"]] = value
    expected = {
        (f"production.{layout}{suffix}", reader)
        for layout in LAYOUTS for suffix, _, _ in PANELS for reader in READERS
    }
    assert len(rows) == len(values) == 40 and values.keys() == expected
    return values


def render(theme_name, values):
    theme = README_THEMES[theme_name]
    colors = (theme["engines"]["delta_arrow_reader"],) + {
        "light": ("#e1e5ea", "#b9c0c8", "#96a1af", "#738091"),
        "dark": ("#53606e", "#697786", "#83909e", "#a5afba"),
    }[theme_name]
    parts = [f'''<svg xmlns="http://www.w3.org/2000/svg" width="1200" height="824" viewBox="0 0 1200 824" role="img" aria-labelledby="title description">
<title id="title">TPC-H-derived selective-read query times</title>
<desc id="description">Eight cases compare five readers on 416-column and 90-column Delta tables derived from TPC-H lineitem at SF10. Row labels describe stored table columns and matching-row layout. Each group follows legend order. Bars show median seconds to open a table and consume the full result over five runs. Lower is faster. Both columns use linear axes starting at zero, with different scales: 40 seconds on the left and 200 on the right. Startup is excluded.</desc>
<style>text{{font-family:Inter,ui-sans-serif,-apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif;font-variant-numeric:tabular-nums}}</style>
<defs><linearGradient id="page" x1="0" y1="0" x2="1" y2="1"><stop offset="0" stop-color="{theme['background']}"/><stop offset="1" stop-color="{theme['background_end']}"/></linearGradient></defs>
<rect width="1200" height="824" fill="url(#page)"/>
<text x="44" y="32" fill="{theme['muted']}" font-size="12" font-weight="700" letter-spacing="2">TPC-H-DERIVED DATA (SF10) | SELECTIVE DELTA LAKE READS</text>
<text x="44" y="70" fill="{theme['text']}" font-size="32" font-weight="700">Fastest across eight workloads</text>
<text x="44" y="99" fill="{theme['muted']}" font-size="16">Median query time over five runs, seconds. Lower is faster.</text>''']
    for x, label, color in zip((44, 300, 465, 625, 780), LABELS, colors, strict=True):
        parts.append(f'''<rect x="{x}" y="121" width="10" height="10" rx="3" fill="{color}"/>
<text x="{x + 18}" y="131" fill="{theme['text']}" font-size="14">{label}</text>''')

    for column, (suffix, heading, maximum) in enumerate(PANELS):
        left = 44 + column * 584
        plot_left = left + 136
        plot_width = 328
        parts.append(f'<text x="{left}" y="197" fill="{theme["muted"]}" font-size="13">Table / matches</text>')
        parts.append(f'<text x="{plot_left}" y="172" fill="{theme["text"]}" font-size="18" font-weight="600">{heading}</text>')
        for tick in range(5):
            x = plot_left + plot_width * tick / 4
            parts.append(f'''<line x1="{x}" y1="206" x2="{x}" y2="756" stroke="{theme['grid']}" stroke-dasharray="3 6"/>
<text x="{x}" y="197" text-anchor="middle" fill="{theme['muted']}" font-size="13">{maximum * tick // 4} s</text>''')
        for row, layout in enumerate(LAYOUTS):
            case = f"production.{layout}{suffix}"
            top = 220 + row * 140
            query, arrangement = layout.split(".")
            case_label = f"{TABLE_COLUMNS[query]}-column table, {arrangement}, {heading.lower()}"
            assert all(values[case, READERS[0]] < values[case, reader] for reader in READERS[1:]), f"Headline does not match {case}"
            if row:
                parts.append(f'<line x1="{left}" y1="{top - 15}" x2="{left + 528}" y2="{top - 15}" stroke="{theme["grid"]}" stroke-dasharray="4 7"/>')
            parts.append(f'''<text x="{left}" y="{top + 43}" fill="{theme['text']}" font-size="18" font-weight="500">{TABLE_COLUMNS[query]} columns</text>
<text x="{left}" y="{top + 65}" fill="{theme['muted']}" font-size="14">{arrangement.capitalize()}</text>''')
            for index, (reader, label, color) in enumerate(zip(READERS, LABELS, colors, strict=True)):
                value = values[case, reader]
                assert value <= maximum, (case, reader, value)
                width = value / maximum * plot_width
                y = top + index * 22
                label_color = theme["text"] if index == 0 else theme["muted"]
                parts.append(f'''<rect x="{plot_left}" y="{y}" width="{width:.2f}" height="14" rx="5" fill="{color}"><title>{case_label}: {label}, {value:.3f} s</title></rect>
<text x="{plot_left + width + 8:.2f}" y="{y + 12}" fill="{label_color}" font-size="13">{value:.3f} s</text>''')
    parts.append(f'''<text x="44" y="780" fill="{theme['muted']}" font-size="13">8 logical CPUs | 8 GiB per reader | Emulated object storage: 200 ms +/-20 ms, shared 150 Mbps</text>
<text x="44" y="802" fill="{theme['muted']}" font-size="13">Open table + full result; startup excluded. Linear scales differ by column. TPC-H-derived data, not a TPC-H score.</text>
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

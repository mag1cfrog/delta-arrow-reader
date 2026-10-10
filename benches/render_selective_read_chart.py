#!/usr/bin/env python3
"""Render the SF10 README chart for the four deletion-vector cases."""

import argparse
import csv
import math
from pathlib import Path
import statistics

from render_selective_s3_chart import README_THEMES


ROOT = Path(__file__).resolve().parents[1]
READERS = ("delta-arrow-reader", "duckdb", "spark", "polars", "delta-rs")
LABELS = ("Delta Arrow Reader", "DuckDB", "Spark (single machine)", "Polars", "delta-rs")
LAYOUTS = ("q2.localized", "q2.scattered", "q4.localized", "q4.scattered")
TABLE_COLUMNS = {"q2": 416, "q4": 90}
MAX_SECONDS = {"q2": 80, "q4": 200}


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
    plot_width = 340
    accent = {"light": "#075985", "dark": "#7dd3fc"}[theme_name]
    other_bar = {"light": "#d0d7de", "dark": "#53606e"}[theme_name]
    parts = [f'''<svg xmlns="http://www.w3.org/2000/svg" width="1200" height="584" viewBox="0 0 1200 584" role="img" aria-labelledby="title description">
<title id="title">Faster Delta Lake reads</title>
<desc id="description">Five readers query 416-column and 90-column Delta tables with about 60 million rows derived from TPC-H lineitem at SF10. All four cases use deletion vectors to mark deleted rows. Matching rows are grouped together in the top panels and spread out within row groups in the bottom panels. Reader names on the left apply to both columns. Blue bars show Delta Arrow Reader; gray bars show the other readers. Bars show median seconds for the first complete query after table initialization over five independent runs. Linear scales start at zero; shorter bars are faster. The 416-column panels span 0 to 80 seconds, and the 90-column panels span 0 to 200 seconds, as labeled in the column headings. Compare bar lengths within the same column. Startup and table initialization are excluded. The full report also includes initialization costs and four cases without deletion vectors.</desc>
<style>text{{font-family:Inter,ui-sans-serif,-apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif;font-variant-numeric:tabular-nums}}</style>
<rect width="1200" height="584" fill="{theme['background']}"/>
<text x="44" y="50" fill="{theme['text']}" font-size="30" font-weight="700">Faster Delta Lake reads</text>
<text x="44" y="80" fill="{theme['muted']}" font-size="16">Query time in seconds. Shorter is faster.</text>''']
    for column, query in enumerate(TABLE_COLUMNS):
        plot_left = 260 + column * 510
        max_seconds = MAX_SECONDS[query]
        parts.append(f'''<text x="{plot_left}" y="124" fill="{theme["text"]}" font-size="22" font-weight="600">{TABLE_COLUMNS[query]} columns</text>
<text x="{plot_left + plot_width}" y="124" text-anchor="end" fill="{theme['muted']}" font-size="13">Scale: 0-{max_seconds} s</text>''')
        for row, arrangement in enumerate(("localized", "scattered")):
            case = f"production.{query}.{arrangement}.dv"
            top = 190 + row * 214
            matches = "Matches grouped" if arrangement == "localized" else "Matches spread out"
            case_label = f"{TABLE_COLUMNS[query]}-column table, {matches.lower()}, with deletion vectors"
            if column == 0:
                parts.append(f'<text x="44" y="{top - 36}" fill="{theme["text"]}" font-size="18" font-weight="500">{matches}</text>')
            for index, (reader, label) in enumerate(zip(READERS, LABELS, strict=True)):
                value = values[case, reader]
                assert 0 < value <= max_seconds, (case, reader, value)
                width = plot_width * value / max_seconds
                y = top + index * 28
                color = theme["engines"]["delta_arrow_reader"] if index == 0 else other_bar
                label_color = accent if index == 0 else theme["muted"]
                weight = 600 if index == 0 else 400
                if column == 0:
                    parts.append(f'<text x="44" y="{y + 5}" fill="{label_color}" font-size="16" font-weight="{weight}">{label}</text>')
                parts.append(f'''<rect x="{plot_left}" y="{y - 7}" width="{width:.2f}" height="14" rx="2" fill="{color}"><title>{case_label}: {label}, {value:.3f} s</title></rect>
<text x="{plot_left + width + 10:.2f}" y="{y + 5}" fill="{label_color}" font-size="15" font-weight="{weight}">{value:.2f} s</text>''')
    parts.append(f'''<text x="44" y="560" fill="{theme['muted']}" font-size="13">TPC-H-derived data, about 60M rows per table, with deletion vectors. Median of 5 first queries; startup and table initialization excluded.</text>
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

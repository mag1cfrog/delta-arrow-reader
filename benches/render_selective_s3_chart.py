#!/usr/bin/env python3
"""Validate and render the anonymized selective-S3 benchmark results."""

import argparse
import csv
import math
import statistics
from collections import Counter, defaultdict
from html import escape
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
RESULTS = ROOT / "docs/content/benchmarks/selective-s3-results.csv"
OUTPUT = ROOT / "docs/content/assets"
HEADER = (
    "benchmark_date",
    "engine",
    "engine_revision",
    "query",
    "round",
    "included",
    "position",
    "order",
    "wall_seconds",
    "server_seconds",
    "planning_seconds",
    "execution_seconds",
    "output_rows",
    "reported_files_read",
    "reported_remote_bytes",
    "result_cache_hit",
    "io_cache_bytes",
    "rss_before_mib",
    "rss_peak_mib",
    "rss_after_mib",
    "metadata_initialization_seconds",
    "metadata_rss_mib",
    "process_peak_rss_mib",
    "correctness_parity",
)
ENGINES = {
    "lakehouse_rt": ("Lakehouse RT Small", "Small"),
    "serverless_sql": ("Serverless SQL Small", "Small"),
    "delta_arrow_reader": ("Delta Arrow Reader", "0.6.0"),
    "delta_rs": (
        "delta-rs",
        "365fd2c2f5b825106b41b1c39410165334e5a687",
    ),
}
QUERIES = ("Q1", "Q2", "Q3", "Q4")
OUTPUT_ROWS = {"Q1": 20, "Q2": 718, "Q3": 1, "Q4": 668}
EXPECTED_MEDIANS = {
    ("lakehouse_rt", "Q1"): 0.959055,
    ("lakehouse_rt", "Q2"): 1.601068,
    ("lakehouse_rt", "Q3"): 1.004014,
    ("lakehouse_rt", "Q4"): 1.296959,
    ("serverless_sql", "Q1"): 1.447966,
    ("serverless_sql", "Q2"): 3.879621,
    ("serverless_sql", "Q3"): 1.359400,
    ("serverless_sql", "Q4"): 2.821148,
    ("delta_arrow_reader", "Q1"): 1.058943,
    ("delta_arrow_reader", "Q2"): 3.732986,
    ("delta_arrow_reader", "Q3"): 0.806196,
    ("delta_arrow_reader", "Q4"): 1.735564,
    ("delta_rs", "Q1"): 4.949590,
    ("delta_rs", "Q2"): 267.859799,
    ("delta_rs", "Q3"): 1.015567,
    ("delta_rs", "Q4"): 52.899171,
}
EXPECTED_RESOURCES = {
    "delta_arrow_reader": (12.033261, 116.972656, 431.144531),
    "delta_rs": (9.747569, 115.132812, 2751.625000),
}
THEMES = {
    "dark": {
        "background": "#0b0d10",
        "background_end": "#11151a",
        "text": "#f0f2f5",
        "muted": "#aab3c0",
        "grid": "#343b46",
        "engines": {
            "lakehouse_rt": "#a78bfa",
            "serverless_sql": "#38bdf8",
            "delta_arrow_reader": "#4ade80",
            "delta_rs": "#fbbf24",
        },
    },
    "light": {
        "background": "#ffffff",
        "background_end": "#f7f8fa",
        "text": "#1f2328",
        "muted": "#59636f",
        "grid": "#d2d8e0",
        "engines": {
            "lakehouse_rt": "#6d28d9",
            "serverless_sql": "#0369a1",
            "delta_arrow_reader": "#15803d",
            "delta_rs": "#a16207",
        },
    },
}
TICKS = (0.3, 1, 3, 10, 30, 100, 300)


def load_rows() -> list[dict[str, str]]:
    with RESULTS.open(newline="") as source:
        reader = csv.DictReader(source)
        assert tuple(reader.fieldnames or ()) == HEADER
        rows = list(reader)
    assert len(rows) == 144
    return rows


def optional_float(row: dict[str, str], field: str) -> float | None:
    value = row[field]
    return float(value) if value else None


def validate(rows: list[dict[str, str]]) -> None:
    grouped = defaultdict(list)
    rounds = defaultdict(list)

    for row in rows:
        engine = row["engine"]
        query = row["query"]
        assert row["benchmark_date"] == "2026-08-31"
        assert engine in ENGINES
        assert row["engine_revision"] == ENGINES[engine][1]
        assert query in QUERIES
        assert row["correctness_parity"] == "true"
        assert int(row["output_rows"]) == OUTPUT_ROWS[query]

        round_number = int(row["round"])
        position = int(row["position"])
        order = row["order"].split("-")
        assert 0 <= round_number <= 8
        assert row["included"] == str(round_number > 0).lower()
        assert sorted(order) == list(QUERIES)
        assert 1 <= position <= 4
        assert query == order[position - 1]
        wall = float(row["wall_seconds"])
        assert TICKS[0] <= wall <= TICKS[-1]

        if engine in {"lakehouse_rt", "serverless_sql"}:
            assert float(row["server_seconds"]) > 0
            assert not row["planning_seconds"]
            assert not row["execution_seconds"]
            assert row["result_cache_hit"] == "false"
            assert row["io_cache_bytes"] == "0"
            assert int(row["reported_files_read"]) > 0
            assert int(row["reported_remote_bytes"]) > 0
            for field in (
                "rss_before_mib",
                "rss_peak_mib",
                "rss_after_mib",
                "metadata_initialization_seconds",
                "metadata_rss_mib",
                "process_peak_rss_mib",
            ):
                assert not row[field]
        else:
            assert not row["server_seconds"]
            assert not row["result_cache_hit"]
            assert not row["io_cache_bytes"]
            planning = float(row["planning_seconds"])
            execution = float(row["execution_seconds"])
            assert math.isclose(planning + execution, wall, abs_tol=0.000002)
            for field in (
                "rss_before_mib",
                "rss_peak_mib",
                "rss_after_mib",
                "metadata_initialization_seconds",
                "metadata_rss_mib",
                "process_peak_rss_mib",
            ):
                assert optional_float(row, field) is not None
            if engine == "delta_arrow_reader":
                assert int(row["reported_files_read"]) > 0
                assert int(row["reported_remote_bytes"]) > 0
            else:
                assert not row["reported_files_read"]
                assert not row["reported_remote_bytes"]

        grouped[engine, query].append(row)
        rounds[engine, round_number].append(row)

    for (engine, round_number), values in rounds.items():
        assert len(values) == 4
        assert {int(row["position"]) for row in values} == {1, 2, 3, 4}
        orders = {row["order"] for row in values}
        assert len(orders) == 1
        ordered_queries = [
            row["query"] for row in sorted(values, key=lambda row: int(row["position"]))
        ]
        assert ordered_queries == next(iter(orders)).split("-")

    for key, values in grouped.items():
        assert len(values) == 9
        assert {int(row["round"]) for row in values} == set(range(9))
        measured = [row for row in values if row["included"] == "true"]
        assert Counter(int(row["position"]) for row in measured) == Counter(
            {1: 2, 2: 2, 3: 2, 4: 2}
        )
        median = statistics.median(float(row["wall_seconds"]) for row in measured)
        assert math.isclose(median, EXPECTED_MEDIANS[key], abs_tol=0.000001)

    for engine, expected in EXPECTED_RESOURCES.items():
        values = grouped[engine, "Q1"]
        actual = tuple(
            float(values[0][field])
            for field in (
                "metadata_initialization_seconds",
                "metadata_rss_mib",
                "process_peak_rss_mib",
            )
        )
        assert actual == expected
        assert all(
            tuple(
                float(row[field])
                for field in (
                    "metadata_initialization_seconds",
                    "metadata_rss_mib",
                    "process_peak_rss_mib",
                )
            )
            == expected
            for query in QUERIES
            for row in grouped[engine, query]
        )

    dar = [EXPECTED_MEDIANS["delta_arrow_reader", query] for query in QUERIES]
    rt = [EXPECTED_MEDIANS["lakehouse_rt", query] for query in QUERIES]
    serverless = [EXPECTED_MEDIANS["serverless_sql", query] for query in QUERIES]
    delta_rs = [EXPECTED_MEDIANS["delta_rs", query] for query in QUERIES]
    dar_rt = [value / baseline for value, baseline in zip(dar, rt, strict=True)]
    dar_serverless = [
        value / baseline for value, baseline in zip(dar, serverless, strict=True)
    ]
    assert round(math.prod(dar_rt) ** 0.25, 3) == 1.290
    assert round(sum(dar) / sum(rt), 3) == 1.509
    assert round(math.prod(dar_serverless) ** 0.25, 3) == 0.712
    assert round(sum(dar) / sum(serverless), 3) == 0.771
    delta_rs_speedups = [
        baseline / value for baseline, value in zip(delta_rs, dar, strict=True)
    ]
    assert round(min(delta_rs_speedups), 2) == 1.26
    assert round(max(delta_rs_speedups), 2) == 71.75
    q2_dar = [
        float(row["wall_seconds"])
        for row in grouped["delta_arrow_reader", "Q2"]
        if row["included"] == "true"
    ]
    assert min(q2_dar) == 1.830749
    assert max(q2_dar) == 20.385515


def measurements(rows: list[dict[str, str]]) -> dict[tuple[str, str], list[float]]:
    values = defaultdict(list)
    for row in rows:
        if row["included"] == "true":
            values[row["engine"], row["query"]].append(float(row["wall_seconds"]))
    return values


def x_position(value: float, left: float, width: float) -> float:
    minimum = math.log10(TICKS[0])
    maximum = math.log10(TICKS[-1])
    return left + (math.log10(value) - minimum) / (maximum - minimum) * width


def render(theme_name: str, values: dict[tuple[str, str], list[float]]) -> str:
    theme = THEMES[theme_name]
    width = 1200
    height = 875
    plot_left = 305
    plot_width = 835
    plot_top = 155
    row_height = 27
    group_gap = 25
    group_height = row_height * len(ENGINES) + group_gap
    plot_bottom = plot_top + group_height * len(QUERIES) - group_gap
    parts = [
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" '
        f'viewBox="0 0 {width} {height}" role="img" aria-labelledby="title description">',
        '<title id="title">Selective S3 query wall time across four Delta engines</title>',
        '<desc id="description">Each query has one row per engine. Horizontal lines '
        'show the minimum and maximum over eight measured rounds, and circles show '
        'the median. The horizontal axis uses a logarithmic scale. Lower is faster. '
        'The local reader used a Core Ultra 7 265H with 16 heterogeneous cores and '
        '19 GiB of usable RAM. A published Pro or Classic Small warehouse has 24 '
        'Broadwell physical cores, 48 virtual CPUs, and 366 GiB of RAM. An AWS '
        'virtual CPU is one hardware thread, so the CPU counts are not equivalent '
        'compute measurements. The tested serverless hardware is not published.</desc>',
        '<style>text{font-family:Inter,ui-sans-serif,-apple-system,'
        'BlinkMacSystemFont,"Segoe UI",sans-serif}</style>',
        '<defs><linearGradient id="page" x1="0" y1="0" x2="1" y2="1">'
        f'<stop offset="0" stop-color="{theme["background"]}"/>'
        f'<stop offset="1" stop-color="{theme["background_end"]}"/>'
        '</linearGradient></defs>',
        f'<rect width="{width}" height="{height}" fill="url(#page)"/>',
        f'<text x="44" y="35" fill="{theme["muted"]}" font-size="12" '
        'font-weight="700" letter-spacing="2">SELECTIVE S3 CASE STUDY</text>',
        f'<text x="44" y="72" fill="{theme["text"]}" font-size="30" '
        'font-weight="700">Open source on a laptop vs managed Small warehouses</text>',
        f'<text x="44" y="101" fill="{theme["muted"]}" font-size="16">'
        'Median and measured range over eight rounds. Lower is faster.</text>',
        f'<text x="{plot_left}" y="130" fill="{theme["muted"]}" font-size="12" '
        'font-weight="700" letter-spacing="1.4">WALL TIME (SECONDS, LOG SCALE)</text>',
    ]

    for tick in TICKS:
        x = x_position(tick, plot_left, plot_width)
        parts.extend(
            [
                f'<line x1="{x:.2f}" y1="{plot_top - 8}" x2="{x:.2f}" '
                f'y2="{plot_bottom + 8}" stroke="{theme["grid"]}" '
                'stroke-dasharray="3 6"/>',
                f'<text x="{x:.2f}" y="{plot_bottom + 30}" text-anchor="middle" '
                f'fill="{theme["muted"]}" font-size="13">{tick:g}</text>',
            ]
        )

    for query_index, query in enumerate(QUERIES):
        group_top = plot_top + query_index * group_height
        parts.extend(
            [
                f'<text x="44" y="{group_top + 46}" fill="{theme["text"]}" '
                f'font-size="24" font-weight="700">{query}</text>',
                f'<text x="44" y="{group_top + 67}" fill="{theme["muted"]}" '
                f'font-size="13">Table {chr(ord("A") + query_index)}</text>',
            ]
        )
        for engine_index, (engine, (label, _)) in enumerate(ENGINES.items()):
            y = group_top + engine_index * row_height
            color = theme["engines"][engine]
            samples = values[engine, query]
            low = min(samples)
            high = max(samples)
            median = statistics.median(samples)
            low_x = x_position(low, plot_left, plot_width)
            high_x = x_position(high, plot_left, plot_width)
            median_x = x_position(median, plot_left, plot_width)
            label_x = median_x + 12
            label_anchor = "start"
            if median_x > plot_left + plot_width - 62:
                label_x = median_x - 12
                label_anchor = "end"
            weight = "700" if engine == "delta_arrow_reader" else "500"
            parts.extend(
                [
                    f'<text x="280" y="{y + 5}" text-anchor="end" fill="{color}" '
                    f'font-size="14" font-weight="{weight}">{escape(label)}</text>',
                    f'<line x1="{low_x:.2f}" y1="{y}" x2="{high_x:.2f}" y2="{y}" '
                    f'stroke="{color}" stroke-width="5" stroke-linecap="round" '
                    'stroke-opacity="0.68"/>',
                    f'<circle cx="{low_x:.2f}" cy="{y}" r="3" fill="{color}"/>',
                    f'<circle cx="{high_x:.2f}" cy="{y}" r="3" fill="{color}"/>',
                    f'<circle cx="{median_x:.2f}" cy="{y}" r="7" fill="{color}" '
                    f'stroke="{theme["background"]}" stroke-width="2"/>',
                    f'<text x="{label_x:.2f}" y="{y + 5}" text-anchor="{label_anchor}" '
                    f'fill="{theme["text"]}" font-size="13" font-weight="700">'
                    f'{median:.3f}s</text>',
                ]
            )

    parts.extend(
        [
            f'<text x="44" y="730" fill="{theme["muted"]}" font-size="13">'
            'Local WSL2: Core Ultra 7 265H, 16 heterogeneous cores / 16 threads, '
            '19 GiB usable RAM</text>',
            f'<text x="44" y="754" fill="{theme["muted"]}" font-size="13">'
            'Published Pro/Classic Small reference: 24 Broadwell cores / 48 vCPUs, '
            '366 GiB total; workers: 16 cores / 32 vCPUs, 244 GiB</text>',
            f'<text x="44" y="778" fill="{theme["muted"]}" font-size="13">'
            'One AWS vCPU is one hardware thread; cross-generation CPU counts are not '
            'compute-equivalent</text>',
            f'<text x="44" y="802" fill="{theme["muted"]}" font-size="13">'
            'The backing hardware for Serverless and RT is not published; the Small '
            'configuration is sizing context</text>',
            f'<text x="44" y="842" fill="{theme["muted"]}" font-size="13">'
            '8 measured rounds after 1 discarded warmup | table initialization excluded | '
            'managed result cache disabled and I/O cache verified empty</text>',
            '</svg>',
        ]
    )
    return "\n".join(parts) + "\n"


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--check", action="store_true", help="verify the CSV and generated SVGs"
    )
    args = parser.parse_args()
    rows = load_rows()
    validate(rows)
    values = measurements(rows)
    OUTPUT.mkdir(parents=True, exist_ok=True)
    for theme in THEMES:
        target = OUTPUT / f"selective-s3-wall-{theme}.svg"
        rendered = render(theme, values)
        if args.check:
            assert target.read_text() == rendered
        else:
            target.write_text(rendered)
            print(target.relative_to(ROOT))
    print("validated 144 anonymized benchmark measurements")


if __name__ == "__main__":
    main()

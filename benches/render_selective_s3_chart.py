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
    "lakehouse_rt": ("Lakehouse//RT Small (Beta)", "Small"),
    "serverless_sql": ("Serverless SQL Small", "Small"),
    "delta_arrow_reader": ("Delta Arrow Reader", "0.6.0"),
    "delta_rs": (
        "delta-rs",
        "365fd2c2f5b825106b41b1c39410165334e5a687",
    ),
}
QUERIES = ("Q1", "Q2", "Q3", "Q4")
EXPECTED_ORDERS = (
    "Q1-Q2-Q4-Q3",
    "Q1-Q2-Q4-Q3",
    "Q2-Q3-Q1-Q4",
    "Q3-Q4-Q2-Q1",
    "Q4-Q1-Q3-Q2",
    "Q1-Q2-Q4-Q3",
    "Q2-Q3-Q1-Q4",
    "Q3-Q4-Q2-Q1",
    "Q4-Q1-Q3-Q2",
)
LATENCY_ENGINES = ("delta_arrow_reader", "lakehouse_rt", "serverless_sql")
LATENCY_TICKS = (0.3, 1, 3, 10, 30)
REMOTE_TICKS = (0, 10, 20, 30)
MEMORY_TICKS = (0, 1000, 2000, 3000)
OUTPUT_ROWS = {"Q1": 20, "Q2": 718, "Q3": 1, "Q4": 668}
PLANNED_FILES = {"Q1": 5, "Q2": 5, "Q3": 4, "Q4": 6}
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
EXPECTED_SERVER_MEDIANS = {
    ("lakehouse_rt", "Q1"): 0.887,
    ("lakehouse_rt", "Q2"): 1.0825,
    ("lakehouse_rt", "Q3"): 0.8885,
    ("lakehouse_rt", "Q4"): 0.763,
    ("serverless_sql", "Q1"): 1.2635,
    ("serverless_sql", "Q2"): 3.114,
    ("serverless_sql", "Q3"): 1.219,
    ("serverless_sql", "Q4"): 2.5215,
}
EXPECTED_REMOTE_MIB = {
    ("lakehouse_rt", "Q1"): 17.681,
    ("lakehouse_rt", "Q2"): 22.700,
    ("lakehouse_rt", "Q3"): 2.900,
    ("lakehouse_rt", "Q4"): 25.510,
    ("serverless_sql", "Q1"): 19.049,
    ("serverless_sql", "Q2"): 25.082,
    ("serverless_sql", "Q3"): 3.662,
    ("serverless_sql", "Q4"): 27.005,
    ("delta_arrow_reader", "Q1"): 8.436,
    ("delta_arrow_reader", "Q2"): 23.459,
    ("delta_arrow_reader", "Q3"): 3.303,
    ("delta_arrow_reader", "Q4"): 25.885,
}
EXPECTED_RESOURCES = {
    "delta_arrow_reader": (12.033261, 116.972656, 431.144531),
    "delta_rs": (9.747569, 115.132812, 2751.625000),
}
THEMES = {
    "dark": {
        "background": "#111417",
        "text": "#edf0f2",
        "muted": "#a2abb3",
        "grid": "#2a3035",
        "engines": {
            "lakehouse_rt": "#a5b4fc",
            "serverless_sql": "#94a3b8",
            "delta_arrow_reader": "#7dd3fc",
            "delta_rs": "#94a3b8",
        },
    },
    "light": {
        "background": "#ffffff",
        "text": "#20252b",
        "muted": "#68737d",
        "grid": "#e7e9e7",
        "engines": {
            "lakehouse_rt": "#7187a7",
            "serverless_sql": "#8793a3",
            "delta_arrow_reader": "#4c8fa8",
            "delta_rs": "#8793a3",
        },
    },
}
TICKS = (0.3, 1, 3, 10, 30, 100, 300)
README_TICKS = (0, 1, 2, 3, 4)
README_THEMES = {
    "dark": {
        "background": "#0b0d10",
        "background_end": "#11151a",
        "text": "#f0f2f5",
        "muted": "#98a2b0",
        "grid": "#29303a",
        "engines": {
            "delta_arrow_reader": "#7dd3fc",
            "lakehouse_rt": "#53606e",
            "serverless_sql": "#333c47",
        },
    },
    "light": {
        "background": "#ffffff",
        "background_end": "#f7f8fa",
        "text": "#1f2328",
        "muted": "#68707d",
        "grid": "#d9dee6",
        "engines": {
            "delta_arrow_reader": "#7dd3fc",
            "lakehouse_rt": "#e1e5ea",
            "serverless_sql": "#b9c0c8",
        },
    },
}


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
    remote_medians = {}

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
        assert row["order"] == EXPECTED_ORDERS[round_number]
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
        if key in EXPECTED_SERVER_MEDIANS:
            server_median = statistics.median(
                float(row["server_seconds"]) for row in measured
            )
            assert math.isclose(
                server_median, EXPECTED_SERVER_MEDIANS[key], abs_tol=0.000001
            )
        if key in EXPECTED_REMOTE_MIB:
            remote_mib = statistics.median(
                int(row["reported_remote_bytes"]) for row in measured
            ) / (1024 * 1024)
            assert round(remote_mib, 3) == EXPECTED_REMOTE_MIB[key]
            remote_medians[key] = remote_mib

    for engine in ("delta_arrow_reader", "lakehouse_rt"):
        for query in QUERIES:
            assert {
                int(row["reported_files_read"])
                for row in grouped[engine, query]
                if row["included"] == "true"
            } == {PLANNED_FILES[query]}

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
    assert [round(value, 2) for value in delta_rs_speedups] == [
        4.67,
        71.75,
        1.26,
        30.48,
    ]
    delta_rs_peak = EXPECTED_RESOURCES["delta_rs"][2]
    dar_peak = EXPECTED_RESOURCES["delta_arrow_reader"][2]
    assert round(delta_rs_peak / dar_peak, 1) == 6.4

    dar_remote = [
        remote_medians["delta_arrow_reader", query] for query in QUERIES
    ]
    rt_remote = [remote_medians["lakehouse_rt", query] for query in QUERIES]
    serverless_remote = [
        remote_medians["serverless_sql", query] for query in QUERIES
    ]
    assert [
        round((value / baseline - 1) * 100, 1)
        for value, baseline in zip(dar_remote, rt_remote, strict=True)
    ] == [-52.3, 3.3, 13.9, 1.5]
    assert [
        round((value / baseline - 1) * 100, 1)
        for value, baseline in zip(dar_remote, serverless_remote, strict=True)
    ] == [-55.7, -6.5, -9.8, -4.1]
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


def remote_measurements(
    rows: list[dict[str, str]],
) -> dict[tuple[str, str], list[float]]:
    values = defaultdict(list)
    for row in rows:
        if row["included"] == "true" and row["reported_remote_bytes"]:
            values[row["engine"], row["query"]].append(
                int(row["reported_remote_bytes"]) / (1024 * 1024)
            )
    return values


def x_position(
    value: float, left: float, width: float, domain: tuple[float, float]
) -> float:
    minimum = math.log10(domain[0])
    maximum = math.log10(domain[1])
    return left + (math.log10(value) - minimum) / (maximum - minimum) * width


def linear_position(
    value: float, left: float, width: float, domain: tuple[float, float]
) -> float:
    return left + (value - domain[0]) / (domain[1] - domain[0]) * width


def marker(
    engine: str, x: float, y: float, color: str, background: str, size: float = 5
) -> str:
    if engine == "delta_arrow_reader":
        return f'<circle cx="{x}" cy="{y}" r="{size}" fill="{color}"/>'
    if engine == "lakehouse_rt":
        points = f"{x},{y - size} {x + size},{y} {x},{y + size} {x - size},{y}"
        return f'<polygon points="{points}" fill="{color}"/>'
    return (
        f'<rect x="{x - size}" y="{y - size}" width="{size * 2}" '
        f'height="{size * 2}" fill="{background}" stroke="{color}" stroke-width="2"/>'
    )


def legend(theme_name: str, y: float) -> list[str]:
    theme = THEMES[theme_name]
    legend_x = {
        "delta_arrow_reader": 250,
        "lakehouse_rt": 480,
        "serverless_sql": 660,
    }
    parts = []
    for engine in LATENCY_ENGINES:
        x = legend_x[engine]
        color = theme["engines"][engine]
        label = ENGINES[engine][0].replace(" Small", "")
        parts.extend(
            [
                marker(engine, x, y, color, theme["background"], 4),
                f'<text x="{x + 12}" y="{y + 5}" fill="{theme["text"]}" '
                f'font-size="14" font-weight="400">{escape(label)}</text>',
            ]
        )
    return parts


def render_wall_time(
    theme_name: str, values: dict[tuple[str, str], list[float]]
) -> str:
    theme = THEMES[theme_name]
    width = 840
    height = 425
    plot_left = 95
    plot_width = 710
    plot_top = 145
    group_height = 64
    plot_bottom = plot_top + group_height * (len(QUERIES) - 1)
    domain = (LATENCY_TICKS[0], LATENCY_TICKS[-1])
    parts = [
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" '
        f'viewBox="0 0 {width} {height}" role="img" aria-labelledby="title description">',
        '<title id="title">Selective S3 latency</title>',
        '<desc id="description">Four selective S3 queries compare Lakehouse RT '
        'Small (Beta), Serverless SQL Small, and Delta Arrow Reader on a laptop. Thin '
        'horizontal whiskers show the minimum and maximum over eight measured rounds. '
        'The circle, diamond, and square show each median. The horizontal axis is '
        'logarithmic and lower is faster.</desc>',
        '<style>text{font-family:Inter,ui-sans-serif,-apple-system,'
        'BlinkMacSystemFont,"Segoe UI",sans-serif;font-variant-numeric:tabular-nums}</style>',
        f'<rect width="{width}" height="{height}" fill="{theme["background"]}"/>',
        f'<text x="32" y="40" fill="{theme["text"]}" font-size="26" '
        'font-weight="500">Selective S3 latency</text>',
        f'<text x="32" y="66" fill="{theme["muted"]}" font-size="14">'
        'p50 and min-max over 8 measured runs, seconds (log scale). Lower is faster.</text>',
    ]

    parts.extend(legend(theme_name, 99))

    for tick in LATENCY_TICKS:
        x = x_position(tick, plot_left, plot_width, domain)
        parts.extend(
            [
                f'<line x1="{x:.2f}" y1="{plot_top - 24}" x2="{x:.2f}" '
                f'y2="{plot_bottom + 24}" stroke="{theme["grid"]}"/>',
                f'<text x="{x:.2f}" y="{plot_bottom + 49}" text-anchor="middle" '
                f'fill="{theme["muted"]}" font-size="13">{tick:g}</text>',
            ]
        )

    for query_index, query in enumerate(QUERIES):
        query_y = plot_top + query_index * group_height
        parts.append(
            f'<text x="32" y="{query_y + 6}" fill="{theme["text"]}" '
            f'font-size="18" font-weight="500">{query}</text>'
        )
        offsets = (-11, 0, 11)
        for engine_index, engine in enumerate(LATENCY_ENGINES):
            y = query_y + offsets[engine_index]
            color = theme["engines"][engine]
            samples = values[engine, query]
            low = min(samples)
            high = max(samples)
            median = statistics.median(samples)
            low_x = x_position(low, plot_left, plot_width, domain)
            high_x = x_position(high, plot_left, plot_width, domain)
            median_x = x_position(median, plot_left, plot_width, domain)
            parts.extend(
                [
                    f'<line x1="{low_x:.2f}" y1="{y}" x2="{high_x:.2f}" y2="{y}" '
                    f'stroke="{color}" stroke-width="1.5"/>',
                    f'<line x1="{low_x:.2f}" y1="{y - 3}" x2="{low_x:.2f}" '
                    f'y2="{y + 3}" stroke="{color}" stroke-width="1.5"/>',
                    f'<line x1="{high_x:.2f}" y1="{y - 3}" x2="{high_x:.2f}" '
                    f'y2="{y + 3}" stroke="{color}" stroke-width="1.5"/>',
                    marker(
                        engine,
                        round(median_x, 2),
                        y,
                        color,
                        theme["background"],
                    ),
                ]
            )

    parts.append('</svg>')
    return "\n".join(parts) + "\n"


def render_readme_latency(
    theme_name: str, values: dict[tuple[str, str], list[float]]
) -> str:
    theme = README_THEMES[theme_name]
    width = 1200
    height = 610
    plot_left = 150
    plot_width = 900
    plot_top = 190
    group_height = 96
    bar_height = 14
    bar_gap = 8
    domain = (README_TICKS[0], README_TICKS[-1])
    parts = [
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" '
        f'viewBox="0 0 {width} {height}" role="img" aria-labelledby="title description">',
        '<title id="title">Laptop Delta reads compared with managed warehouses</title>',
        '<desc id="description">Median query time for four existing selective Delta '
        'queries. Delta Arrow Reader ran from a laptop and was faster than Databricks '
        'Serverless SQL Small on all four. It was faster than Lakehouse RT Small '
        '(Beta) on Q3. '
        'Lower is better.</desc>',
        '<style>text{font-family:Inter,ui-sans-serif,-apple-system,'
        'BlinkMacSystemFont,"Segoe UI",sans-serif;font-variant-numeric:tabular-nums}</style>',
        '<defs><linearGradient id="page" x1="0" y1="0" x2="1" y2="1">'
        f'<stop offset="0" stop-color="{theme["background"]}"/>'
        f'<stop offset="1" stop-color="{theme["background_end"]}"/>'
        '</linearGradient></defs>',
        f'<rect width="{width}" height="{height}" fill="url(#page)"/>',
        f'<text x="44" y="32" fill="{theme["muted"]}" font-size="12" '
        'font-weight="700" letter-spacing="2">SELECTIVE DELTA READS FROM S3</text>',
        f'<text x="44" y="70" fill="{theme["text"]}" font-size="32" '
        'font-weight="700">Laptop vs managed warehouses</text>',
        f'<text x="44" y="99" fill="{theme["muted"]}" font-size="16">'
        'Median query time over eight measured runs. Lower is faster.</text>',
    ]

    legend_x = 44
    for engine in LATENCY_ENGINES:
        label = ENGINES[engine][0]
        color = theme["engines"][engine]
        label_color = (
            theme["text"] if engine == "delta_arrow_reader" else theme["muted"]
        )
        parts.extend(
            [
                f'<rect x="{legend_x}" y="121" width="10" height="10" rx="3" '
                f'fill="{color}"/>',
                f'<text x="{legend_x + 18}" y="131" fill="{label_color}" '
                f'font-size="14">{escape(label)}</text>',
            ]
        )
        legend_x += 275

    for tick in README_TICKS:
        x = linear_position(tick, plot_left, plot_width, domain)
        parts.extend(
            [
                f'<line x1="{x:.2f}" y1="{plot_top - 28}" x2="{x:.2f}" '
                f'y2="{plot_top + group_height * len(QUERIES) - 22}" '
                f'stroke="{theme["grid"]}" stroke-dasharray="3 6"/>',
                f'<text x="{x:.2f}" y="{plot_top - 39}" text-anchor="middle" '
                f'fill="{theme["muted"]}" font-size="13">{tick}</text>',
            ]
        )
    parts.append(
        f'<text x="1156" y="{plot_top - 39}" text-anchor="end" '
        f'fill="{theme["muted"]}" font-size="13">Seconds</text>'
    )

    for query_index, query in enumerate(QUERIES):
        group_top = plot_top + query_index * group_height
        query_y = group_top + 25
        if query_index:
            separator_y = group_top - 15
            parts.append(
                f'<line x1="44" y1="{separator_y}" x2="1156" y2="{separator_y}" '
                f'stroke="{theme["grid"]}" stroke-dasharray="4 7"/>'
            )
        parts.append(
            f'<text x="44" y="{query_y + 5}" fill="{theme["text"]}" '
            f'font-size="18" font-weight="500">{query}</text>'
        )
        for engine_index, engine in enumerate(LATENCY_ENGINES):
            median = statistics.median(values[engine, query])
            bar_y = group_top + engine_index * (bar_height + bar_gap)
            bar_width = linear_position(median, 0, plot_width, domain)
            color = theme["engines"][engine]
            label_color = (
                theme["text"] if engine == "delta_arrow_reader" else theme["muted"]
            )
            parts.extend(
                [
                    f'<rect x="{plot_left}" y="{bar_y}" width="{bar_width:.2f}" '
                    f'height="{bar_height}" rx="5" fill="{color}"/>',
                    f'<text x="{plot_left + bar_width + 8:.2f}" '
                    f'y="{bar_y + bar_height - 2}" fill="{label_color}" '
                    f'font-size="13">{median:.3f} s</text>',
                ]
            )

    parts.extend(
        [
            f'<text x="44" y="584" fill="{theme["muted"]}" font-size="13">'
            'Laptop over public WAN | 8 measured runs per query | result parity verified'
            '</text>',
            '</svg>',
        ]
    )
    return "\n".join(parts) + "\n"


def render_remote_bytes(
    theme_name: str, values: dict[tuple[str, str], list[float]]
) -> str:
    theme = THEMES[theme_name]
    width = 840
    height = 400
    plot_left = 95
    plot_width = 710
    plot_top = 145
    group_height = 58
    plot_bottom = plot_top + group_height * (len(QUERIES) - 1)
    domain = (REMOTE_TICKS[0], REMOTE_TICKS[-1])
    parts = [
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" '
        f'viewBox="0 0 {width} {height}" role="img" aria-labelledby="title description">',
        '<title id="title">RT (Beta) did not read materially less</title>',
        '<desc id="description">Median reported remote bytes for four selective S3 '
        'queries. Delta Arrow Reader reports fewer bytes than Serverless SQL on every '
        'query. It reports half as many bytes as Lakehouse RT (Beta) on Q1 and '
        'similar amounts on the other three queries.</desc>',
        '<style>text{font-family:Inter,ui-sans-serif,-apple-system,'
        'BlinkMacSystemFont,"Segoe UI",sans-serif;font-variant-numeric:tabular-nums}</style>',
        f'<rect width="{width}" height="{height}" fill="{theme["background"]}"/>',
        f'<text x="32" y="40" fill="{theme["text"]}" font-size="26" '
        'font-weight="500">RT (Beta) did not read materially less</text>',
        f'<text x="32" y="66" fill="{theme["muted"]}" font-size="14">'
        'Median reported remote bytes per query, MiB</text>',
    ]
    parts.extend(legend(theme_name, 99))

    for tick in REMOTE_TICKS:
        x = linear_position(tick, plot_left, plot_width, domain)
        parts.extend(
            [
                f'<line x1="{x:.2f}" y1="{plot_top - 24}" x2="{x:.2f}" '
                f'y2="{plot_bottom + 24}" stroke="{theme["grid"]}"/>',
                f'<text x="{x:.2f}" y="{plot_bottom + 49}" text-anchor="middle" '
                f'fill="{theme["muted"]}" font-size="13">{tick:g}</text>',
            ]
        )

    for query_index in range(1, len(QUERIES)):
        y = plot_top + (query_index - 0.5) * group_height
        parts.append(
            f'<line x1="32" y1="{y:.2f}" x2="805" y2="{y:.2f}" '
            f'stroke="{theme["grid"]}" stroke-dasharray="4 6"/>'
        )

    offsets = (-10, 0, 10)
    for query_index, query in enumerate(QUERIES):
        query_y = plot_top + query_index * group_height
        parts.append(
            f'<text x="32" y="{query_y + 6}" fill="{theme["text"]}" '
            f'font-size="18" font-weight="500">{query}</text>'
        )
        for engine_index, engine in enumerate(LATENCY_ENGINES):
            y = query_y + offsets[engine_index]
            color = theme["engines"][engine]
            median = statistics.median(values[engine, query])
            x = linear_position(median, plot_left, plot_width, domain)
            parts.append(marker(engine, round(x, 2), y, color, theme["background"]))

    parts.append('</svg>')
    return "\n".join(parts) + "\n"


def render_delta_rs_comparison(
    theme_name: str, values: dict[tuple[str, str], list[float]]
) -> str:
    theme = THEMES[theme_name]
    width = 840
    height = 500
    plot_left = 95
    plot_width = 710
    plot_top = 160
    group_height = 44
    plot_bottom = plot_top + group_height * (len(QUERIES) - 1)
    time_domain = (TICKS[0], TICKS[-1])
    memory_y = 410
    memory_domain = (MEMORY_TICKS[0], MEMORY_TICKS[-1])
    parts = [
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" '
        f'viewBox="0 0 {width} {height}" role="img" aria-labelledby="title description">',
        '<title id="title">Same laptop: Delta Arrow Reader and delta-rs</title>',
        '<desc id="description">Median query time and peak process memory for '
        'Delta Arrow Reader and delta-rs on the same laptop. Delta Arrow Reader '
        'was faster on all four queries and used less peak memory. Lower is better.</desc>',
        '<style>text{font-family:Inter,ui-sans-serif,-apple-system,'
        'BlinkMacSystemFont,"Segoe UI",sans-serif;font-variant-numeric:tabular-nums}</style>',
        f'<rect width="{width}" height="{height}" fill="{theme["background"]}"/>',
        f'<text x="32" y="40" fill="{theme["text"]}" font-size="26" '
        'font-weight="500">Same laptop: Delta Arrow Reader vs delta-rs</text>',
        f'<text x="32" y="66" fill="{theme["muted"]}" font-size="14">'
        'Median query time and peak process RSS. Lower is better.</text>',
        marker(
            "delta_arrow_reader",
            410,
            99,
            theme["engines"]["delta_arrow_reader"],
            theme["background"],
            4,
        ),
        f'<text x="422" y="104" fill="{theme["text"]}" font-size="14">'
        'Delta Arrow Reader</text>',
        marker(
            "delta_rs",
            650,
            99,
            theme["engines"]["delta_rs"],
            theme["background"],
            4,
        ),
        f'<text x="662" y="104" fill="{theme["text"]}" font-size="14">'
        'delta-rs</text>',
        f'<text x="32" y="130" fill="{theme["muted"]}" font-size="14">'
        'Median query time, seconds (log scale)</text>',
    ]

    for tick in TICKS:
        x = x_position(tick, plot_left, plot_width, time_domain)
        parts.extend(
            [
                f'<line x1="{x:.2f}" y1="{plot_top - 16}" x2="{x:.2f}" '
                f'y2="{plot_bottom + 24}" stroke="{theme["grid"]}"/>',
                f'<text x="{x:.2f}" y="{plot_bottom + 49}" text-anchor="middle" '
                f'fill="{theme["muted"]}" font-size="13">{tick:g}</text>',
            ]
        )

    for query_index in range(1, len(QUERIES)):
        y = plot_top + (query_index - 0.5) * group_height
        parts.append(
            f'<line x1="32" y1="{y:.2f}" x2="805" y2="{y:.2f}" '
            f'stroke="{theme["grid"]}" stroke-dasharray="4 6"/>'
        )

    for query_index, query in enumerate(QUERIES):
        y = plot_top + query_index * group_height
        dar_x = x_position(
            statistics.median(values["delta_arrow_reader", query]),
            plot_left,
            plot_width,
            time_domain,
        )
        delta_rs_x = x_position(
            statistics.median(values["delta_rs", query]),
            plot_left,
            plot_width,
            time_domain,
        )
        parts.extend(
            [
                f'<text x="32" y="{y + 6}" fill="{theme["text"]}" '
                f'font-size="18" font-weight="500">{query}</text>',
                f'<line x1="{dar_x:.2f}" y1="{y}" x2="{delta_rs_x:.2f}" '
                f'y2="{y}" stroke="{theme["grid"]}" stroke-width="2"/>',
                marker(
                    "delta_arrow_reader",
                    round(dar_x, 2),
                    y,
                    theme["engines"]["delta_arrow_reader"],
                    theme["background"],
                    5,
                ),
                marker(
                    "delta_rs",
                    round(delta_rs_x, 2),
                    y,
                    theme["engines"]["delta_rs"],
                    theme["background"],
                    5,
                ),
            ]
        )

    parts.append(
        f'<text x="32" y="375" fill="{theme["muted"]}" font-size="14">'
        'Peak process RSS, MiB</text>'
    )
    for tick in MEMORY_TICKS:
        x = linear_position(tick, plot_left, plot_width, memory_domain)
        parts.extend(
            [
                f'<line x1="{x:.2f}" y1="{memory_y - 16}" x2="{x:.2f}" '
                f'y2="{memory_y + 16}" stroke="{theme["grid"]}"/>',
                f'<text x="{x:.2f}" y="{memory_y + 44}" text-anchor="middle" '
                f'fill="{theme["muted"]}" font-size="13">'
                f'{tick // 1000 if tick else 0}{"k" if tick else ""}</text>',
            ]
        )

    dar_memory_x = linear_position(
        EXPECTED_RESOURCES["delta_arrow_reader"][2],
        plot_left,
        plot_width,
        memory_domain,
    )
    delta_rs_memory_x = linear_position(
        EXPECTED_RESOURCES["delta_rs"][2],
        plot_left,
        plot_width,
        memory_domain,
    )
    parts.extend(
        [
            f'<text x="32" y="{memory_y + 6}" fill="{theme["text"]}" '
            'font-size="18" font-weight="500">RSS</text>',
            f'<line x1="{dar_memory_x:.2f}" y1="{memory_y}" '
            f'x2="{delta_rs_memory_x:.2f}" y2="{memory_y}" '
            f'stroke="{theme["grid"]}" stroke-width="2"/>',
            marker(
                "delta_arrow_reader",
                round(dar_memory_x, 2),
                memory_y,
                theme["engines"]["delta_arrow_reader"],
                theme["background"],
                5,
            ),
            marker(
                "delta_rs",
                round(delta_rs_memory_x, 2),
                memory_y,
                theme["engines"]["delta_rs"],
                theme["background"],
                5,
            ),
        ]
    )

    parts.append('</svg>')
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
    remote_values = remote_measurements(rows)
    OUTPUT.mkdir(parents=True, exist_ok=True)
    for theme in THEMES:
        outputs = {
            OUTPUT / f"selective-s3-wall-{theme}.svg": render_wall_time(theme, values),
            OUTPUT / f"selective-s3-readme-{theme}.svg": render_readme_latency(
                theme, values
            ),
            OUTPUT / f"selective-s3-remote-bytes-{theme}.svg": render_remote_bytes(
                theme, remote_values
            ),
            OUTPUT / f"selective-s3-delta-rs-comparison-{theme}.svg": (
                render_delta_rs_comparison(theme, values)
            ),
        }
        for target, rendered in outputs.items():
            if args.check:
                assert target.read_text() == rendered
            else:
                target.write_text(rendered)
                print(target.relative_to(ROOT))
    print("validated 144 anonymized benchmark measurements")


if __name__ == "__main__":
    main()

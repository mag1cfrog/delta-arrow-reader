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
    "lakehouse_rt": ("Lakehouse//RT Small", "Small"),
    "serverless_sql": ("Serverless SQL Small", "Small"),
    "delta_arrow_reader": ("Delta Arrow Reader", "0.6.0"),
    "delta_rs": (
        "delta-rs",
        "365fd2c2f5b825106b41b1c39410165334e5a687",
    ),
}
QUERIES = ("Q1", "Q2", "Q3", "Q4")
LATENCY_ENGINES = ("delta_arrow_reader", "lakehouse_rt", "serverless_sql")
LATENCY_TICKS = (0.3, 1, 3, 10, 30)
REMOTE_TICKS = (0, 10, 20, 30)
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
            "lakehouse_rt": "#c0c7ce",
            "serverless_sql": "#87929a",
            "delta_arrow_reader": "#5ecdb7",
            "delta_rs": "#899198",
        },
    },
    "light": {
        "background": "#ffffff",
        "text": "#20252b",
        "muted": "#68737d",
        "grid": "#e7e9e7",
        "engines": {
            "lakehouse_rt": "#3f4b59",
            "serverless_sql": "#a0a7ad",
            "delta_arrow_reader": "#0f766e",
            "delta_rs": "#737b82",
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


def render(theme_name: str, values: dict[tuple[str, str], list[float]]) -> str:
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
        'Small, Serverless SQL Small, and Delta Arrow Reader on a laptop. Thin '
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
        '<title id="title">RT did not read materially less</title>',
        '<desc id="description">Median reported remote bytes for four selective S3 '
        'queries. Delta Arrow Reader reports fewer bytes than Serverless SQL on every '
        'query. It reports half as many bytes as Lakehouse RT on Q1 and similar amounts '
        'on the other three queries.</desc>',
        '<style>text{font-family:Inter,ui-sans-serif,-apple-system,'
        'BlinkMacSystemFont,"Segoe UI",sans-serif;font-variant-numeric:tabular-nums}</style>',
        f'<rect width="{width}" height="{height}" fill="{theme["background"]}"/>',
        f'<text x="32" y="40" fill="{theme["text"]}" font-size="26" '
        'font-weight="500">RT did not read materially less</text>',
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
            OUTPUT / f"selective-s3-wall-{theme}.svg": render(theme, values),
            OUTPUT / f"selective-s3-remote-bytes-{theme}.svg": render_remote_bytes(
                theme, remote_values
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

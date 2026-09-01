#!/usr/bin/env python3
"""Validate and render the general reader benchmark results."""

import csv
from collections import Counter, defaultdict
from html import escape
from math import ceil
from pathlib import Path
from statistics import median


ROOT = Path(__file__).resolve().parents[1]
RESULTS = ROOT / "docs/content/benchmarks.md"
MEASUREMENTS = ROOT / "docs/content/benchmarks/reader-results.csv"
OUTPUT = ROOT / "docs/content/assets"
READERS = ("Delta Arrow Reader", "delta-rs", "DuckDB", "Polars", "Daft")
READER_KEYS = ("delta-arrow-reader", "delta-rs", "duckdb", "polars", "daft")
WORKLOAD_KEYS = ("mixed-column", "text", "dv-limit", "dv-full")
WORKLOADS = (
    ("Mixed-column projection", "Mixed-column projection", "Mixed projection"),
    ("3 GB text projection", "3 GB text projection", "3 GB text"),
    (
        "Read one row from a table with deletion vectors",
        "Read one row with deletion vectors",
        "DV first row",
    ),
    (
        "Scan a full table with deletion vectors",
        "Full deletion-vector scan",
        "DV full scan",
    ),
)
WORKLOAD_READERS = {
    "mixed-column": ("delta-arrow-reader", "delta-rs", "duckdb", "polars"),
    "text": ("delta-arrow-reader", "delta-rs", "duckdb", "polars", "daft"),
    "dv-limit": ("delta-arrow-reader", "delta-rs", "duckdb", "polars"),
    "dv-full": ("delta-arrow-reader", "delta-rs", "duckdb", "polars"),
}
EXPECTED_RESULT_ROWS = {
    "mixed-column": "14490269+56350",
    "text": "2697774",
    "dv-limit": "1",
    "dv-full": "199999800",
}
MEASUREMENT_HEADER = (
    "benchmark_date",
    "workload",
    "round",
    "included",
    "position",
    "reader",
    "wall_seconds",
    "cpu_seconds",
    "peak_rss_mib",
    "result_rows",
    "correctness_parity",
    "status",
)
DISPLAY_DECIMALS = {
    "wall": (3, 3, 4, 4),
    "cpu": (2, 2, 3, 2),
    "memory": (0, 0, 0, 0),
}
SELECTED_BACKGROUND_OPACITY = 0.055
METRICS = {
    "wall": (
        "Wall time",
        "Lower is faster",
        "Seconds",
        5,
        "Wall timing excludes process startup and Python imports",
    ),
    "cpu": (
        "CPU time",
        "Lower uses less compute",
        "CPU seconds",
        10,
        "Whole-process CPU includes startup and Python imports",
    ),
    "memory": (
        "Peak memory",
        "Lower uses less RAM",
        "Peak RSS (MiB)",
        1_000,
        "Whole-process RSS includes startup and Python imports",
    ),
}
THEMES = {
    "dark": {
        "background": "#0b0d10",
        "background_end": "#11151a",
        "text": "#f0f2f5",
        "muted": "#98a2b0",
        "grid": "#29303a",
        "series": ("#a5b4fc", "#c4b5fd", "#7dd3fc", "#6ee7b7"),
        "delta_rs": "#c2c8d0",
        "other": "#69727e",
    },
    "light": {
        "background": "#ffffff",
        "background_end": "#f7f8fa",
        "text": "#1f2328",
        "muted": "#68707d",
        "grid": "#d9dee6",
        "series": ("#c7d2fe", "#ddd6fe", "#bae6fd", "#a7f3d0"),
        "delta_rs": "#87909a",
        "other": "#69727e",
    },
}


def load_measurements() -> dict[str, list[tuple[str, list[float | None]]]]:
    with MEASUREMENTS.open(newline="") as source:
        reader = csv.DictReader(source)
        assert tuple(reader.fieldnames or ()) == MEASUREMENT_HEADER
        rows = list(reader)

    assert len(rows) == 149
    assert {row["included"] for row in rows} == {"true", "false"}
    measured = [row for row in rows if row["included"] == "true"]
    probes = [row for row in rows if row["included"] == "false"]
    assert len(measured) == 146
    assert len(probes) == 3
    assert {
        (row["workload"], row["reader"], row["status"])
        for row in probes
    } == {
        ("mixed-column", "daft", "deletion_vectors_not_supported"),
        ("dv-limit", "daft", "deletion_vectors_not_supported"),
        ("dv-full", "daft", "deletion_vectors_not_supported"),
    }

    grouped: defaultdict[tuple[str, str], list[dict[str, str]]] = defaultdict(list)
    orders: defaultdict[tuple[str, int], list[tuple[int, str]]] = defaultdict(list)
    for row in measured:
        workload = row["workload"]
        candidate = row["reader"]
        assert row["benchmark_date"] == "2026-09-01"
        assert candidate in WORKLOAD_READERS[workload]
        assert row["status"] == "success"
        assert row["correctness_parity"] == "true"
        assert row["result_rows"] == EXPECTED_RESULT_ROWS[workload]
        assert all(
            float(row[field]) > 0
            for field in ("wall_seconds", "cpu_seconds", "peak_rss_mib")
        )
        round_number = int(row["round"])
        position = int(row["position"])
        grouped[(workload, candidate)].append(row)
        orders[(workload, round_number)].append((position, candidate))

    for workload, candidates in WORKLOAD_READERS.items():
        expected_runs = 10 if workload == "text" else 8
        workload_orders = []
        for round_number in range(1, expected_runs + 1):
            order = sorted(orders[(workload, round_number)])
            assert [position for position, _ in order] == list(
                range(1, len(candidates) + 1)
            )
            workload_orders.append(tuple(candidate for _, candidate in order))
        positions = Counter(
            (candidate, position)
            for order in workload_orders
            for position, candidate in enumerate(order)
        )
        adjacent_pairs = Counter(
            pair for order in workload_orders for pair in zip(order, order[1:])
        )
        assert set(positions.values()) == {2}
        assert set(adjacent_pairs.values()) == {2}
        assert len(adjacent_pairs) == len(candidates) * (len(candidates) - 1)
        for candidate in candidates:
            assert len(grouped[(workload, candidate)]) == expected_runs

    parsed = {metric: [] for metric in METRICS}
    for workload, (_, _, display_name) in zip(WORKLOAD_KEYS, WORKLOADS, strict=True):
        values = {metric: [] for metric in METRICS}
        for reader in READER_KEYS:
            runs = grouped[(workload, reader)]
            values["wall"].append(
                median(float(row["wall_seconds"]) for row in runs) if runs else None
            )
            values["cpu"].append(
                median(float(row["cpu_seconds"]) for row in runs) if runs else None
            )
            values["memory"].append(
                median(float(row["peak_rss_mib"]) for row in runs) if runs else None
            )
        for metric in METRICS:
            parsed[metric].append((display_name, values[metric]))
    return parsed


def load_published_results() -> dict[str, list[tuple[str, list[float | None]]]]:
    lines = RESULTS.read_text().splitlines()
    header = "| Workload | " + " | ".join(READERS) + " |"
    starts = [index + 2 for index, line in enumerate(lines) if line == header]
    assert len(starts) == 2
    parsed = {metric: [] for metric in METRICS}

    for row, (expected_timing, expected_resource, display_name) in enumerate(WORKLOADS):
        timing_cells = [
            cell.strip()
            for cell in lines[starts[0] + row].strip("|").split("|")
        ]
        resource_cells = [
            cell.strip()
            for cell in lines[starts[1] + row].strip("|").split("|")
        ]
        assert timing_cells[0] == expected_timing
        assert resource_cells[0] == expected_resource

        wall_values = []
        cpu_values = []
        memory_values = []
        for timing, resource in zip(timing_cells[1:], resource_cells[1:], strict=True):
            if timing == resource == "Unsupported":
                wall_values.append(None)
                cpu_values.append(None)
                memory_values.append(None)
                continue
            cpu, memory = resource.split(" / ")
            wall_values.append(float(timing.removesuffix(" s")))
            cpu_values.append(float(cpu.removesuffix(" s")))
            memory_values.append(
                float(memory.removesuffix(" MiB").replace(",", ""))
            )

        for metric, values in (
            ("wall", wall_values),
            ("cpu", cpu_values),
            ("memory", memory_values),
        ):
            assert len(values) == len(READERS)
            parsed[metric].append((display_name, values))
    return parsed


def validate_published_results(
    measured: dict[str, list[tuple[str, list[float | None]]]],
    published: dict[str, list[tuple[str, list[float | None]]]],
) -> None:
    for metric in METRICS:
        for row_index, (measured_row, published_row) in enumerate(
            zip(measured[metric], published[metric], strict=True)
        ):
            assert measured_row[0] == published_row[0]
            for measured_value, published_value in zip(
                measured_row[1], published_row[1], strict=True
            ):
                if measured_value is None or published_value is None:
                    assert measured_value is published_value
                else:
                    decimals = DISPLAY_DECIMALS[metric][row_index]
                    assert f"{measured_value:.{decimals}f}" == (
                        f"{published_value:.{decimals}f}"
                    )


def relative_luminance(color: str) -> float:
    channels = [int(color[index : index + 2], 16) / 255 for index in (1, 3, 5)]
    linear = [
        channel / 12.92
        if channel <= 0.04045
        else ((channel + 0.055) / 1.055) ** 2.4
        for channel in channels
    ]
    return 0.2126 * linear[0] + 0.7152 * linear[1] + 0.0722 * linear[2]


def contrast_ratio(first: str, second: str) -> float:
    lighter, darker = sorted(
        (relative_luminance(first), relative_luminance(second)), reverse=True
    )
    return (lighter + 0.05) / (darker + 0.05)


def validate_theme_contrast() -> None:
    for theme in THEMES.values():
        assert all(
            contrast_ratio(theme[name], background) >= 3
            for name in ("delta_rs", "other")
            for background in (theme["background"], theme["background_end"])
        )


def tick_label(metric: str, value: float) -> str:
    return f"{value:,.0f}" if metric == "memory" else f"{value:g}"


def render(
    theme_name: str,
    metric: str,
    results: list[tuple[str, list[float | None]]],
) -> str:
    theme = THEMES[theme_name]
    title, guidance, axis_label, step, footer = METRICS[metric]
    maximum = max(
        value
        for _, values in results
        for value in values
        if value is not None
    )
    axis_max = ceil(maximum / step) * step
    plot_x = 92
    plot_width = 1056
    plot_top = 150
    plot_bottom = 500
    group_width = plot_width / len(READERS)
    bar_width = 24
    bar_gap = 7
    cluster_width = len(WORKLOADS) * bar_width + (len(WORKLOADS) - 1) * bar_gap
    parts = [
        '<svg xmlns="http://www.w3.org/2000/svg" width="1200" height="610" '
        'viewBox="0 0 1200 610" role="img" aria-labelledby="title description">',
        f'<title id="title">Delta reader {escape(title.lower())} comparison</title>',
        f'<desc id="description">{escape(guidance)}. Delta Arrow Reader keeps '
        'the workload colors shown in the legend. delta-rs is light gray and '
        'the other candidates are darker gray. Bars remain in legend order.</desc>',
        '<style>text{font-family:Inter,ui-sans-serif,-apple-system,'
        'BlinkMacSystemFont,"Segoe UI",sans-serif}</style>',
        '<defs><linearGradient id="page" x1="0" y1="0" x2="1" y2="1">'
        f'<stop offset="0" stop-color="{theme["background"]}"/>'
        f'<stop offset="1" stop-color="{theme["background_end"]}"/>'
        '</linearGradient></defs>',
        '<rect width="1200" height="610" fill="url(#page)"/>',
        f'<text x="44" y="32" fill="{theme["muted"]}" font-size="12" '
        'font-weight="700" '
        'letter-spacing="2">DELTA ARROW READER BENCHMARK</text>',
        f'<text x="44" y="70" fill="{theme["text"]}" font-size="32" '
        f'font-weight="700">{escape(title)}</text>',
        f'<text x="44" y="99" fill="{theme["muted"]}" font-size="16">'
        f'{escape(guidance)}</text>',
    ]

    legend_x = 44
    for workload_index, (_, _, workload) in enumerate(WORKLOADS):
        color = theme["series"][workload_index]
        parts.extend(
            [
                f'<circle cx="{legend_x + 5}" cy="127" r="5" fill="{color}"/>',
                f'<text x="{legend_x + 18}" y="132" fill="{theme["text"]}" '
                f'font-size="14">{escape(workload)}</text>',
            ]
        )
        legend_x += 215

    for tick in range(6):
        value = axis_max * tick / 5
        y = plot_bottom - (plot_bottom - plot_top) * tick / 5
        parts.extend(
            [
                f'<line x1="{plot_x}" y1="{y}" x2="{plot_x + plot_width}" y2="{y}" '
                f'stroke="{theme["grid"]}" stroke-dasharray="3 6"/>',
                f'<text x="76" y="{y + 5}" text-anchor="end" '
                f'fill="{theme["muted"]}" font-size="13">'
                f'{tick_label(metric, value)}</text>',
            ]
        )

    for reader_index, reader in enumerate(READERS):
        center_x = plot_x + group_width * (reader_index + 0.5)
        if reader == "Delta Arrow Reader":
            parts.append(
                f'<rect x="{plot_x + 4}" y="{plot_top - 10}" '
                f'width="{group_width - 8}" height="414" '
                f'rx="12" fill="{theme["text"]}" '
                f'fill-opacity="{SELECTED_BACKGROUND_OPACITY}"/>'
            )
        weight = "700" if reader == "Delta Arrow Reader" else "500"
        parts.append(
            f'<text x="{center_x}" y="530" text-anchor="middle" '
            f'fill="{theme["text"]}" font-size="16" font-weight="{weight}">'
            f'{escape(reader)}</text>'
        )
        unsupported = sum(values[reader_index] is None for _, values in results)
        if unsupported:
            parts.append(
                f'<text x="{center_x}" y="549" text-anchor="middle" '
                f'fill="{theme["muted"]}" font-size="12">'
                f'{unsupported} workloads not supported</text>'
            )

        for workload_index, (_, values) in enumerate(results):
            value = values[reader_index]
            if value is None:
                continue
            height = max(3, round(value / axis_max * (plot_bottom - plot_top)))
            bar_x = (
                center_x
                - cluster_width / 2
                + workload_index * (bar_width + bar_gap)
            )
            if reader == "Delta Arrow Reader":
                color = theme["series"][workload_index]
            elif reader == "delta-rs":
                color = theme["delta_rs"]
            else:
                color = theme["other"]
            parts.append(
                f'<rect x="{bar_x}" y="{plot_bottom - height}" '
                f'width="{bar_width}" height="{height}" rx="4" fill="{color}"/>'
            )

    parts.extend(
        [
            f'<line x1="{plot_x}" y1="{plot_bottom}" '
            f'x2="{plot_x + plot_width}" '
            f'y2="{plot_bottom}" stroke="{theme["grid"]}"/>',
            f'<text x="{plot_x + plot_width}" y="144" text-anchor="end" '
            f'fill="{theme["muted"]}" font-size="13">'
            f'{escape(axis_label)}</text>',
            f'<text x="44" y="586" fill="{theme["muted"]}" font-size="13">'
            f'Local MinIO | 16 logical CPUs | {escape(footer)}</text>',
            "</svg>",
        ]
    )
    return "\n".join(parts) + "\n"


def main() -> None:
    results = load_measurements()
    validate_published_results(results, load_published_results())
    validate_theme_contrast()
    OUTPUT.mkdir(parents=True, exist_ok=True)
    for metric in METRICS:
        for theme in THEMES:
            target = OUTPUT / f"reader-benchmark-{metric}-{theme}.svg"
            target.write_text(render(theme, metric, results[metric]))
            print(target.relative_to(ROOT))


if __name__ == "__main__":
    main()

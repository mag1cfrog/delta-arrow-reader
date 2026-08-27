#!/usr/bin/env python3
"""Render README comparison charts from the published benchmark tables."""

from html import escape
from math import ceil
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
RESULTS = ROOT / "docs/content/benchmarks.md"
OUTPUT = ROOT / "docs/content/assets"
READERS = ("Delta Arrow Reader", "delta-rs", "DuckDB", "Polars", "Daft")
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
METRICS = {
    "wall": ("Wall time", "Lower is faster", "Seconds", 5),
    "cpu": ("CPU time", "Lower uses less compute", "CPU seconds", 10),
    "memory": ("Peak memory", "Lower uses less RAM", "Peak RSS (MiB)", 1_000),
}
THEMES = {
    "dark": {
        "background": "#0b0d10",
        "background_end": "#11151a",
        "text": "#f0f2f5",
        "muted": "#98a2b0",
        "grid": "#29303a",
        "series": ("#a5b4fc", "#c4b5fd", "#7dd3fc", "#6ee7b7"),
    },
    "light": {
        "background": "#ffffff",
        "background_end": "#f7f8fa",
        "text": "#1f2328",
        "muted": "#68707d",
        "grid": "#d9dee6",
        "series": ("#c7d2fe", "#ddd6fe", "#bae6fd", "#a7f3d0"),
    },
}


def load_results() -> dict[str, list[tuple[str, list[float | None]]]]:
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


def tick_label(metric: str, value: float) -> str:
    return f"{value:,.0f}" if metric == "memory" else f"{value:g}"


def render(
    theme_name: str,
    metric: str,
    results: list[tuple[str, list[float | None]]],
) -> str:
    theme = THEMES[theme_name]
    title, guidance, axis_label, step = METRICS[metric]
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
        f'<desc id="description">{escape(guidance)}. Delta Arrow Reader is '
        'highlighted. Each color represents one workload.</desc>',
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
                f'rx="12" fill="{theme["text"]}" fill-opacity="0.055"/>'
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
            color = theme["series"][workload_index]
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
            'Local MinIO | 16 logical CPUs | process startup excluded</text>',
            "</svg>",
        ]
    )
    return "\n".join(parts) + "\n"


def main() -> None:
    results = load_results()
    OUTPUT.mkdir(parents=True, exist_ok=True)
    for metric in METRICS:
        for theme in THEMES:
            target = OUTPUT / f"reader-benchmark-{metric}-{theme}.svg"
            target.write_text(render(theme, metric, results[metric]))
            print(target.relative_to(ROOT))


if __name__ == "__main__":
    main()

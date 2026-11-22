"""Reproduce the two causes of the original regression-window differences."""

import gzip
import json
import math
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path
import sys

from capture_spark import FUNCTIONS, WORKSPACE, bench_input, encode


@dataclass
class RegressionState:
    count: int = 0
    mean_x: float = 0.0
    mean_y: float = 0.0
    ck: float = 0.0
    corr_ck: float = 0.0
    m2_x: float = 0.0
    m2_y: float = 0.0
    corr_m2_x: float = 0.0
    corr_m2_y: float = 0.0
    sum_x: float = 0.0
    sum_y: float = 0.0

    def add(self, x, y):
        if x is None or y is None:
            return
        next_count = self.count + 1.0
        dx, dy = x - self.mean_x, y - self.mean_y
        dx_n, dy_n = dx / next_count, dy / next_count
        mx, my = self.mean_x + dx_n, self.mean_y + dy_n
        self.count += 1
        self.ck += dx * (y - my)
        self.corr_ck += dy * (x - mx)
        self.m2_x += dx * (dx - dx_n)
        self.m2_y += dy * (dy - dy_n)
        self.corr_m2_x += dx * (x - mx)
        self.corr_m2_y += dy * (y - my)
        self.sum_x += x
        self.sum_y += y
        self.mean_x, self.mean_y = mx, my

    def retract(self, x, y):
        if x is None or y is None:
            return
        if self.count <= 1:
            self.__init__()
            return
        self.count -= 1
        dx, dy = x - self.mean_x, y - self.mean_y
        self.mean_x -= dx / self.count
        self.mean_y -= dy / self.count
        dx2, dy2 = x - self.mean_x, y - self.mean_y
        self.ck -= dx2 * dy
        self.corr_ck -= dy2 * dx
        self.m2_x -= dx * dx2
        self.m2_y -= dy * dy2
        self.corr_m2_x -= dx * dx2
        self.corr_m2_y -= dy * dy2
        self.sum_x -= x
        self.sum_y -= y

    def values(self, *, spark_r2):
        slope = None if self.m2_x == 0.0 else self.ck / self.m2_x
        x, y = self.corr_m2_x, self.corr_m2_y
        require_variance, constant_result = (x, y) if spark_r2 else (y, x)
        if require_variance == 0.0:
            r2 = None
        elif constant_result == 0.0:
            r2 = 1.0
        else:
            # The original reduced inputs never produce an underflowed product.
            product = x * y
            assert product != 0.0
            corr = self.corr_ck / math.sqrt(product) if product > 0.0 else math.nan
            r2 = corr * corr
        return {
            "regr_r2": r2,
            "regr_avgx": self.sum_x / self.count if self.count else None,
            "regr_avgy": self.sum_y / self.count if self.count else None,
            "regr_count": self.count,
            "regr_intercept": None if slope is None else self.mean_y - slope * self.mean_x,
            "regr_slope": slope,
            "regr_sxx": self.m2_x if self.count else None,
            "regr_sxy": self.corr_ck if self.count else None,
            "regr_syy": self.m2_y if self.count else None,
        }


def frames():
    _, rows = bench_input()
    groups = defaultdict(list)
    for row_id, group, p, d in rows:
        groups[group].append((row_id, None if p is None else float(p), d))
    for group in groups.values():
        group.sort()
        sliding = RegressionState()
        for index, (row_id, p, d) in enumerate(group):
            sliding.add(p, d)
            if index >= 5:
                sliding.retract(*group[index - 5][1:])
            members = group[max(0, index - 4) : index + 1]
            rebuilt = RegressionState()
            for _, x, y in members:
                rebuilt.add(x, y)
            yield row_id, members, sliding, rebuilt


def load_spark(path):
    with path.open() as source:
        records = list(map(json.loads, source))
    assert records[0]["spark_version"] == "4.2.0"
    return {(r["id"].removeprefix("original_"), r["ansi"]): dict(r["rows"])
            for r in records[1:] if r["id"].startswith("original_")}


def load_previous_dar():
    path = WORKSPACE / "benches/reports/functions-1406-comparisons.jsonl.gz"
    with gzip.open(path, "rt") as source:
        return {(r["id"].split(".")[-1], r["configuration"] == "spark-on"):
                dict(r["dar"]["result"]["rows"])
                for r in map(json.loads, source)
                if r.get("id", "").startswith("window.window.regr_")}


def main(spark_path):
    spark = load_spark(spark_path)
    baseline = load_previous_dar()
    mismatches = {name: defaultdict(int) for name in FUNCTIONS}
    details = {}
    for row_id, members, sliding, rebuilt in frames():
        old, pure, swapped, corrected = (
            sliding.values(spark_r2=False),
            rebuilt.values(spark_r2=False),
            sliding.values(spark_r2=True),
            rebuilt.values(spark_r2=True),
        )
        for name in FUNCTIONS:
            index = str(row_id)
            if (name, False) in baseline:
                assert encode(old[name]) == baseline[name, False][index], (name, index)
                assert baseline[name, False][index] == baseline[name, True][index], (name, index)
            expected = spark[name, False][index]
            assert expected == spark[name, True][index], (name, index)
            for label, values in (
                ("sliding", old), ("rebuild_only", pure),
                ("correct_r2_only", swapped), ("rebuild_and_correct_r2", corrected),
            ):
                mismatches[name][label] += encode(values[name]) != expected
        if row_id in (4099, 4106):
            details[str(row_id)] = {
                "pairs_x_y": [[x, y] for _, x, y in members if x is not None and y is not None],
                "sliding_corr_second_moments_x_y": [sliding.corr_m2_x, sliding.corr_m2_y],
                "rebuilt_corr_second_moments_x_y": [rebuilt.corr_m2_x, rebuilt.corr_m2_y],
                "sliding": old["regr_r2"], "rebuild_only": pure["regr_r2"],
                "corrected": corrected["regr_r2"],
            }
    print(json.dumps({"mismatches_per_ansi_mode": mismatches, "constant_y_rows": details}, indent=2))


if __name__ == "__main__":
    main(Path(sys.argv[1]))

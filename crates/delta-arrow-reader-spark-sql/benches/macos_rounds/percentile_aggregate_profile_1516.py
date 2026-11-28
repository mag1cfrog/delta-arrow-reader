"""Attribute Spark percentile aggregate summary work on native macOS."""

from statistics import median
import sys
from unittest.mock import patch

sys.dont_write_bytecode = True
import percentile_profile_1516 as profile

EXTRA = {**profile.EXTRA, "summary_flush": "SummaryFlush", "summary_sort": "SummarySort"}
base_overlay = profile.overlay


def overlay(workspace, targets):
    paths, phases = base_overlay(workspace, targets)
    path = workspace / profile.SAIL
    replace = profile.frozen.mac_build.replace
    source = replace(
        paths[path],
        "        probe::time(Phase::SummaryCompress, 0, || self.summary.compress());",
        "        self.summary.compress();",
    )
    source = replace(
        source,
        "        self.pending_values.sort_by(f64::total_cmp);",
        """        let rows = self.pending_values.len();
        let started = std::time::Instant::now();
        probe::time(Phase::SummarySort, rows, || self.pending_values.sort_by(f64::total_cmp));""",
    )
    source = replace(
        source,
        """        self.samples = samples;
    }

    fn compress(&mut self) {""",
        """        self.samples = samples;
        probe::record(Phase::SummaryFlush, started, rows);
    }

    fn compress(&mut self) {""",
    )
    start = source.index("    fn compress(&mut self) {\n") + len("    fn compress(&mut self) {\n")
    end = source.index("\n    }\n\n    fn merge(", start)
    body = source[start:end]
    source = (
        source[:start]
        + "        let rows = self.samples.len() + self.pending_values.len();\n"
        + "        probe::time(Phase::SummaryCompress, rows, || {\n"
        + "".join(f"    {line}\n" for line in body.splitlines())
        + "        });"
        + source[end:]
    )
    paths[path] = source
    return paths, phases


def phase_result(records, rows):
    names = tuple(records[0])
    stages = ("update", "merge", "state", "evaluate")
    result = {key: median(record[key][0] / rows for record in records) for key in names}
    result["aggregate_work"] = median(sum(record[key][0] for key in stages) / rows for record in records)
    result["buffer_merge_without_sort"] = median(
        (record["summary_flush"][0] - record["summary_sort"][0]) / rows for record in records
    )
    return result


if __name__ == "__main__":
    with patch.multiple(profile, EXTRA=EXTRA, overlay=overlay, phase_result=phase_result, __file__=__file__):
        profile.main()

"""Attribute the ordered Float64 TRY_AVG update on native macOS."""

from statistics import median
import sys
from unittest.mock import patch

sys.dont_write_bytecode = True
import percentile_profile_1516 as profile

EXTRA = {**profile.EXTRA, "try_float_update": "TryFloatUpdate"}
base_overlay = profile.overlay


def overlay(workspace, targets):
    paths, phases = base_overlay(workspace, targets)
    del paths[workspace / profile.SAIL]
    source = workspace / "vendor/sail/crates/sail-function/src/aggregate/try_avg.rs"
    text = source.read_text()
    import_at = text.index("\nuse ") + 1
    text = text[:import_at] + profile.IMPORT + text[import_at:]
    beginning = "    fn update_f64(&mut self, arr: &PrimitiveArray<Float64Type>) {\n"
    start = text.index(beginning) + len(beginning)
    end = text.index("\n    }\n\n    fn update_dec128", start)
    body = text[start:end]
    text = (
        text[:start]
        + "        let rows = arr.len() - arr.null_count();\n"
        + "        probe::time(Phase::TryFloatUpdate, rows, || {\n"
        + "".join(f"    {line}\n" for line in body.splitlines())
        + "        });"
        + text[end:]
    )
    paths[source] = text
    return paths, phases


def phase_result(records, rows):
    return {name: median(record[name][0] / rows for record in records) for name in records[0]}


if __name__ == "__main__":
    with patch.multiple(profile, EXTRA=EXTRA, overlay=overlay, phase_result=phase_result, __file__=__file__):
        profile.main()

"""Compare exact observations and repeated RANGE work before candidate clocks."""
import argparse
import json
from pathlib import Path

def lines(path):
    return [json.loads(line) for line in path.read_text().splitlines()]

def compare(baseline, candidate):
    for path in (baseline, candidate):
        assert (path / "eligibility.json").is_file(), "trace must seal Spark and native agreement"
        assert all(not (path / f"{mode}.stderr").read_bytes() for mode in ("canonical", "observed"))
    assert lines(baseline / "canonical.jsonl") == lines(candidate / "canonical.jsonl")
    before = {row["id"]: row for row in lines(baseline / "observed.jsonl")}
    after = {row["id"]: row for row in lines(candidate / "observed.jsonl")}
    assert len(before) == len(after) == 63 and before.keys() == after.keys()
    reduced = []
    native_contexts = 0
    counter_snapshots = ("before_eof", "after_eof")
    for context, original in before.items():
        current = after[context]
        count = current["rows"]
        batch = current["batch"]
        spark_pending = ((current["configuration"] != "native" and current["shape"] == "constant")
                         or (current["configuration"] == "spark-off" and current["shape"] == "wrapped"))
        assert {key:value for key,value in current.items() if key not in counter_snapshots} == {
            key:value for key,value in original.items() if key not in counter_snapshots
        }, context
        if current["configuration"] == "native":
            native_contexts += 1
            assert current == original, context
        if spark_pending:
            old = original["before_eof"]["counters"]
            new = current["before_eof"]["counters"]
            assert new["upper_comparisons"] == count, (context,new["upper_comparisons"])
            assert old["upper_comparisons"] == count * (count // batch + 1) // 2
            for stage in ("before_eof", "after_eof"):
                assert {k:v for k,v in original[stage]["counters"].items() if k!="upper_comparisons"} == {
                    k:v for k,v in current[stage]["counters"].items() if k!="upper_comparisons"
                }, (context,stage)
            eof = current["after_eof"]["counters"]["upper_comparisons"]
            assert eof == count, (context,eof)
            reduced.append({"context":context, "baseline_before_eof":old["upper_comparisons"],
                            "candidate_before_eof":new["upper_comparisons"],
                            "baseline_end_to_end":original["after_eof"]["counters"]["upper_comparisons"],
                            "candidate_end_to_end":eof})
    assert len(reduced)==27 and native_contexts==21
    return {"canonical_contexts_equal":63,"native_complete_contexts_equal":21,
            "spark_pending_contexts":reduced,"clock_eligible_cells":27}

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("baseline", type=Path)
    parser.add_argument("candidate", type=Path)
    args = parser.parse_args()
    print(json.dumps(compare(args.baseline,args.candidate),indent=2))

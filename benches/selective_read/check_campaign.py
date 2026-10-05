"""Bounded schedule/accounting and real subprocess failure check; no engine builds."""

from collections import Counter
from itertools import combinations
import json
from pathlib import Path
import sys
import tempfile

import campaign
import run
import supervise


FAKE = '''import json, os, sys, time
from pathlib import Path
p = json.loads(Path(sys.argv[1]).read_text())
out = Path(sys.argv[2]); out.mkdir()
fd = int(os.environ["SELECTIVE_READ_CONTROL_FD"])
def emit(phase, index=None, query=None):
    os.write(fd, (json.dumps(dict(phase=phase, query_index=index, query=query, initialization_ns=10)) + "\\n").encode())
mode = p.get("test", "success")
if mode == "crash": sys.exit(7)
if mode == "truncated": os.write(fd, b'{"phase":'); sys.exit(0)
if mode == "invalid-progress": os.write(fd, b'{}\\n'); time.sleep(2)
reuse = p["execution_mode"] == "reuse"
emit("initialization" if reuse else "open")
if mode == "initialization-timeout": time.sleep(2)
queries = []
count = (2 if p.get("comparison_revision") == 4 else 10) if reuse else 1
if mode == "extra-query": count += 1
if mode == "missing-query": count -= 1
for index in range(count):
    if reuse: emit("query", index)
    if mode == "query-timeout" and index == 1: time.sleep(2)
    query = dict(query_index=index, output_rows=3, output_batches=1, completion_ns=100, first_batch_ns=20)
    if mode == "reported-timeout": query["completion_ns"] = 10**12
    emit("query_end", index, query); queries.append(query)
emit("cleanup")
if mode == "cleanup-timeout": time.sleep(2)
(out / "record.json").write_text("[]" if mode == "malformed" else json.dumps(dict(status="success", queries=queries)))
'''


def observation(value, reuse=False, revision=2):
    count = (2 if revision == 4 else 10) if reuse else 1
    return {"open_query_ns": None if reuse else value, "initialization_ns": 5 if reuse else None,
            "initialization_plus_query1_ns": value + 5 if reuse else None,
            "initialization_plus_all_queries_ns": count * value + 5 if reuse else None,
            "session_elapsed_ns": count * value + (5 if reuse else 0), "cleanup_ns": 1,
            "external_metrics": {key: 1 for key in ("process_user_cpu_ns", "process_system_cpu_ns", "process_cpu_ns", "peak_rss_bytes", "process_elapsed_ns")},
            "queries": [{"query_index": index, "output_rows": 3, "output_batches": 1,
                         "completion_ns": value, "first_batch_ns": 1, "first_batch_unavailable_reason": None} for index in range(count)]}


def check():
    assert campaign.distribution([1, 2, 3, 4]) == {"samples": 4, "median": 2.5, "q1": 1.75, "q3": 3.25, "iqr": 1.5}
    expected = {1: 10, 2: 12, 3: 12, 4: 16, 5: 10}
    comparison = {"comparison_revision": 3, "protocol_sha256": run.digest(run.AMENDMENT),
                  "base_protocol_sha256": run.digest(run.PROTOCOL), "workload_manifest_sha256": "1" * 64}
    entries = {r: {"runnable": True, "status": "success"} for r in campaign.READERS}
    slots = campaign.schedule({s: entries for s in campaign.large_workloads.SESSIONS}, "revision3", comparison)
    assert all(run.comparison_identity(slot) == comparison for slot in slots)
    assert all(s["job_id"] in campaign.large_workloads.SESSIONS for s in slots)
    reduced = comparison | {"comparison_revision": 4, "sampling_sha256": run.digest(run.SAMPLING)}
    assert run.comparison_identity(reduced) == reduced
    for change in ({"sampling_sha256": "0" * 64}, {"sampling_sha256": None}, {"comparison_revision": 3}):
        try:
            run.comparison_identity(reduced | change)
        except ValueError:
            pass
        else:
            raise AssertionError("stale or missing sampling identity accepted")
    for count in range(6):
        for subset in combinations(campaign.READERS, count):
            entries = {r: {"runnable": r in subset, "status": "success" if r in subset else "unsupported"} for r in campaign.READERS}
            inventory = {"case": entries, "reuse.li": entries}
            short = campaign.schedule(inventory, "reduced", reduced)
            assert all(run.comparison_identity(s) == reduced for s in short)
            for job in inventory:
                for reader in subset:
                    stages = Counter(s["stage"] for s in short if s["job_id"] == job and s["reader_id"] == reader)
                    assert stages == {"warmup": 1, "timing": 5, "plan": 1, "diagnostic": 1}
                positions = Counter((s["reader_id"], s["order"]) for s in short if s["job_id"] == job and s["stage"] == "timing")
                assert not positions or max(positions.values()) - min(positions.values()) <= 1
            short_rows = [{**s, "status": "success", "observation": observation(100, s["job_id"].startswith("reuse"), 4)}
                          for s in short if s["stage"] in ("warmup", "timing")]
            short_summary = campaign.summarize(inventory, short, short_rows, 1)
            for reader in subset:
                metrics = short_summary["reuse.li"][reader]["metrics"]
                assert metrics["query_1.completion_ns"]["samples"] == 5 and "query_2.completion_ns" not in metrics
            if short_rows:
                missing = next(row for row in short_rows if row["stage"] == "timing")
                short_rows.remove(missing)
                incomplete = campaign.summarize(inventory, short, short_rows, 1)[missing["job_id"]][missing["reader_id"]]
                assert not incomplete["eligible"] and incomplete["sample_statuses"]["not_run"] == 1
            slots = campaign.schedule(inventory, "check")
            assert len({s["run_id"] for s in slots}) == len(slots)
            assert all(s["reader_id"] in subset for s in slots)
            if not count:
                assert slots == []
                continue
            orders = campaign.balanced_orders(subset)
            assert set(Counter((r, p) for order in orders for p, r in enumerate(order)).values()) == {2}
            adjacent = Counter(pair for order in orders for pair in zip(order, order[1:]))
            assert len(adjacent) == count * (count - 1) and all(n == 2 for n in adjacent.values())
            timing = [s for s in slots if s["stage"] == "timing"]
            assert set(Counter((s["job_id"], s["reader_id"]) for s in timing).values()) == {expected[count]}
            assert max(slots.index(s) for s in timing) < min(i for i, s in enumerate(slots) if s["stage"] == "plan")
            for job in inventory:
                for stage in ("warmup", "diagnostic_warmup", "diagnostic", "observer_baseline"):
                    assert [s["reader_id"] for s in slots if s["job_id"] == job and s["stage"] == stage] == list(subset + subset[::-1])
            rows = [{**s, "status": "success", "observation": observation(100, s["job_id"].startswith("reuse"))}
                    for s in slots if s["stage"] in ("warmup", "timing")]
            summary = campaign.summarize(inventory, slots, rows, 1)
            assert all(summary["reuse.li"][r]["metrics"]["query_9.completion_ns"]["samples"] == expected[count] for r in subset)
            if campaign.READERS[0] in subset and count > 1:
                comparator = subset[1]
                assert summary["case"][comparator]["speedup_vs_dar"]["open_query_ns"]["value"] == 1
                for row in rows:
                    if row["reader_id"] == comparator:
                        row["observation"] = observation(50, row["job_id"].startswith("reuse"))
                assert campaign.summarize(inventory, slots, rows, 1)["case"][comparator]["speedup_vs_dar"]["open_query_ns"]["value"] == .5
                assert campaign.summarize(inventory, slots, rows, 101)["case"][comparator]["speedup_vs_dar"]["open_query_ns"]["value"] is None
                failed = next(row for row in rows if row["reader_id"] == comparator and row["job_id"] == "case" and row["stage"] == "timing")
                failed["status"] = "timeout"
                result = campaign.summarize(inventory, slots, rows, 1)["case"][comparator]
                assert not result["eligible"] and result["scheduled_samples"] == expected[count] and result["sample_statuses"]["timeout"] == 1
                assert result["speedup_vs_dar"]["open_query_ns"]["value"] is None
                rows.remove(failed)
                assert campaign.summarize(inventory, slots, rows, 1)["case"][comparator]["sample_statuses"]["not_run"] == 1
    with tempfile.TemporaryDirectory(prefix="selective-read-campaign-") as temporary:
        root = Path(temporary)
        fake = root / "reader"
        fake.write_text("#!" + sys.executable + "\n" + FAKE)
        fake.chmod(0o755)
        for mode in ("success", "initialization-timeout", "query-timeout", "cleanup-timeout", "reported-timeout", "truncated", "invalid-progress"):
            output = root / mode
            output.mkdir()
            payload = {"execution_mode": "reuse", "purpose": "timing", "test": mode}
            request = output / "request.json"
            request.write_text(json.dumps(payload))
            with (output / "stdout").open("w") as stdout, (output / "stderr").open("w") as stderr:
                result = supervise.launch([str(fake), str(request), str(output / "reader")], payload, output, stdout, stderr,
                                          query_seconds=.3, cleanup_seconds=.3)
            assert result["status"] == ("success" if mode == "success" else "operational_failure" if mode in ("truncated", "invalid-progress") else "timeout"), result
            assert result["process_cpu_ns"] > 0 and result["peak_rss_bytes"] > 0
            if mode == "query-timeout":
                assert len(result["completed_queries"]) == 1 and result["last_phase"] == "query"
            if mode == "success":
                assert len(result["completed_queries"]) == 10 and result["last_phase"] == "cleanup"
        for mode in ("crash", "malformed"):
            result = run.invoke(fake, {"execution_mode": "open", "purpose": "timing", "test": mode}, root / mode, supervised=True)
            assert result["status"] == "operational_failure" and (root / mode / "observation.json").is_file(), result
        for mode in ("success", "missing-query", "extra-query", "query-timeout"):
            payload = {"execution_mode": "reuse", "purpose": "timing", "comparison_revision": 4, "test": mode}
            if mode == "query-timeout":
                # The existing short-deadline check above covers the watchdog's
                # timeout path; exercise the second query in revision 4 too.
                output = root / ("reduced-" + mode)
                output.mkdir()
                request = output / "request.json"
                request.write_text(json.dumps(payload))
                with (output / "stdout").open("w") as stdout, (output / "stderr").open("w") as stderr:
                    result = supervise.launch([str(fake), str(request), str(output / "reader")], payload, output, stdout, stderr,
                                              query_seconds=.3, cleanup_seconds=.3)
                assert result["status"] == "timeout" and len(result["completed_queries"]) == 1
            else:
                result = run.invoke(fake, payload, root / ("reduced-" + mode), supervised=True)
                assert result["status"] == ("success" if mode == "success" else "operational_failure"), result
                if mode == "success":
                    assert len(result["queries"]) == 2 and result["supervision"]["last_phase"] == "cleanup"
        bad = {"status": "success"}
        try:
            campaign.validate(bad, {}, "delta-arrow-reader")
        except (KeyError, ValueError):
            pass
        else:
            raise AssertionError("malformed success accepted")
    print("passed: all 32 reader subsets, historical and five-sample schedules, two-query sessions, identity rejection, missing/failed slots and subprocess watchdogs")


if __name__ == "__main__":
    check()

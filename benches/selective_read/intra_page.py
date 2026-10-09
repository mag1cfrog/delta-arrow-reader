"""Compare ordinary and automatic partial-page reads using a retained query and oracle."""

import argparse
from collections import Counter
import json
from pathlib import Path
import statistics
import sys

import pyarrow as pa

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE / "runners"))
import network
import oracle
import storage
from run import comparison_identity, digest, invoke, save


def network_metrics(capture, path):
    if capture is None:
        return None
    records = [json.loads(line) for line in path.read_text().splitlines()]
    events = sorted(event for r in records for event in
                    ((r["started_ns"], 1), (r["ended_ns"], -1)))
    active = peak = 0
    for _, change in events:
        active += change
        peak = max(peak, active)
    assert active == 0
    identical = Counter((r["method"], r["object"], r["range"]) for r in records)
    return {"response_bytes": capture["response_bytes"], "requests": capture["requests"],
            "peak_active_requests": peak, "by_class": capture["by_class"],
            "incomplete_bodies": sum(r["incomplete_body"] for r in records),
            "http_statuses": dict(Counter(r["status"] for r in records)),
            "repeated_identical_requests": sum(n - 1 for n in identical.values()),
            "retries": None, "retry_note": "Repeated ranges may be separate reads; the proxy cannot identify SDK retries."}


def run(args, request, mode, purpose, repetition):
    name = f"{mode}-{purpose}-{repetition}"
    slot = args.output / name
    output = slot / "reader"
    certificate = args.output / f"{mode}-correctness.json"
    payload = request | {"purpose": purpose, "run_id": name, "campaign_id": "intra-page-networks",
                         "repetition": repetition, "correctness_file": str(certificate),
                         "validation_diagnostics": purpose == "validation"}
    env = storage.reader_environment(args.state)
    env.pop("RUST_LOG", None)
    env["DAR_INTRA_PAGE_READS"] = mode
    env["DAR_NETWORK_WARMUP"] = ("none" if args.no_warmup else
                                 "on" if args.network_warmup and mode == "auto" else "off")
    if purpose == "validation":
        env["RUST_LOG"] = ("delta_arrow_reader::diagnostics::intra_page=debug,"
                           "delta_arrow_reader::diagnostics::parquet_range_planning=debug,"
                           "delta_arrow_reader::diagnostics::network_warmup=debug")
    remote = args.local_table is None
    if remote:
        network.control(args.state, reset=True, traced=True)
    print(f"START {name}", flush=True)
    record = invoke(args.binary, payload, slot, reference=args.reference, env=env,
                    command_prefix=storage.reader_prefix(args.state), supervised=True, defer_validation=True)
    capture = network.finish(args.state, slot) if remote else None
    assert record["status"] == "success", record.get("failure_reason")
    resources = record["supervision"]
    if purpose == "validation":
        metadata = json.loads((args.reference / "reference.json").read_text())
        for field in oracle.IDENTITY_FIELDS:
            assert record["identity"][field] == metadata[field], f"reference identity mismatch: {field}"
        checks = []
        for query in record["queries"]:
            path = output / query["result"]
            with path.open("rb") as source, pa.ipc.open_stream(source) as batches:
                oracle.check_schema(batches.schema, metadata["projection"])
                rows = oracle.compare(batches, args.reference / "reference.parquet",
                                      metadata["projection"], metadata["output_rows"])
                assert not source.read(1)
            checks.append(record["identity"] | {"status": "passed", "output_rows": rows,
                          "oracle_sha256": digest(HERE / "oracle.py"),
                          "reference_sha256": metadata["reference_sha256"], "result_sha256": digest(path)})
        save(certificate, {"status": "passed", "checks": checks})
    duration = record["session_elapsed_ns"] if request["execution_mode"] == "reuse" else record["open_query_ns"]
    sample = {"mode": mode, "purpose": purpose, "repetition": repetition,
              "rows": record["queries"][0]["output_rows"],
              "seconds": duration / 1e9 if duration is not None else None,
              "initialization_seconds": record["initialization_ns"] / 1e9 if record["initialization_ns"] is not None else None,
              "query_seconds": [q["completion_ns"] / 1e9 if q["completion_ns"] is not None else None
                                for q in record["queries"]],
              "network": network_metrics(capture, slot / "network-requests.jsonl"),
              "process_cpu_ns": resources["process_cpu_ns"], "peak_rss_bytes": resources["peak_rss_bytes"],
              "record": str((output / "record.json").relative_to(args.output))}
    save(slot / "sample.json", sample)
    print(json.dumps(sample), flush=True)
    return sample


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ("binary", "request", "reference", "state", "output"):
        parser.add_argument("--" + name, type=Path, required=True)
    parser.add_argument("--local-table", type=Path)
    parser.add_argument("--execution-mode", choices=("open", "reuse"), default="open")
    parser.add_argument("--concurrent-queries", type=int, choices=(2, 4),
                        help="Run this many queries together through one initialized provider (requires reuse)")
    warmup = parser.add_mutually_exclusive_group()
    warmup.add_argument("--network-warmup", action="store_true",
                        help="Initialize the auto mode's network profile before querying")
    warmup.add_argument("--no-warmup", action="store_true",
                        help="Skip both metadata and network warmup, including in reuse mode")
    parser.add_argument("--samples", type=int, default=3, choices=range(1, 6))
    args = parser.parse_args()
    if args.concurrent_queries is not None and args.execution_mode != "reuse":
        parser.error("--concurrent-queries requires --execution-mode reuse")
    for name in ("binary", "request", "reference", "state", "output"):
        setattr(args, name, getattr(args, name).resolve())
    request = json.loads(args.request.read_text())
    assert request["comparison_revision"] == 6, "use the retained published campaign requests"
    request["execution_mode"] = args.execution_mode
    request.pop("concurrent_queries", None)
    if args.concurrent_queries is not None:
        request["concurrent_queries"] = args.concurrent_queries
    metadata = json.loads((args.reference / "reference.json").read_text())
    assert comparison_identity(metadata) == comparison_identity(request)
    for field in ("fixture_manifest_sha256", "case_id", "snapshot_version"):
        assert metadata[field] == request[field], f"reference identity mismatch: {field}"
    assert metadata["canonical_sql"] == request["canonical_sql"]
    assert digest(args.reference / "reference.parquet") == metadata["reference_sha256"]
    if args.local_table:
        request["table_uri"] = args.local_table.resolve().as_uri()
    else:
        assert request["table_uri"].startswith("s3://") and network.config(args.state) is not None
    args.output.mkdir()
    profile = None if args.local_table else network.config(args.state)
    save(args.output / "experiment.json", {
        "source_request_sha256": digest(args.request), "request": request,
        "build": json.loads(args.binary.with_name("build.json").read_text()),
        "reference": metadata, "network": profile,
        "harness_sha256": digest(Path(__file__)), "samples_per_mode": args.samples,
        "network_warmup": args.network_warmup,
        "no_warmup": args.no_warmup,
        "concurrent_queries": args.concurrent_queries,
        "modes": {"off": "ordinary reader", "auto": "public opt-in with transport cost gate"},
        "session": (f"table open, then {args.concurrent_queries} concurrent queries sharing the provider"
                    if args.concurrent_queries else "one query including table open" if args.execution_mode == "open"
                    else "table open, then two sequential queries sharing the provider"),
        "order": [list(("off", "auto") if n % 2 == 0 else ("auto", "off")) for n in range(args.samples)],
        "cache": "fresh processes; reused OS and MinIO caches; validation before timing",
        "cpu_affinity": storage.state(args.state)["cpus"]["reader"],
        "process_memory_bytes": 8 * 1024**3,
        "resource_boundary": "whole process including startup and cleanup; query time excludes them",
        "preparation": "build, data preparation and reference hashing finish before samples"})
    with storage.exclusive(args.state):
        storage.verify_server(args.state)
        for mode in ("off", "auto"):
            run(args, request, mode, "validation", 0)
        samples = []
        for repetition in range(args.samples):
            for mode in (("off", "auto") if repetition % 2 == 0 else ("auto", "off")):
                samples.append(run(args, request, mode, "timing", repetition))
        summary = {"samples": samples, "results": {}}
        for mode in ("off", "auto"):
            times = [s["seconds"] for s in samples if s["mode"] == mode]
            summary["results"][mode] = {"median_seconds": statistics.median(times),
                                         "min_seconds": min(times), "max_seconds": max(times),
                                         "query_medians_seconds": [statistics.median(s["query_seconds"][index]
                                             for s in samples if s["mode"] == mode)
                                             for index in range(len(samples[0]["query_seconds"]))]}
        save(args.output / "summary.json", summary)
        print(json.dumps(summary["results"]), flush=True)


if __name__ == "__main__":
    main()

"""Probe and generate Q2/Q4 Delta inputs with the existing pinned Parquet writer."""

import argparse
import json
from pathlib import Path
import resource
import shutil
import subprocess
import sys
import time

import duckdb
import pyarrow as pa

from oracle import ORIGINAL, bounded, digest_file, inside, require
import production_shapes as shapes

GIB = 1024**3
SPILL = 16 * GIB


def save(path, value):
    with path.open("x") as stream:
        json.dump(value, stream, indent=2)
        stream.write("\n")


def inputs(source_root, plan_path, name):
    plan = json.loads(plan_path.read_text())
    require(plan["status"] == "logical_geometry_checked" and plan["publication_ready"] is False
            and plan["native_campaign_ready"] is False, "expected an untimed production layout plan")
    require(plan["contract_sha256"] == digest_file(shapes.CONTRACT)
            and plan["planner_sha256"] == digest_file(Path(shapes.__file__)), "layout definition changed")
    require(plan["source_manifest_sha256"] == digest_file(source_root / "manifest.json"), "source manifest changed")
    manifest = json.loads((source_root / "manifest.json").read_text())
    require(manifest["status"] == "complete" and manifest["protocol"] == "selective-read-v1"
            and manifest["generator"]["tpchgen"] == "3.0.0"
            and manifest["generator"]["tpchgen_git"] == "4f6bf4c5ab40511c8fdef5888fc8d022e5e546d7"
            and manifest["generator"]["lockfile_sha256"] == digest_file(source_root / "generator-Cargo.lock"),
            "source does not identify the pinned completed generator")
    source = next(s for s in manifest["sources"] if s["scale_factor"] == 10)
    paths = []
    for file in source["files"]:
        path = inside(source_root, str(Path(source["path"]) / file["path"]))
        require(path.stat().st_size == file["bytes"] and digest_file(path) == file["sha256"], "source file changed")
        paths.append(path)
    require(len(set(paths)) == len(paths) == source["file_count"]
            and set(paths) == set(inside(source_root, source["path"]).glob("*.parquet")), "source inventory changed")
    shape = plan["shapes"][name]
    definition = shapes.definitions(plan["file_target_mib"], plan["data_page_rows"])[name]
    require(all(shape[k] == v for k, v in definition.items()), "production SQL or shape changed")
    geometry_path = plan_path.with_name(name + "-files.json")
    require(digest_file(geometry_path) == shape["file_geometry_sha256"], "logical file geometry changed")
    files = json.loads(geometry_path.read_text())
    require(len(files) == shape["files"] and sum(f["rows"] for f in files) == source["rows"]
            and sum(f["matching_rows"] for f in files) == shape["output_rows"], "incomplete file geometry")
    expected = [(stripe, index) for stripe in range(shape["stripes"])
                for index in range(shape["files"] // shape["stripes"] + int(stripe < shape["files"] % shape["stripes"]))]
    require([(f["stripe"], f["file_index"]) for f in files] == expected, "wrong file membership order")
    require(all(type(f["rows"]) is int and 0 < f["rows"] <= 1048576
                and type(f["candidate"]) is bool for f in files), "invalid file boundaries")
    return plan, source, paths, shape, files


def probe_selection(files, stripes):
    selected = {i for i, file in enumerate(files) if file["candidate"]}
    for stripe in range(stripes):
        ordinary = [i for i, file in enumerate(files) if file["stripe"] == stripe and not file["candidate"]]
        require(ordinary, "stripe has no ordinary interior file")
        selected.add(ordinary[len(ordinary) // 2])
    return sorted(selected)


def capacity(probe, files, layout, source_bytes, probe_bytes):
    """Use the largest measured bytes/row per stripe, plus 25% write headroom."""
    table = next(t for t in probe["writer"]["tables"] if t["layout"] == layout)
    settings = table["writer"]
    page_ceiling = ((settings["data_page_rows"] + settings["write_batch_rows"] - 1)
                    // settings["write_batch_rows"] * settings["write_batch_rows"])
    rates = {}
    for file, evidence in zip(table["files"], table["file_evidence"], strict=True):
        planned = files[evidence["source_file_ordinal"]]
        require(evidence["planned"] == planned and file["rows"] == planned["rows"], "probe geometry changed")
        require(evidence["full_value_roundtrip"] == "passed" and evidence["maximum_page_rows"] <= page_ceiling, "probe checks failed")
        # Sidecars plus bounded allowance for both manifests and Delta JSON actions.
        metadata = (file["geometry"]["bytes"] + 2 * len(json.dumps(file, indent=2))
                    + 2 * len(json.dumps(evidence, indent=2)) + len(json.dumps(file["delta_stats"])) + 1024)
        stripe = planned["stripe"]
        values = rates.setdefault(stripe, {"parquet": (0, 1), "metadata": (0, 1)})
        for key, value in (("parquet", file["bytes"]), ("metadata", metadata)):
            if value * values[key][1] > values[key][0] * file["rows"]:
                values[key] = value, file["rows"]
    require(set(rates) == {f["stripe"] for f in files}, "probe misses a stripe")
    estimates = {"parquet": 0, "metadata": 0}
    for stripe, values in rates.items():
        rows = sum(f["rows"] for f in files if f["stripe"] == stripe)
        for key, (numerator, denominator) in values.items():
            estimates[key] += (rows * numerator * 5 + denominator * 4 - 1) // (denominator * 4)
    output = estimates["parquet"] + estimates["metadata"] + 64 * 1024**2
    # The query returns hundreds of rows. References/exports are not full-table copies.
    fixed = source_bytes * 2 + probe_bytes + 256 * 1024**2
    preparation = fixed + output + SPILL
    validation = fixed + output + estimates["parquet"] + estimates["metadata"]
    return {"basis": "max measured bytes/row in each stripe; 25% headroom; actual writes remain capped",
            "layout": layout, "estimated_parquet_bytes": estimates["parquet"],
            "estimated_metadata_bytes": estimates["metadata"], "native_output_limit_bytes": output,
            "preparation_peak_bytes": preparation, "validation_with_minio_peak_bytes": validation,
            "estimated_phase_peak_bytes": max(preparation, validation), "probe_bytes": probe_bytes,
            "source_bytes_counted_twice": source_bytes * 2, "sort_spill_limit_bytes": SPILL,
            "scope": "one generated layout, retained probe/source and later MinIO/selected-output validation; other layouts/DV copies need separate budgets"}


def tree_bytes(root):
    return sum(path.stat().st_size for path in root.rglob("*") if path.is_file())


def generate(args):
    source_root, plan_path, output = args.source.resolve(), args.plan.resolve(), args.output.resolve()
    require(not output.exists(), "output directory already exists")
    require(args.elapsed_limit_seconds > 0 and args.disk_limit_mib > 0, "positive resource limits required")
    limit = args.disk_limit_mib * 1024**2
    require(limit <= 192 * GIB, "this generator is limited to the approved 192 GiB allocation")
    plan, source, paths, shape, files = inputs(source_root, plan_path, args.shape)
    require(duckdb.__version__ == "1.5.5" and pa.__version__ == "25.0.1", "use the pinned oracle Python environment")
    selected = probe_selection(files, shape["stripes"]) if args.command == "probe" else list(range(len(files)))
    if args.command == "probe":
        require(args.probe is None and args.layout is None, "probe writes both sample layouts")
        sample_rows = sum(files[i]["rows"] for i in selected)
        # Two layouts, each allowing 16 bytes/cell for values and metadata with >=2048-row pages.
        # This is a hard write budget, not an assumption about the achieved compression ratio.
        output_limit = sample_rows * shape["stored_columns"] * 32 + 256 * 1024**2
        phase = {"estimated_phase_peak_bytes": source["bytes"] * 2 + SPILL + output_limit + GIB,
                 "native_output_limit_bytes": output_limit, "scope": "bounded writer probe only"}
    else:
        require(args.probe is not None and args.layout in ("localized", "scattered"), "generate requires --probe and --layout")
        probe_root = args.probe.resolve()
        probe = json.loads((probe_root / "manifest.json").read_text())
        require(probe["mode"] == "probe" and probe["status"] == "complete"
                and probe["shape"] == args.shape and probe["plan_sha256"] == digest_file(plan_path)
                and probe["driver_sha256"] == digest_file(Path(__file__))
                and probe["writer"]["generator"]["executable_sha256"] == digest_file(args.writer), "stale or incompatible capacity probe")
        require(probe["writer_result_sha256"] == digest_file(probe_root / "writer-result.json"), "probe writer result changed")
        require(probe["writer"] == json.loads((probe_root / "writer-result.json").read_text()), "probe manifest changed")
        for table in probe["writer"]["tables"]:
            for file in table["files"]:
                for item in (file, file["geometry"]):
                    path = inside(probe_root, str(Path(table["path"]) / item["path"]))
                    require(path.stat().st_size == item["bytes"] and digest_file(path) == item["sha256"], "probe object changed")
        phase = capacity(probe, files, args.layout, source["bytes"], tree_bytes(probe_root))
    require(phase["estimated_phase_peak_bytes"] <= limit, "production phase exceeds disk allowance: " + json.dumps(phase))
    output.parent.mkdir(parents=True, exist_ok=True)
    # Retained inputs count against the phase allocation but already occupy disk.
    phase["additional_disk_bytes"] = phase["estimated_phase_peak_bytes"] - source["bytes"] - phase.get("probe_bytes", 0)
    require(shutil.disk_usage(output.parent).free >= phase["additional_disk_bytes"] + GIB, "insufficient free disk")
    output.mkdir()
    save(output / "capacity.json", phase | {"disk_limit_bytes": limit})
    request = {"format": "selective-read-production-write-v1", "mode": args.command, "shape": args.shape,
               "file_target_mib": shape["file_target_mib"], "data_page_rows": shape["data_page_rows"],
               "layout": args.layout, "contract_sha256": digest_file(shapes.CONTRACT),
               "driver_sha256": digest_file(Path(__file__)), "files": files, "selected": selected,
               "projection": shape["projection"], "output_limit_bytes": phase["native_output_limit_bytes"]}
    save(output / "writer-request.json", request)
    started = time.monotonic()
    with bounded(output, {"memory_bytes": 8 * GIB, "disk_bytes": phase["additional_disk_bytes"],
                          "elapsed_seconds": args.elapsed_limit_seconds}):
        copied_source = output / source["path"]
        copied_source.mkdir(parents=True)
        for path in paths:
            shutil.copyfile(path, copied_source / path.name)
        shutil.copyfile(source_root / "generator-Cargo.lock", output / "generator-Cargo.lock")
        with (output / "writer.log").open("wb") as log:
            child = subprocess.Popen([str(args.writer.resolve()), "production-write", str(output / "writer-request.json"),
                                      str(output), str(args.elapsed_limit_seconds)], stdin=subprocess.PIPE, stdout=log, stderr=log)
            try:
                with duckdb.connect(config={"memory_limit": "2GiB", "threads": "4", "temp_directory": str(output / "spill"),
                                            "max_temp_directory_size": "16GiB"}) as connection:
                    connection.read_parquet([str(copied_source / path.name) for path in paths]).create_view("source")
                    query = "SELECT " + ", ".join(ORIGINAL) + " FROM source ORDER BY l_orderkey % " + str(shape["stripes"]) + ", l_shipdate, l_shipmode, l_linenumber, l_orderkey"
                    batches = connection.execute(query).to_arrow_reader(8192)
                    with pa.ipc.new_stream(child.stdin, batches.schema) as writer:
                        for batch in batches:
                            writer.write_batch(batch)
                child.stdin.close()
                require(child.wait(timeout=60) == 0, "native writer failed; see writer.log")
            finally:
                if child.poll() is None:
                    child.kill()
                    child.wait()
        require(digest_file(source_root / "manifest.json") == plan["source_manifest_sha256"], "source manifest changed during writing")
        for file in source["files"]:
            require(digest_file(copied_source / file["path"]) == file["sha256"], "copied source changed")
        result = json.loads((output / "writer-result.json").read_text())
        require(result["status"] == "complete" and result["request_sha256"] == digest_file(output / "writer-request.json"), "incomplete native write")
        require(result["generator"]["executable_sha256"] == digest_file(args.writer), "writer binary changed")
        manifest = {"protocol": "selective-read-production-fixtures-v1", "status": "complete", "mode": args.command,
                    "shape": args.shape, "layout": args.layout, "plan_sha256": digest_file(plan_path),
                    "contract_sha256": request["contract_sha256"], "driver_sha256": request["driver_sha256"],
                    "source_parent_manifest_sha256": plan["source_manifest_sha256"], "sources": [source],
                    "shape_definition": shape, "writer": result, "writer_result_sha256": digest_file(output / "writer-result.json"),
                    "source_sort": {"engine": "duckdb", "version": duckdb.__version__, "pyarrow": pa.__version__,
                                    "python": sys.version, "threads": 4, "memory_limit_bytes": 2 * GIB,
                                    "spill_limit_bytes": SPILL, "ipc_batch_rows": 8192},
                    "producer_peak_rss_bytes": resource.getrusage(resource.RUSAGE_SELF).ru_maxrss * 1024,
                    "capacity": phase, "preparation_seconds": time.monotonic() - started,
                    "native_campaign_ready": False, "publication_ready": False}
        save(output / "manifest.json", manifest)
    print(json.dumps({"status": "complete", "mode": args.command, "shape": args.shape,
                      "tables": [{k: t[k] for k in ("id", "rows", "bytes", "file_count")} for t in result["tables"]],
                      "preparation_seconds": manifest["preparation_seconds"]}), flush=True)
    return manifest


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("probe", "generate"))
    parser.add_argument("--source", type=Path, required=True)
    parser.add_argument("--plan", type=Path, required=True)
    parser.add_argument("--shape", choices=("q2", "q4"), required=True)
    parser.add_argument("--writer", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--probe", type=Path)
    parser.add_argument("--layout", choices=("localized", "scattered"))
    parser.add_argument("--disk-limit-mib", type=int, default=196608)
    parser.add_argument("--elapsed-limit-seconds", type=int, default=3600)
    generate(parser.parse_args())

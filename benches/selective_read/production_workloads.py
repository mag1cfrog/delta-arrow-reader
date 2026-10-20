"""Bind Q2/Q4 fixtures to the existing oracle, native runners and campaign."""

from datetime import date
import json
from pathlib import Path
import platform
import sys

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

import oracle
import native
import production_shapes as shapes

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE / "runners"))
from run import digest, fixture_tables, save
from oracle import require

PREDICATES = [("l_shipdate", "=", date(1995, 3, 15)), ("l_shipmode", "=", "AIR"),
              ("l_linenumber", "IN", frozenset([1]))]
_SOURCE_SCANS = {}
_PHYSICAL_SCANS = {}
_EXTRA_KEY_SCANS = {}


def load(path, value):
    return native.call("workload", {"action": "load", "value": value, "harness": str(HERE)})


def binding(value, fixtures, case):
    return native.call("workload", {"action": "binding", "value": value, "fixtures": str(fixtures),
                                    "case": case, "harness": str(HERE)})


def masks(batch):
    result = [pc.equal(batch.column("l_shipdate"), date(1995, 3, 15))]
    result.append(pc.and_(result[-1], pc.equal(batch.column("l_shipmode"), "AIR")))
    result.append(pc.and_(result[-1], pc.is_in(batch.column("l_linenumber"), value_set=pa.array([1], type=pa.int32()))))
    return result


def check_metrics(batch, count):
    def remainder(values, divisor):
        return pc.subtract(values, pc.multiply(pc.divide(values, divisor), divisor))
    order, part, supplier, line = (pc.cast(batch.column(c), pa.int64()) for c in (
        "l_orderkey", "l_partkey", "l_suppkey", "l_linenumber"))
    for j in range(count):
        name = f"metric_{j:03}"
        require(batch.schema.field(name).type == pa.int64(), "wrong stored metric type")
        null = pc.equal(remainder(pc.add(pc.add(order, line), j), 17), 0)
        expected = pc.if_else(null, pa.scalar(None, pa.int64()),
                              remainder(pc.add(pc.add(part, pc.multiply(supplier, j + 1)), line), 1024))
        require(batch.column(name).equals(expected), "wrong stored metric values/nulls: " + name)


def extra_keys(fixtures, manifest):
    """Independently recompute the shared DV union from unfiltered base files."""
    saved = manifest.get("production_dv", {}).get("extra_logical_keys")
    require(saved is not None, "missing shared production DV union")
    extras, minimum_match = set(), None
    bases = [t for t in fixture_tables(manifest) if not t["deletion_vectors"]]
    require(sorted(t["id"] for t in bases) == manifest["production_dv"]["base_case_ids"],
            "shared DV union inventory changed")
    paired_shapes = {shapes.cases()[t["id"]]["shape"] for t in bases}
    layout = manifest["production_dv"].get("layout")
    require(layout in (None, "localized", "scattered"), "unknown pairing layout")
    require(paired_shapes and {t["id"] for t in bases} ==
            {c for c, v in shapes.cases().items() if not v["deletion_vectors"] and v["shape"] in paired_shapes
             and (layout is None or v["layout"] == layout)},
            "shared DV union inventory does not match requested layout scope")
    identities = tuple(oracle.file_identity(oracle.inside(fixtures, str(Path(t["path"]) / f["path"])))
                       for t in bases for f in t["files"])
    if identities in _EXTRA_KEY_SCANS:
        extras = _EXTRA_KEY_SCANS[identities]
        require(sorted(extras) == [tuple(k) for k in saved], "shared production deletion union changed")
        return extras
    for table in bases:
        for item in table["files"]:
            minimum = None
            path = oracle.inside(fixtures, str(Path(table["path"]) / item["path"]))
            for batch in pq.ParquetFile(path).iter_batches(columns=shapes.INPUT_COLUMNS):
                match = masks(batch)[-1].to_pylist()
                for order, line, selected in zip(batch.column("l_orderkey").to_pylist(), batch.column("l_linenumber").to_pylist(), match):
                    key = order, line
                    if selected:
                        minimum_match = min(minimum_match, key) if minimum_match else key
                    else:
                        minimum = min(minimum, key) if minimum else key
            require(minimum is not None, "DV base file has no nonmatching row")
            extras.add(minimum)
    require(minimum_match is not None, "DV bases have no matching rows")
    extras.add(minimum_match)
    require(sorted(extras) == [tuple(k) for k in saved], "shared production deletion union changed")
    require(identities == tuple(oracle.file_identity(oracle.inside(fixtures, str(Path(t["path"]) / f["path"])))
                                for t in bases for f in t["files"]), "DV union inputs changed during scan")
    extras = frozenset(extras)
    _EXTRA_KEY_SCANS[identities] = extras
    return extras


def scan_source(fixtures, source):
    paths = [oracle.inside(fixtures, str(Path(source["path"]) / item["path"])) for item in source["files"]]
    key = tuple((oracle.file_identity(path), item["rows"], item["sha256"])
                for path, item in zip(paths, source["files"], strict=True))
    if key in _SOURCE_SCANS:
        counts, records = _SOURCE_SCANS[key]
        require(counts[0] == source["rows"], "source row count changed")
        return counts, records, True
    counts, records, previous = [0, 0, 0, 0], [], None
    print("production oracle: scan original source", flush=True)
    for path, item in zip(paths, source["files"], strict=True):
        parquet = pq.ParquetFile(path)
        oracle.check_schema(parquet.schema_arrow, oracle.ORIGINAL)
        file_rows = 0
        for batch in parquet.iter_batches(batch_size=65536):
            require(all(c.null_count == 0 for c in batch.columns), "null in original source")
            orders, lines = (batch.column(c) for c in oracle.KEYS)
            ordered = pc.or_(pc.greater(orders.slice(1), orders.slice(0, len(orders) - 1)),
                             pc.and_(pc.equal(orders.slice(1), orders.slice(0, len(orders) - 1)),
                                     pc.greater(lines.slice(1), lines.slice(0, len(lines) - 1))))
            first = orders[0].as_py(), lines[0].as_py()
            require(pc.all(ordered).as_py() is not False and (previous is None or previous < first), "source keys duplicated or unordered")
            previous = orders[-1].as_py(), lines[-1].as_py()
            selected = masks(batch)
            counts[0] += batch.num_rows
            file_rows += batch.num_rows
            for i, mask in enumerate(selected, 1):
                counts[i] += pc.sum(pc.cast(mask, pa.int64())).as_py()
            for record in batch.filter(selected[-1]).to_pylist():
                records.append(oracle.record(record, oracle.ORIGINAL + oracle.PAYLOADS, derive_payloads=True))
        require(file_rows == item["rows"], "source file row count changed")
    require(counts[0] == source["rows"] and all(a > b > 0 for a, b in zip(counts, counts[1:])), "source predicate geometry changed")
    require(300 <= counts[-1] <= 2000, "production output exceeds the declared hundreds-of-rows shape")
    require(key == tuple((oracle.file_identity(path), item["rows"], item["sha256"])
                         for path, item in zip(paths, source["files"], strict=True)), "source changed during scan")
    _SOURCE_SCANS[key] = tuple(counts), tuple(records)
    return tuple(counts), tuple(records), False


def prepare_reference(fixtures, case, output, workload, quota):
    import large_workloads
    fixtures, output = Path(fixtures).resolve(), Path(output)
    frozen = large_workloads.load(workload)
    row = binding(frozen, fixtures, case)
    manifest = oracle.load_json(fixtures / "manifest.json")
    table = next(t for t in fixture_tables(manifest) if t["id"] == case)
    source = next(s for s in manifest["sources"] if s["scale_factor"] == 10)
    projection = row["projection"]
    verified = oracle.objects(fixtures, table, source)
    extras = extra_keys(fixtures, manifest) if table["deletion_vectors"] else set()
    counts, records, source_reused = scan_source(fixtures, source)
    expected, projected_bytes, deleted_matches = [], [0], 0
    for record in records:
        if table["deletion_vectors"] and (oracle.deleted(record) or tuple(record[k] for k in oracle.KEYS) in extras):
            deleted_matches += 1
        else:
            expected.append(oracle.record(record, projection, logical_bytes=projected_bytes))
    output.mkdir()
    reference = output / "reference.parquet"
    oracle.write_reference(reference, expected, projection, quota // 4)
    candidates, matching, actual_rows, actual_matches, deleted_rows = [], [], 0, 0, 0
    metric_count = shapes.SHAPES[row["shape"]]["extra_numeric_columns"]
    stored = [*oracle.ORIGINAL, *oracle.PAYLOADS, *[f"metric_{j:03}" for j in range(metric_count)]]
    saved_keys = [tuple(k) for f in table["files"] for k in f.get("deletion_vector", {}).get("logical_ids", [])]
    require(len(saved_keys) == len(set(saved_keys)), "duplicate deleted logical key")
    scans = {"full_column_scans": 0, "reused_full_column_scans": 0, "source_scan_reused": source_reused,
             "scan_batch_rows": 65536,
             "reuse_guard": "unchanged local device/inode/size/mtime_ns/ctime_ns"}

    def fixture_batches():
        nonlocal actual_rows, actual_matches, deleted_rows
        print("production oracle: scan physical fixture and stored metrics", flush=True)
        for item, evidence in zip(table["files"], table["file_evidence"], strict=True):
            if oracle.candidate(item["delta_stats"], PREDICATES):
                candidates.append(item["path"])
            path = oracle.inside(fixtures, str(Path(table["path"]) / item["path"]))
            identity = oracle.file_identity(path)
            cached = _PHYSICAL_SCANS.get((identity, metric_count))
            scans["reused_full_column_scans" if cached else "full_column_scans"] += 1
            parquet = pq.ParquetFile(path)
            require(parquet.schema_arrow.names == stored, "stored schema differs")
            oracle.check_schema(pa.schema([parquet.schema_arrow.field(c) for c in oracle.ORIGINAL + oracle.PAYLOADS]), oracle.ORIGINAL + oracle.PAYLOADS)
            dv = item.get("deletion_vector", {})
            by_ordinal = dict(zip(dv.get("physical_ordinals", []), map(tuple, dv.get("logical_ids", [])), strict=True))
            require(not table["deletion_vectors"] or bool(by_ordinal), "production file has an empty DV")
            ordinal, matched = 0, False
            physical_matches = list(cached["ordinals"]) if cached else []
            physical_batches = list(cached["batches"]) if cached else []
            batches = parquet.iter_batches(batch_size=scans["scan_batch_rows"], columns=list(oracle.KEYS) if cached else None)
            if cached and not table["deletion_vectors"]:
                ordinal, batches = cached["rows"], []
            for batch in batches:
                if not cached:
                    require(all(batch.column(c).null_count == 0 for c in oracle.ORIGINAL), "null in original fixture field")
                    check_metrics(batch, metric_count)
                    mask = masks(batch)[-1]
                    physical_matches.extend(ordinal + i for i in pc.indices_nonzero(mask).to_pylist())
                    selected = batch.filter(mask)
                    if selected.num_rows:
                        physical_batches.append(selected.select(oracle.ORIGINAL + oracle.PAYLOADS))
                if table["deletion_vectors"]:
                    live = []
                    for i, (order, line) in enumerate(zip(batch.column("l_orderkey").to_pylist(), batch.column("l_linenumber").to_pylist())):
                        key = order, line
                        deleted = oracle.deleted({"l_orderkey": order, "l_linenumber": line}) or key in extras
                        require((ordinal + i in by_ordinal) == deleted
                                and (not deleted or by_ordinal[ordinal + i] == key), "wrong physical deletion ordinal/key")
                        deleted_rows += deleted
                        if not cached:
                            live.append(not deleted)
                    if not cached:
                        mask = pc.and_(mask, pa.array(live))
                if not cached:
                    if table["deletion_vectors"]:
                        selected = batch.filter(mask)
                    if selected.num_rows:
                        matched = True
                        yield selected.select(projection)
                ordinal += batch.num_rows
            if cached:
                deleted_keys = set(by_ordinal.values())
                for batch in physical_batches:
                    selected = batch
                    if table["deletion_vectors"]:
                        live = [key not in deleted_keys for key in zip(batch.column("l_orderkey").to_pylist(),
                                                                      batch.column("l_linenumber").to_pylist())]
                        selected = batch.filter(pa.array(live))
                    if selected.num_rows:
                        matched = True
                        yield selected.select(projection)
            require(ordinal == item["rows"] and physical_matches == evidence["matching_ordinals"], "fixture rows/match geometry changed")
            require(oracle.file_identity(path) == identity, "fixture changed during scan")
            _PHYSICAL_SCANS[identity, metric_count] = {"rows": ordinal, "ordinals": tuple(physical_matches),
                                                      "batches": tuple(physical_batches)}
            actual_rows += ordinal
            actual_matches += len(physical_matches)
            if matched:
                matching.append(item["path"])
    oracle.compare(fixture_batches(), reference, projection, len(expected), quota // 4)
    require(actual_rows == table["rows"] and actual_matches == counts[-1]
            and deleted_rows == len(saved_keys) and set(matching) <= set(candidates), "fixture/reference populations disagree")
    if table["deletion_vectors"]:
        summary = table["deletion_summary"]
        require(deleted_matches > 0 and len(expected) > 0 and 0 < len(candidates) < table["file_count"]
                and summary["physical_rows"] == actual_rows and summary["deleted_rows"] == deleted_rows
                and summary["live_rows"] == actual_rows - deleted_rows and summary["dv_files"] == table["file_count"], "invalid DV coverage/deletions")
    for item in verified:
        oracle.verify_object(fixtures, item)
    metadata = {"format": "selective-read-reference-v2", "status": "complete", **large_workloads.identity(workload),
                "oracle_sha256": digest(Path(oracle.__file__)), "production_oracle_sha256": digest(Path(__file__)),
                "oracle_dependencies_sha256": digest(oracle.REQUIREMENTS), "python": platform.python_version(),
                "pyarrow": pa.__version__, "duckdb_sort": oracle.duckdb.__version__,
                "reference_storage": {"format": "parquet", "compression": "zstd", "row_group_rows": oracle.REFERENCE_ROWS},
                "sort_memory_bytes": oracle.SORT_MEMORY_BYTES, "sort_threads": 1,
                "fixture_manifest_sha256": digest(fixtures / "manifest.json"), "generator_protocol_sha256": manifest["contract_sha256"],
                "case_id": case, "snapshot_version": table["snapshot_version"], "profile": "production-" + manifest["mode"],
                "canonical_sql": row["canonical_sql"], "canonical_sql_sha256": row["canonical_sql_sha256"],
                "in_literals": [1], "projection": projection, "limit": None,
                "source_rows": counts[0], "physical_rows": actual_rows, "qualifying_rows": len(expected),
                "physical_qualifying_rows": counts[-1], "deleted_qualifying_rows": deleted_matches,
                "deleted_rows": deleted_rows, "live_rows": actual_rows - deleted_rows, "output_rows": len(expected),
                "predicate_step_population": "original source rows before deletions", "predicate_stage_rows": counts,
                "active_files": table["file_count"], "candidate_files": candidates, "matching_files": matching,
                "within_file_geometry": {"matching_groups": sum(len(e["matching_groups"]) for e in table["file_evidence"]),
                    "matching_projected_pages": sum(g["matching_output_pages"] for e in table["file_evidence"] for g in e["matching_groups"]),
                    "scope": "physical geometry before deletions, not reader decode counters"},
                "derived_value_checks": {"payloads": "all qualifying output rows, independent SHA256",
                    "metrics": "all stored rows, independent Arrow arithmetic"},
                "physical_validation": scans,
                "verified_objects": verified, "reference_sha256": digest(reference),
                "workload_manifest": str(Path(workload).resolve()), "oracle_limits": frozen["oracle_limits"],
                "native_expression_sha256": row["native_expression_sha256"], "projected_logical_bytes": projected_bytes[0]}
    save(output / "reference.json", metadata)
    return metadata

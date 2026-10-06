"""Bind Q2/Q4 fixtures to the existing oracle, native runners and campaign."""

import argparse
from datetime import date
import json
from pathlib import Path
import platform
import sys

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

import oracle
import production_shapes as shapes

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE / "runners"))
from run import PRODUCTION, PROTOCOL, SAMPLING, SPARK_MATRIX, digest, fixture_tables, reader_roster, save
from oracle import require

READERS = ["delta-arrow-reader", "delta-rs", "duckdb", "polars", "daft"]
SOURCES = (Path(__file__), HERE / "oracle.py", HERE / "oracle-requirements.txt",
           HERE / "production_shapes.py", HERE / "large_workloads.py", HERE / "matrix.py",
           HERE / "runners/run.py")
PREDICATES = [("l_shipdate", "=", date(1995, 3, 15)), ("l_shipmode", "=", "AIR"),
              ("l_linenumber", "IN", frozenset([1]))]
_SOURCE_SCANS = {}
_PHYSICAL_SCANS = {}
_EXTRA_KEY_SCANS = {}


def query_fields(case, manifest):
    definition = shapes.cases()[case]
    declared = (manifest["shape_definitions"][definition["shape"]] if "shape_definitions" in manifest
                else manifest["shape_definition"])
    table = next(t for t in fixture_tables(manifest) if t["id"] == case)
    if "data_page_bytes" not in declared:
        require(table["writer"].get("data_page_bytes") == 1048576,
                "legacy shape requires its recorded 1 MiB page target")
        declared = dict(declared, data_page_bytes=table["writer"]["data_page_bytes"])
    shape = shapes.definitions(declared["file_target_mib"], declared["data_page_rows"],
                               declared["data_page_bytes"], declared["write_batch_rows"])[definition["shape"]]
    require(all(declared[k] == v for k, v in shape.items()), "production shape definition changed")
    require(manifest["protocol"] in ("selective-read-production-fixtures-v1", "selective-read-production-pairs-v1")
            and manifest["contract_sha256"] == digest(shapes.CONTRACT)
            and manifest["mode"] in ("probe", "generate"), "unknown production fixture/definition")
    source = next(s for s in manifest["sources"] if s["scale_factor"] == 10)
    require(table["deletion_vectors"] is definition["deletion_vectors"]
            and type(table["snapshot_version"]) is int
            and table["snapshot_version"] == int(definition["deletion_vectors"])
            and table["layout"] == definition["layout"] and table["scale_factor"] == 10, "wrong production snapshot")
    columns = [f["name"] for f in table["schema"]["fields"]]
    stored = [*oracle.ORIGINAL, *oracle.PAYLOADS, *[f"metric_{j:03}" for j in range(shape["extra_numeric_columns"])]]
    require(columns == stored and len(columns) == shape["stored_columns"], "wrong stored production columns")
    require(table["file_count"] == len(table["files"]) == len(table["file_evidence"])
            and table["rows"] == sum(f["rows"] for f in table["files"])
            and table["bytes"] == sum(f["bytes"] for f in table["files"]), "wrong physical inventory")
    require(table["file_target_mib"] == shape["file_target_mib"]
            and all(table["writer"][k] == shape[k] for k in
                    ("row_group_rows", "data_page_rows", "data_page_bytes", "write_batch_rows", "dictionary")),
            "production page/group settings changed")
    if manifest["mode"] == "generate":
        require(table["file_count"] == shape["files"] and table["rows"] == source["rows"], "incomplete full production fixture")
    else:
        require(0 < table["file_count"] < shape["files"] and 0 < table["rows"] < source["rows"], "probe must retain its reduced geometry")
    if definition["deletion_vectors"]:
        base = next(t for t in fixture_tables(manifest) if t["id"] == case.removesuffix(".dv"))
        files = []
        for f in table["files"]:
            f = dict(f)
            if f.pop("deletion_vector", None):
                f["delta_stats"] = {k: v for k, v in f["delta_stats"].items() if k != "tightBounds"}
            files.append(f)
        require(files == base["files"] and table["schema"] == base["schema"]
                and table["file_evidence"] == base["file_evidence"], "DV pair changed Parquet data or geometry")
    sql = shape["canonical_sql"]
    return {"case_id": case, "role": "production", "query_case_id": case, "fixture_id": case,
            "fixture_path": table["path"], "snapshot_version": table["snapshot_version"],
            **definition, "scale_factor": 10, "source_rows": source["rows"], "physical_rows": table["rows"],
            "fixture_mode": manifest["mode"], "in_literals": [1], "projection": shape["projection"],
            "projection_columns": len(shape["projection"]), "predicate": "production-eq2-in1",
            "predicate_columns": [p[0] for p in PREDICATES], "predicate_only_columns": [], "limit": None,
            "canonical_sql": sql, "canonical_sql_sha256": oracle.digest_bytes(sql.encode()),
            "writer": table["writer"], "source_parent_manifest_sha256": manifest["source_parent_manifest_sha256"],
            "geometry": {"files": table["file_count"], "stored_columns": len(columns), "physical_bytes": table["bytes"],
                         "file_bytes": [f["bytes"] for f in table["files"]],
                         "row_groups": [len(f["row_groups"]) for f in table["files"]]}}


def check_sampling(stage, cases, modes):
    require(stage in ("pilot", "formal"), "unknown production sampling stage")
    if stage == "formal":
        bases = {case.removesuffix(".dv") for case in cases}
        require(modes == {"generate"} and cases and cases <= set(shapes.cases())
                and cases == {case for base in bases for case in (base, base + ".dv")},
                "formal sampling requires complete full no-DV/DV pairs")


def define(fixtures, binaries, output, stage="pilot", comparison_revision=6):
    require(comparison_revision in (5, 6), "unknown production comparison revision")
    readers = list(reader_roster({"comparison_revision": comparison_revision}))
    import large_workloads
    rows, seen = [], set()
    for root in fixtures:
        root = root.resolve()
        manifest = json.loads((root / "manifest.json").read_text())
        for table in fixture_tables(manifest):
            case = table["id"]
            require(case not in seen, "duplicate production case")
            require(table["delta_log"] is not None, "raw probes need native Delta logs before reader validation")
            seen.add(case)
            row = query_fields(case, manifest)
            rows.append(row | {"fixtures": str(root), "fixture_manifest_sha256": digest(root / "manifest.json")})
    modes = {r["fixture_mode"] for r in rows}
    require(rows and len(modes) == 1
            and len({r["source_parent_manifest_sha256"] for r in rows}) == 1, "mixed probe/full or source identities")
    check_sampling(stage, seen, modes)
    output.mkdir()
    translations = large_workloads.translate(rows, binaries, output, comparison_revision=comparison_revision)
    result = {"format": "selective-read-production-workload-v1", "family": "production", "comparison_revision": comparison_revision,
              "protocol_sha256": digest(SPARK_MATRIX if comparison_revision == 6 else PRODUCTION), "base_protocol_sha256": digest(PROTOCOL),
              "sampling_sha256": digest(SAMPLING), "sampling_stage": stage, "definition_sha256": digest(shapes.CONTRACT),
              "scope": "probe" if rows[0]["fixture_mode"] == "probe" else stage, "publication_ready": False,
              "source_sha256": {str(p.relative_to(HERE)): digest(p) for p in SOURCES},
              "readers": readers, "cases": rows, "sessions": {"reuse." + r["case_id"]: r["case_id"] for r in rows},
              "inventory": {case: {**shape, "status": "prepared" if case in seen else "not_prepared", "readers": readers}
                            for case, shape in shapes.cases().items()},
              "oracle_limits": {"memory_bytes": 16 * 1024**3, "disk_bytes": 1024**3, "elapsed_seconds": 1800},
              "translations": translations}
    save(output / "workload.json", result)
    large_workloads.load(output / "workload.json")
    return result


def load(path, value):
    import matrix
    readers = list(reader_roster(value))
    require(value["comparison_revision"] in (5, 6) and value["family"] == "production"
            and value["definition_sha256"] == digest(shapes.CONTRACT)
            and value["source_sha256"] == {str(p.relative_to(HERE)): digest(p) for p in SOURCES}, "production workload sources changed")
    cases = {r["case_id"]: r for r in value["cases"]}
    require(cases and len(cases) == len(value["cases"]) and set(cases) <= set(shapes.cases()), "unknown/duplicate production cases")
    require(value["readers"] == readers and value["sessions"] == {"reuse." + c: c for c in cases}
            and value["inventory"] == {c: {**v, "status": "prepared" if c in cases else "not_prepared", "readers": readers}
                                       for c, v in shapes.cases().items()}, "incomplete production inventory")
    modes = {r["fixture_mode"] for r in cases.values()}
    require(modes in ({"probe"}, {"generate"}) and value["scope"] == ("probe" if modes == {"probe"} else value["sampling_stage"])
            and value["publication_ready"] is False, "invalid production scope")
    check_sampling(value["sampling_stage"], set(cases), modes)
    require(value["oracle_limits"] == {"memory_bytes": 16 * 1024**3, "disk_bytes": 1024**3, "elapsed_seconds": 1800}, "production oracle limits changed")
    require(set(value["translations"]) == ({"polars"} if value["comparison_revision"] == 6 else {"polars", "daft"}), "missing native translations")
    require(all(set(row["native_expression_sha256"]) == set(value["translations"]) for row in cases.values()),
            "case native expressions differ from the comparison roster")
    for reader, translation in value["translations"].items():
        require(translation["lock_sha256"] == digest(HERE / "runners" / reader / "lock.json")
                and set(translation["expressions"]) == set(cases), "native translation lock/cases changed")
        for case, expression in translation["expressions"].items():
            require(matrix.sha(expression["native_expression"]) == expression["sha256"]
                    == cases[case]["native_expression_sha256"][reader], "native expression changed")
    deletion_unions = {}
    for case, row in cases.items():
        binding(value, Path(row["fixtures"]), case)
        if row["deletion_vectors"]:
            manifest = oracle.load_json(Path(row["fixtures"]) / "manifest.json")
            keys = manifest["production_dv"]["extra_logical_keys"]
            require(deletion_unions.setdefault(row["shape"], keys) == keys,
                    "paired DV layouts delete different logical rows")
    return value


def binding(value, fixtures, case):
    row = next(r for r in value["cases"] if r["case_id"] == case)
    path = Path(fixtures) / "manifest.json"
    require(digest(path) == row["fixture_manifest_sha256"], "production fixture identity changed")
    expected = query_fields(case, json.loads(path.read_text()))
    require(all(row[k] == v for k, v in expected.items()), "production SQL, literals, source or geometry changed")
    return row


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


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--fixtures", type=Path, action="append", required=True)
    parser.add_argument("--binary", type=Path, action="append", required=True, help="pinned Polars build; revision 5 also requires Daft")
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--stage", choices=("pilot", "formal"), default="pilot")
    parser.add_argument("--comparison-revision", type=int, choices=(5, 6), default=6)
    args = parser.parse_args()
    result = define(args.fixtures, args.binary, args.output, args.stage, args.comparison_revision)
    print(json.dumps({"comparison_revision": result["comparison_revision"], "scope": result["scope"], "prepared_cases": len(result["cases"]), "core_cases": 8}))

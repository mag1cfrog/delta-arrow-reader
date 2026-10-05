"""Small exact examples and corruption checks for the benchmark oracle."""

from datetime import date
from decimal import Decimal
import json
import os
from pathlib import Path
import subprocess
import shutil
import sys
import tempfile
import unittest
from unittest.mock import patch

import pyarrow as pa
import pyarrow.parquet as pq

import oracle
import file_organizations


ARROW_TYPES = {"int64": pa.int64(), "int32": pa.int32(), "date32": pa.date32(),
               "string": pa.string(), "decimal(15,2)": pa.decimal128(15, 2)}
SCHEMA = pa.schema([pa.field(name, ARROW_TYPES[kind], nullable=False) for name, kind in oracle.FIELDS])
WIDE_SCHEMA = pa.schema(list(SCHEMA) + [pa.field(name, pa.int64()) for name in oracle.PAYLOADS])


def write_json(path, value):
    path.write_text(json.dumps(value, default=str))


def dataset(root, key_offset=0):
    rows = []
    for number in range(1, 153):
        rows.append(dict(zip(oracle.ORIGINAL, [
            number + key_offset, number, 10, 1, Decimal("17.00"), Decimal("1234.56"),
            Decimal("0.06"), Decimal("0.02"), "N", "O", date(1995, 3, 15),
            date(1995, 3, 16), date(1995, 3, 20), "DELIVER IN PERSON", "AIR", "text with trailing spaces  ",
        ])))
    changes = {
        141: {"l_shipdate": date(1995, 3, 21)},
        142: {"l_shipdate": date(1995, 3, 22)},
        143: {"l_shipdate": date(1994, 1, 1), "l_discount": Decimal("0.05"), "l_quantity": Decimal("23.99")},
        144: {"l_shipdate": date(1994, 12, 31), "l_discount": Decimal("0.07")},
        145: {"l_shipdate": date(1995, 1, 1)},
        146: {"l_shipdate": date(1994, 7, 1), "l_discount": Decimal("0.04")},
        147: {"l_shipdate": date(1994, 7, 1), "l_discount": Decimal("0.08")},
        148: {"l_shipdate": date(1994, 7, 1), "l_quantity": Decimal("24.00")},
        149: {"l_shipdate": date(1994, 7, 1), "l_quantity": Decimal("23.99")},
        150: {"l_shipdate": date(1993, 12, 31)},
        151: {"l_shipmode": "SHIP"}, 152: {"l_shipmode": "RAIL"},
    }
    for key, values in changes.items():
        rows[key - 1].update(values)
    wide = [row | {name: oracle.payload(row["l_orderkey"], 1, i) for i, name in enumerate(oracle.PAYLOADS)} for row in rows]
    literals = list(range(1, 21))

    def write_group(name, ordered, schema, delta):
        directory = root / name
        directory.mkdir(parents=True)
        files = []
        for start in range(0, len(ordered), 64):
            chunk = ordered[start:start + 64]
            path = directory / f"part-{len(files):05}.parquet"
            pq.write_table(pa.Table.from_pylist(chunk, schema=schema), path, row_group_size=32)
            stats = {"numRecords": len(chunk), "minValues": {}, "maxValues": {}, "nullCount": {}}
            for field in schema:
                values = [r[field.name] for r in chunk if r[field.name] is not None]
                stats["minValues"][field.name] = min(values) if values else None
                stats["maxValues"][field.name] = max(values) if values else None
                stats["nullCount"][field.name] = len(chunk) - len(values)
            files.append({"path": path.name, "rows": len(chunk), "bytes": path.stat().st_size,
                          "sha256": oracle.digest_file(path), "delta_stats": stats})
        group = {"path": name, "files": files, "rows": len(ordered), "scale_factor": "tiny", "delta_log": None}
        if delta:
            log = directory / "_delta_log/00000000000000000000.json"
            log.parent.mkdir()
            actions = [{"protocol": {"minReaderVersion": 1, "minWriterVersion": 2}}]
            actions += [{"add": {"path": f["path"], "size": f["bytes"], "stats": json.dumps(f["delta_stats"], default=str)}} for f in files]
            log.write_text("\n".join(json.dumps(a) for a in actions) + "\n")
            group.update(id=name, snapshot_version=0, deletion_vectors=False,
                         delta_log={"path": "_delta_log/" + log.name, "bytes": log.stat().st_size, "sha256": oracle.digest_file(log)})
        return group

    source = write_group("source", rows, SCHEMA, False)
    source["in_literals"] = literals
    for name, cases in (("queries", oracle.ORIGINAL_CASES), ("wide_queries", oracle.WIDE_CASES)):
        source[name] = {key: oracle.sql_for(*spec, literals) for key, spec in cases.items()}
    tables = []
    for name, data, schema in (("li", rows, SCHEMA), ("wide", wide, WIDE_SCHEMA)):
        for layout in ("clustered", "shuffled"):
            ordered = sorted(data, key=lambda r: (r["l_shipdate"], r["l_shipmode"], r["l_partkey"])) if layout == "clustered" else list(reversed(data))
            tables.append(write_group(f"{name}.{layout}", ordered, schema, True))
    write_json(root / "manifest.json", {"status": "complete", "protocol": "selective-read-v1",
               "protocol_sha256": "0" * 64, "profile": "smoke", "sources": [source], "tables": tables})
    return wide


def expected_keys(query):
    # Hand-selected boundary results, independent of the predicate evaluator.
    if query in ("all-keys", "all-full", "all-wide"):
        return list(range(1, 153))
    if query == "empty":
        return []
    if query.startswith("date7"):
        return list(range(1, 142)) + [151, 152]
    if query == "q6-scan":
        return [143, 144, 149]
    if query == "eq1":
        return list(range(1, 141)) + [151, 152]
    if query == "eq2":
        return list(range(1, 141))
    return [1] if query == "eq2-in1" else list(range(1, 21))


class OracleTests(unittest.TestCase):
    def test_sort_spill_respects_disk_quota(self):
        count = 600000
        schema = oracle.result_schema(oracle.KEYS)
        expected = pa.Table.from_arrays([pa.array(range(count), type=pa.int64()),
                   pa.repeat(pa.scalar(1, type=pa.int32()), count)], schema=schema)
        actual = expected.take(pa.array(range(count - 1, -1, -1)))
        with tempfile.TemporaryDirectory() as temporary, patch.object(oracle, "SORT_MEMORY_BYTES", 8 * 1024**2):
            reference = Path(temporary) / "reference.parquet"
            pq.write_table(expected, reference, compression="zstd")
            self.assertEqual(oracle.compare(actual.to_batches(8192), reference, oracle.KEYS, count,
                                           max_bytes=32 * 1024**2), count)
            with self.assertRaisesRegex(ValueError, "max_temp_directory_size"):
                oracle.compare(actual.to_batches(8192), reference, oracle.KEYS, count, max_bytes=1)
            self.assertFalse(list(reference.parent.glob("oracle-sort-*")))

    def test_sort_rejects_duplicate_keys_across_batch_boundaries(self):
        with tempfile.TemporaryDirectory() as temporary:
            reference = Path(temporary) / "reference.parquet"
            count = oracle.BATCH_ROWS * 2 + 7
            rows = [{"l_orderkey": n, "l_linenumber": 1} for n in range(count)]
            oracle.write_reference(reference, rows, oracle.KEYS)
            table = pa.Table.from_pylist(list(reversed(rows)), schema=oracle.result_schema(oracle.KEYS))
            self.assertEqual(oracle.compare(table.to_batches(37), reference, oracle.KEYS, count), count)
            rows[oracle.BATCH_ROWS] = rows[oracle.BATCH_ROWS - 1]
            table = pa.Table.from_pylist(rows, schema=table.schema)
            with self.assertRaisesRegex(ValueError, "duplicated key"):
                oracle.compare(table.to_batches(37), reference, oracle.KEYS, count)
            self.assertFalse(list(reference.parent.glob("oracle-sort-*")))

    def test_wide_file_geometry_rejects_small_files_and_mixed_scales(self):
        import wide_files
        import large_workloads
        self.assertEqual(len(large_workloads.definitions("files")), 4)
        self.assertEqual(len(large_workloads.FILE_SESSIONS), 2)
        schema = {"fields": [{"name": name} for name in oracle.ORIGINAL + oracle.PAYLOADS]}
        files = [{"path": f"part-{i:05}.parquet", "bytes": 64 * 1024**2,
                  "rows": 1, "source_ordinal_range": [i, i + 1], "row_groups": [{"rows": 1}],
                  "delta_stats": {"numRecords": 1, "minValues": {"l_shipdate": "1995-03-15" if i == 0 else "1995-02-28",
                    "l_shipmode": "AIR", "l_partkey": 1}, "maxValues": {"l_shipdate": "1995-03-15" if i == 0 else "1995-02-28",
                    "l_shipmode": "AIR", "l_partkey": 1}, "nullCount": {"l_shipdate": 0, "l_shipmode": 0, "l_partkey": 0}}} for i in range(4096)]
        normal = {"id": "wide.clustered", "scale_factor": 10, "rows": 4096, "schema": schema,
                  "deletion_vectors": False, "snapshot_version": 0, "files": files, "file_count": 4096}
        repacked = normal | {"id": "wide.files4096"}
        manifest = {"wide_file_pair": {"kind": "large"}, "sources": [{"scale_factor": 10, "rows": 4096, "in_literals": [1]}],
                    "tables": [normal, repacked, normal | {"id": "wide.clustered.dv", "deletion_vectors": True, "snapshot_version": 1},
                               repacked | {"id": "wide.files4096.dv", "deletion_vectors": True, "snapshot_version": 1}]}
        shape = wide_files.geometry(manifest, large=True, smoke=False)
        self.assertEqual(len(shape["excluded_files"]), 4095)
        for file in files:
            file["bytes"] -= 1
        with self.assertRaisesRegex(ValueError, "64 MiB"):
            wide_files.geometry(manifest, large=True, smoke=False)
        for file in files:
            file["bytes"] += 1
        normal["scale_factor"] = 30
        with self.assertRaisesRegex(ValueError, "scale, rows or schema"):
            wide_files.geometry(manifest, large=True, smoke=False)

    def test_large_query_identity_boundaries_and_exact_values(self):
        import large_workloads as large
        import matrix
        from runners import run, python_common
        predicates = oracle.conditions("date30", [])
        for day, matches in ((date(1995, 2, 28), False), (date(1995, 3, 1), True),
                             (date(1995, 3, 30), True), (date(1995, 3, 31), False)):
            self.assertEqual(oracle.prefix_matches({"l_shipdate": day}, predicates) == 2, matches)
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            # Rust formats integral large scales as "1"; JSON preserves 1.0 and
            # the exact oracle decodes it as Decimal("1.0"). Check the real logs.
            metadata_root = root / "metadata"
            metadata_root.mkdir()
            write_json(metadata_root / "manifest.json", {"profile": "large"})
            metadata = {"id": str(oracle.uuid.uuid5(oracle.uuid.NAMESPACE_URL,
                        "https://github.com/mag1cfrog/delta-arrow-reader/selective-read-v1/large-sf1/wide.shuffled.dv")),
                        "schemaString": "{}", "configuration": {}}
            protocols = [{"minReaderVersion": 1, "minWriterVersion": 2},
                         {"minReaderVersion": 3, "minWriterVersion": 7,
                          "readerFeatures": ["deletionVectors"], "writerFeatures": ["deletionVectors"]}]
            logs = []
            for version, protocol in enumerate(protocols):
                path = metadata_root / "_delta_log" / f"{version:020}.json"
                path.parent.mkdir(exist_ok=True)
                current = metadata | {"configuration": {"delta.enableDeletionVectors": "true"}} if version else metadata
                path.write_text(json.dumps({"protocol": protocol}) + "\n" + json.dumps({"metaData": current}) + "\n")
                logs.append({"path": str(path.relative_to(metadata_root)), "bytes": path.stat().st_size, "sha256": run.digest(path)})
            table = {"id": "wide.shuffled.dv", "path": ".", "files": [], "scale_factor": Decimal("1.0"),
                     "snapshot_version": 1, "schema": {}, "delta_logs": logs, "delta_log": logs[-1]}
            self.assertEqual(len(oracle.objects(metadata_root, table, None)), 2)
            fixtures = root / "fixtures"
            rows = dataset(fixtures)
            manifest = json.loads((fixtures / "manifest.json").read_text())
            for group in manifest["tables"] + manifest["sources"]:
                group.update(scale_factor=.01, file_count=len(group["files"]), bytes=sum(f["bytes"] for f in group["files"]))
                for item in group["files"]:
                    item["row_groups"] = []  # This unit fixture tests values, not writer geometry.
            # Bind all definitions, but execute only the no-DV cases here. The real
            # generator/manual smoke checks exercise native DV objects and translations.
            for layout in ("clustered", "shuffled"):
                table = next(t for t in manifest["tables"] if t["id"] == "wide." + layout)
                variant = json.loads(json.dumps(table))
                variant.update(id=table["id"] + ".dv", snapshot_version=1, deletion_vectors=True)
                variant["queries"] = {f"wide.{layout}.date30-wide.dv": oracle.sql_for(oracle.WIDE69, "date30", None, [1])}
                manifest["tables"].append(variant)
            write_json(fixtures / "manifest.json", manifest)
            expressions = {case: {"native_expression": "unit", "sha256": matrix.sha("unit")} for case in large.definitions()}
            cases = [large.query_fields(case, manifest) | {"fixtures": str(fixtures),
                     "fixture_manifest_sha256": run.digest(fixtures / "manifest.json"),
                     "native_expression_sha256": dict.fromkeys(("polars", "daft"), matrix.sha("unit"))}
                     for case in large.definitions()]
            value = {"format": "selective-read-large-workload-v1", "comparison_revision": 4,
                     "protocol_sha256": run.digest(run.AMENDMENT), "base_protocol_sha256": run.digest(run.PROTOCOL),
                     "sampling_sha256": run.digest(run.SAMPLING),
                     "scope": "smoke", "publication_ready": False, "scales": {"data": .01, "control": .01},
                     "readers": list(large.READERS), "sessions": large.SESSIONS, "cases": cases,
                     "source_sha256": {str(p.relative_to(large.HERE)): run.digest(p) for p in large.SOURCES},
                     "oracle_limits": {"memory_bytes": 16 * 1024**3, "disk_bytes": 64 * 1024**2, "elapsed_seconds": 30},
                     "translations": {r: {"expressions": expressions, "lock_sha256": run.digest(large.HERE / "runners" / r / "lock.json")}
                                      for r in ("polars", "daft")}}
            workload = root / "workload.json"
            write_json(workload, value)
            self.assertEqual(len(large.load(workload)["cases"]), 18)
            for case in ("large.wide.clustered.date30-wide", "large.wide.shuffled.date7-wide", "scale-control.wide.clustered.eq2-in20"):
                reference = root / case
                metadata = oracle.prepare(fixtures, case, reference, workload=workload)
                request = run.request(fixtures, case, "reuse", "validation", "unit", workload=workload)
                python_common.validate(request)
                self.assertEqual(run.query_count(request), 2)
                self.assertEqual(run.comparison_identity(request), run.comparison_identity(metadata))
                self.assertEqual(metadata["output_rows"], 144 if "date30" in case else 143 if "date7" in case else 20)
                self.assertGreater(metadata["projected_logical_bytes"], metadata["output_rows"])
                projection = metadata["projection"]
                expected = set(range(1, 143)) | {151, 152} if "date30" in case else set(range(1, 142)) | {151, 152} if "date7" in case else set(range(1, 21))
                table = pa.Table.from_pylist([r for r in rows if r["l_orderkey"] in expected],
                                            schema=pa.schema([WIDE_SCHEMA.field(c) for c in projection]))
                result = root / (case + ".arrow")
                identity = root / (case + ".json")
                provenance = {k: metadata[k] for k in oracle.IDENTITY_FIELDS} | run.comparison_identity(metadata)
                provenance.update(reader_id="delta-arrow-reader", reader_build_sha256="1" * 64,
                                  reader_config_sha256="2" * 64, native_expression_sha256=None)
                def export(current):
                    with result.open("wb") as sink, pa.ipc.new_stream(sink, current.schema) as writer:
                        writer.write_table(current, max_chunksize=7)
                    write_json(identity, provenance | {"result_sha256": run.digest(result)})
                export(table)
                self.assertEqual(oracle.check(reference, fixtures, result, identity)["status"], "passed")
                for field in ("base_protocol_sha256", "workload_manifest_sha256", "sampling_sha256"):
                    altered = json.loads(identity.read_text()) | {field: "0" * 64}
                    write_json(identity, altered)
                    with self.assertRaisesRegex(ValueError, "identity mismatch"):
                        oracle.check(reference, fixtures, result, identity)
                    export(table)
                corrupted = table.set_column(0, table.schema.field(0), pa.array([0] * table.num_rows, type=pa.int64()))
                export(corrupted)
                with self.assertRaises(ValueError):
                    oracle.check(reference, fixtures, result, identity)
                if "date30" in case:
                    export(table)
                    session = root / "session"
                    session.mkdir()
                    shutil.copyfile(result, session / "query-0.arrow")
                    with (session / "query-1.arrow").open("wb") as stream:
                        stream.truncate(value["oracle_limits"]["disk_bytes"] // 2)
                    with self.assertRaisesRegex(ValueError, "session exports"):
                        oracle.check(reference, fixtures, session / "query-0.arrow", identity)
            for change in ({"base_protocol_sha256": "0" * 64}, {"sampling_sha256": "0" * 64},
                           {"comparison_revision": 3}, {"scales": {"data": 10, "control": 10}}, {"cases": cases[:-1]}):
                write_json(workload, value | change)
                with self.assertRaises(ValueError):
                    large.load(workload)
            with self.assertRaisesRegex(ValueError, "disk allowance"):
                oracle.write_reference(root / "quota.parquet", rows, oracle.WIDE69, max_bytes=8192)
            # A pilot can validate the real anchor without preparing the other
            # layouts/DVs; it must never masquerade as the complete data family.
            anchor = large.SESSIONS["reuse.large.date30"]
            pilot = value | {"family": "pilot", "scales": {"data": .01}, "sessions": large.PILOT_SESSIONS,
                             "cases": [r for r in cases if r["case_id"] == anchor],
                             "translations": {r: t | {"expressions": {anchor: t["expressions"][anchor]}}
                                              for r, t in value["translations"].items()}}
            write_json(workload, pilot)
            self.assertEqual(len(large.load(workload)["cases"]), 1)
            metadata = oracle.prepare(fixtures, anchor, root / "pilot-reference", workload=workload)
            self.assertEqual(metadata["output_rows"], 144)
            request = run.request(fixtures, anchor, "reuse", "validation", "pilot", workload=workload)
            python_common.validate(request)
            self.assertEqual(run.comparison_identity(request), run.comparison_identity(metadata))
            for change in ({"family": "data"}, {"scope": "candidate"}, {"publication_ready": True},
                           {"readers": list(large.READERS[:-1])}, {"scales": {"data": 2}}):
                write_json(workload, pilot | change)
                with self.assertRaises(ValueError):
                    large.load(workload)
            for case in ("large.wide.clustered.eq2-in20", "large.wide.clustered.eq2-in20-keys"):
                selected = pilot | {"pilot_case": case, "cases": [r for r in cases if r["case_id"] == case],
                    "sessions": {k: v for k, v in large.SESSIONS.items() if v == case},
                    "translations": {r: t | {"expressions": {case: t["expressions"][case]}}
                                     for r, t in value["translations"].items()}}
                write_json(workload, selected)
                self.assertEqual(large.load(workload)["cases"][0]["case_id"], case)
                metadata = oracle.prepare(fixtures, case, root / case, workload=workload)
                self.assertEqual(metadata["output_rows"], 20)
                self.assertEqual(len(metadata["projection"]), 2 if case.endswith("-keys") else 69)
                for change in ({"family": "data"}, {"pilot_case": ""}, {"pilot_case": anchor},
                               {"pilot_case": "large.wide.clustered.date30-wide.dv"},
                               {"pilot_case": "scale-control.wide.clustered.eq2-in20"},
                               {"sessions": large.SESSIONS}):
                    write_json(workload, selected | change)
                    with self.assertRaises(ValueError):
                        large.load(workload)

    def test_saved_deletions_filter_matching_rows_and_limit_membership(self):
        # The Rust check covers the native bitmap envelope and boundary ordinals.
        # Here isolate the independent logical/physical-list and result checks.
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            fixtures = root / "fixtures"
            rows = dataset(fixtures, key_offset=2500)
            manifest = json.loads((fixtures / "manifest.json").read_text())
            base = manifest["tables"][0]
            base["schema"] = {"fields": list(oracle.ORIGINAL)}
            table = json.loads(json.dumps(base))
            table.update(id="li.clustered.dv", path="li.clustered.dv", base_fixture_id=base["id"],
                         variant="dv", dv_features=True, snapshot_version=1, deletion_vectors=True)
            table["queries"] = {f"li.clustered.{q}.dv": manifest["sources"][0]["queries"][q]
                                for q in ("date7-full", "date7-limit")}
            shutil.copytree(fixtures / base["path"], fixtures / table["path"])
            affected = 0
            for item in table["files"]:
                physical = pq.read_table(fixtures / table["path"] / item["path"]).to_pylist()
                indices = [i for i, row in enumerate(physical) if row["l_orderkey"] == 2581]
                if indices:
                    affected += 1
                    item["deletion_vector"] = {"physical_ordinals": indices, "logical_ids": [[2581, 1]]}
                    item["delta_stats"]["tightBounds"] = False
            table["deletion_summary"] = dict(physical_rows=152, deleted_rows=1, live_rows=151,
                                               dv_files=affected, density=1/152, file_coverage=affected/3)
            manifest["tables"].append(table)
            write_json(fixtures / "manifest.json", manifest)
            with patch.object(oracle, "objects", return_value=[]):
                full = oracle.prepare(fixtures, "li.clustered.date7-full.dv", root / "full")
                limited = oracle.prepare(fixtures, "li.clustered.date7-limit.dv", root / "limited")
                self.assertEqual((full["physical_qualifying_rows"], full["qualifying_rows"], full["deleted_qualifying_rows"]), (143, 142, 1))
                self.assertEqual(limited["output_rows"], 100)
                live = [row for row in rows if row["l_orderkey"] != 2581 and row["l_shipdate"] >= date(1995, 3, 15) and row["l_shipdate"] < date(1995, 3, 22)]
                for i, subset in enumerate((live[:100], list(reversed(live))[:100])):
                    self.assertEqual(oracle.compare(oracle.row_batches(subset, oracle.ORIGINAL),
                                     root / "limited/reference.parquet", oracle.ORIGINAL, 100, subset=True), 100)
                with self.assertRaisesRegex(ValueError, "wrong row membership"):
                    oracle.compare(oracle.row_batches([rows[80], *live[:99]], oracle.ORIGINAL),
                                   root / "limited/reference.parquet", oracle.ORIGINAL, 100, subset=True)
                affected_file = next(f for f in table["files"] if "deletion_vector" in f)
                affected_file["deletion_vector"]["physical_ordinals"][0] += 1
                write_json(fixtures / "manifest.json", manifest)
                with self.assertRaisesRegex(ValueError, "physical deletion ordinal"):
                    oracle.prepare(fixtures, "li.clustered.date7-full.dv", root / "wrong-position")

    def test_control_formulas_and_request_mapping(self):
        from runners import run
        expected = {
            "row-groups.select": [row for f in range(16) for row in range((f * 16 + 7) * 4096, (f * 16 + 8) * 4096)],
            "pages.localized": list(range(32)) + list(range(4096, 4128)),
            "pages.scattered": list(range(0, 8192, 128)),
        }
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            tables = []
            for case, fixture in oracle.CONTROL_CASES.items():
                rows = 1048576 if case == "row-groups.select" else 8192
                self.assertEqual([i for i in range(rows) if oracle.control_matches(case, i)], expected[case])
                sql = f"SELECT {', '.join(oracle.CONTROL_PROJECTION)} FROM bench WHERE event_id = 'match'"
                tables.append(dict(id=fixture, path=fixture, snapshot_version=0, deletion_vectors=False, queries={case: sql}))
            write_json(root / "manifest.json", dict(status="complete", protocol="selective-read-v1", profile="smoke", tables=tables, sources=[]))
            for case, fixture in oracle.CONTROL_CASES.items():
                request = run.request(root, case, "open", "validation", "check")
                self.assertEqual(request["table_uri"], (root / fixture).as_uri())
                self.assertEqual(request["canonical_sql"], oracle.case_input(root, case)[-1])
        row = oracle.control_record(0)
        self.assertEqual(row["row_id"], 0)
        self.assertIsNone(row["payload_000"])
        self.assertEqual(row["payload_001"], "payload-001-00000000-" + "abcdefghijklmnopqrstuvwxyz0123456789" * 12)
        self.assertEqual(oracle.record(row, oracle.CONTROL_PROJECTION), oracle.control_record(0))
        row["row_id"] = None
        with self.assertRaises(ValueError):
            oracle.record(row, oracle.CONTROL_PROJECTION)

    @classmethod
    def setUpClass(cls):
        cls.temp = tempfile.TemporaryDirectory()
        cls.root = Path(cls.temp.name)
        cls.fixtures = cls.root / "fixtures"
        cls.rows = dataset(cls.fixtures)
        cls.references = {}
        for group, cases in (("li", oracle.ORIGINAL_CASES), ("wide", oracle.WIDE_CASES)):
            for layout in ("clustered", "shuffled"):
                for query in cases:
                    case = f"{group}.{layout}.{query}"
                    reference = cls.root / case
                    metadata = oracle.prepare(cls.fixtures, case, reference)
                    cls.references[case] = reference, metadata

    @classmethod
    def tearDownClass(cls):
        cls.temp.cleanup()

    def export(self, case, table=None, identity_change=None):
        reference, metadata = self.references[case]
        if table is None:
            keys = list(reversed(expected_keys(case.split(".")[-1])))
            if metadata["limit"]:
                keys = keys[:metadata["limit"]]
            schema = pa.schema([WIDE_SCHEMA.field(name).with_nullable(True) for name in metadata["projection"]])
            table = pa.Table.from_pylist([self.rows[key - 1] for key in keys], schema=schema)
        # IPC tables can retain file-backed buffers. Keep each exported artifact immutable.
        directory = Path(tempfile.mkdtemp(prefix="result-", dir=self.root))
        result, identity = directory / "result.arrow", directory / "identity.json"
        with result.open("wb") as sink, pa.ipc.new_stream(sink, table.schema) as writer:
            writer.write_table(table, max_chunksize=7)
        provenance = {name: metadata[name] for name in oracle.IDENTITY_FIELDS}
        provenance.update(reader_id="delta-arrow-reader", reader_build_sha256="1" * 64,
                          reader_config_sha256="2" * 64, native_expression_sha256=None,
                          result_sha256=oracle.digest_file(result))
        provenance.update(identity_change or {})
        write_json(identity, provenance)
        return reference, self.fixtures, result, identity

    def result_table(self, case):
        args = self.export(case)
        with pa.ipc.open_stream(args[2]) as stream:
            return stream.read_all()

    def test_all_thirty_cases_and_layout_equivalence(self):
        for case, (reference, metadata) in self.references.items():
            with self.subTest(case=case):
                count = len(expected_keys(case.split(".")[-1]))
                self.assertEqual(metadata["qualifying_rows"], count)
                self.assertEqual(metadata["output_rows"], min(count, metadata["limit"] or count))
                self.assertEqual(oracle.check(*self.export(case))["status"], "passed")
                if ".shuffled." in case:
                    other = self.references[case.replace(".shuffled.", ".clustered.")][1]
                    self.assertEqual(metadata["reference_sha256"], other["reference_sha256"])
        self.assertEqual(oracle.payload(1, 1, 0), 2558623671389681668)
        self.assertEqual(oracle.payload(1, 1, 2), -11468591226400559)
        steps = self.references["wide.clustered.eq2-in20"][1]["predicate_steps"]
        self.assertEqual([step["rows"] for step in steps], [142, 140, 20])
        empty = self.references["li.clustered.empty"][1]
        self.assertEqual(empty["candidate_files"], [])
        self.assertEqual(empty["matching_files"], [])

    def test_duplicate_in_literals(self):
        for query in ("eq2-in1", "eq2-in20"):
            case = "li.clustered." + query
            _, original = self.references[case]
            repeated = oracle.prepare(self.fixtures, case, self.root / (query + "-duplicate"), True)
            self.assertEqual(repeated["reference_sha256"], original["reference_sha256"])
            self.assertNotEqual(repeated["canonical_sql_sha256"], original["canonical_sql_sha256"])
            self.assertEqual(repeated["output_rows"], original["output_rows"])

    def test_repacked_values_and_pruning_regressions(self):
        table = self.result_table("li.clustered.all-full")
        self.assertEqual(file_organizations.same_rows(table.to_batches(7), table.to_batches(33)), 152)
        for bad in (table.slice(1), table.set_column(1, "l_partkey", pa.array([0] * 152, type=pa.int64()))):
            with self.assertRaisesRegex(ValueError, "repacked"):
                file_organizations.same_rows(table.to_batches(7), bad.to_batches(33))
        good = {"case_id": "files4096.eq2-in20", "output_rows": 20, "active_files": 4096,
                "candidate_files": ["part-0"], "matching_files": ["part-0"]}
        file_organizations.pruning(good)
        for changes in ({"candidate_files": []}, {"candidate_files": ["part-0"] * 4096}, {"output_rows": 0}):
            with self.assertRaises(ValueError):
                file_organizations.pruning(good | changes)
        file_organizations.pruning(good | {"case_id": "files64.empty", "output_rows": 0,
                                          "candidate_files": [], "matching_files": []})
        with self.assertRaisesRegex(ValueError, "exclude every file"):
            file_organizations.pruning(good | {"case_id": "files64.empty"})
        manifest_path = self.fixtures / "manifest.json"
        original = manifest_path.read_bytes()
        try:
            manifest = json.loads(original)
            base = next(t for t in manifest["tables"] if t["id"] == "li.clustered")
            manifest["tables"].append(base | {"id": "files64"})
            write_json(manifest_path, manifest)
            for query in ("empty", "eq2-in20"):
                result = oracle.prepare(self.fixtures, "files64." + query, self.root / ("repack-" + query))
                self.assertEqual(result["reference_sha256"], self.references["li.clustered." + query][1]["reference_sha256"])
        finally:
            manifest_path.write_bytes(original)

    def test_file_organization_report_keeps_failures(self):
        import campaign
        from check_campaign import observation
        with tempfile.TemporaryDirectory(dir=self.root) as temporary:
            root = Path(temporary)
            references = root / "references"
            references.mkdir()
            cases = []
            for case in file_organizations.CASES:
                directory = references / case
                directory.mkdir()
                empty = case.endswith(".empty")
                metadata = {"case_id": case, "output_rows": 0 if empty else 20,
                            "active_files": int(case.split(".")[0][5:]),
                            "candidate_files": [] if empty else ["part-0"], "matching_files": [] if empty else ["part-0"]}
                write_json(directory / "reference.json", metadata)
                cases.append(metadata | {"reference_metadata_sha256": oracle.digest_file(directory / "reference.json")})
            jobs = [{"id": c, "case_id": c, "execution_mode": "open"} for c in file_organizations.CASES]
            jobs.append({"id": "reuse.files4096", "case_id": "files4096.eq2-in20", "execution_mode": "reuse"})
            write_json(references / "file-organizations.json", {"status": "complete", "profile": "smoke", "tables": [], "cases": cases,
                       "fixture_manifest_sha256": oracle.digest_file(self.fixtures / "manifest.json")})
            inventory = {j["id"]: {r: {"status": "unsupported" if r == "daft" else "success", "runnable": r != "daft",
                         "reference_sha256": oracle.digest_file(references / j["case_id"] / "reference.json")} for r in campaign.READERS} for j in jobs}
            slots = campaign.schedule(inventory, "test")
            rows = [{**slot, "status": "success", "artifacts": str(root / slot["run_id"]), "observation": observation(100)} for slot in slots]
            next(r for r in rows if r["reader_id"] == "polars" and r["stage"] == "timing")["status"] = "timeout"
            summary = {"status": "incomplete", "integrity_passed": True, "timer_resolution": {"ratio_floor_ns": 1},
                       "jobs": campaign.summarize(inventory, slots, rows, 1)}
            for name, value in (("campaign", {"fixtures": str(self.fixtures), "jobs": jobs}), ("inventory", inventory), ("schedule", slots), ("summary", summary)):
                write_json(root / (name + ".json"), value)
            write_json(root / "frozen.json", {n: oracle.digest_file(root / n) for n in ("campaign.json", "inventory.json", "schedule.json")})
            (root / "observations.jsonl").write_text("".join(json.dumps(r) + "\n" for r in rows))
            self.assertEqual(file_organizations.report(root, references, root / "report"), {"status": "incomplete", "entries": 25})
            result = json.loads((root / "report/file-organizations-report.json").read_text())["rows"]
            self.assertEqual(sum(r["gate"]["status"] == "unsupported" for r in result), 5)
            self.assertTrue(any(r["result"]["sample_statuses"].get("timeout") == 1 and not r["result"]["eligible"] for r in result))
            summary["jobs"][jobs[0]["id"]]["delta-arrow-reader"]["metrics"] = {}
            write_json(root / "summary.json", summary)
            with self.assertRaisesRegex(ValueError, "raw observations"):
                file_organizations.report(root, references, root / "bad-report")

    @unittest.skipUnless(os.environ.get("SELECTIVE_READ_FIXTURES"), "set SELECTIVE_READ_FIXTURES to generated smoke inputs")
    def test_generated_public_smoke(self):
        fixtures = Path(os.environ["SELECTIVE_READ_FIXTURES"])
        checksums = {}
        for group, cases in (("li", oracle.ORIGINAL_CASES), ("wide", oracle.WIDE_CASES)):
            for layout in ("clustered", "shuffled"):
                for query in cases:
                    case = f"{group}.{layout}.{query}"
                    with self.subTest(case=case):
                        metadata = oracle.prepare(fixtures, case, self.root / ("public-" + case))
                        self.assertEqual(metadata["source_rows"], 60175)
                        self.assertLessEqual(set(metadata["matching_files"]), set(metadata["candidate_files"]))
                        if layout == "clustered":
                            checksums[group, query] = metadata["reference_sha256"]
                        else:
                            self.assertEqual(checksums[group, query], metadata["reference_sha256"])

    def test_same_count_corruptions(self):
        case = "li.clustered.q6-scan"
        good = self.result_table(case)
        rows = good.to_pylist()
        rows[0]["l_extendedprice"] += Decimal("0.01")
        wrong_value = pa.Table.from_pylist(rows, schema=good.schema)
        wrong_scale = good.set_column(3, "l_discount", good.column(3).cast(pa.decimal128(15, 3)))
        wrong_interpretation = good.set_column(3, "l_discount", pa.array([Decimal("0.60")] * len(rows), type=pa.decimal128(15, 2)))
        duplicate = pa.concat_tables([good.slice(0, 1), good.slice(0, len(rows) - 1)])
        for wrong in (wrong_value, wrong_scale, wrong_interpretation, duplicate, good.slice(1)):
            with self.subTest(schema=str(wrong.schema)):
                with self.assertRaises(ValueError):
                    oracle.check(*self.export(case, wrong))

    def test_nulls_and_equivalent_strings(self):
        case = "wide.clustered.eq2-in20"
        good = self.result_table(case)
        rows = good.to_pylist()
        name = next(name for name in oracle.PAYLOADS if rows[0][name] is None)
        rows[0][name] = 0
        with self.assertRaisesRegex(ValueError, "wrong row membership or value"):
            oracle.check(*self.export(case, pa.Table.from_pylist(rows, schema=good.schema)))
        nonnull = self.result_table("li.clustered.all-full")
        for name in ("l_shipmode", "l_comment"):
            index = nonnull.schema.get_field_index(name)
            for kind in (pa.string_view(), pa.large_string(), pa.dictionary(pa.int32(), pa.string())):
                equivalent = nonnull.set_column(index, name, nonnull.column(index).cast(kind))
                self.assertEqual(oracle.check(*self.export("li.clustered.all-full", equivalent))["status"], "passed")
        nonnull = nonnull.set_column(0, "l_orderkey", pa.array([None] * nonnull.num_rows, type=pa.int64()))
        with self.assertRaisesRegex(ValueError, "unexpected null"):
            oracle.check(*self.export("li.clustered.all-full", nonnull))

    def test_limit_membership_and_identity(self):
        case = "li.clustered.date7-limit"
        good = self.result_table(case)
        # The positive case already returns the last 100 qualifying keys, in reverse order.
        rows = good.to_pylist()
        rows[0] = self.rows[141]  # March 22 is outside the half-open interval.
        with self.assertRaisesRegex(ValueError, "wrong row membership"):
            oracle.check(*self.export(case, pa.Table.from_pylist(rows, schema=good.schema)))
        for name, value in (("snapshot_version", 1), ("snapshot_version", False),
                            ("reader_build_sha256", ""), ("canonical_sql_sha256", "0" * 64),
                            ("native_expression_sha256", "bad"), ("result_sha256", "0" * 64)):
            with self.subTest(field=name):
                with self.assertRaises(ValueError):
                    oracle.check(*self.export(case, identity_change={name: value}))
        args = self.export(case, identity_change={"snapshot_version": 1})
        command = [sys.executable, str(Path(oracle.__file__)), "check"]
        for name, value in zip(("reference", "fixtures", "result", "identity"), args):
            command += ["--" + name, str(value)]
        process = subprocess.run(command, capture_output=True, text=True)
        self.assertEqual(process.returncode, 1)
        self.assertEqual(json.loads(process.stdout)["status"], "validation_failed")

    def test_incomplete_stale_and_unsafe_inputs(self):
        with self.assertRaises(FileExistsError):
            oracle.prepare(self.fixtures, "li.clustered.all-full", self.references["li.clustered.all-full"][0])
        with self.assertRaisesRegex(ValueError, "escapes"):
            oracle.inside(self.fixtures, "../outside")
        args = self.export("li.clustered.all-full")
        result = args[2]
        with result.open("ab") as stream:
            stream.write(b"extra")
        with self.assertRaisesRegex(ValueError, "checksum"):
            oracle.check(*args)
        for field, value in (("protocol_sha256", "bad"), ("oracle_sha256", "bad"),
                             ("oracle_dependencies_sha256", "bad"), ("duckdb_sort", "bad"),
                             ("fixture_manifest_sha256", "bad"), ("reference_sha256", "bad")):
            path = args[0] / "reference.json"
            original = path.read_bytes()
            try:
                metadata = json.loads(original)
                metadata[field] = value
                write_json(path, metadata)
                with self.assertRaises(ValueError):
                    oracle.check(*args)
            finally:
                path.write_bytes(original)

    def test_source_corruption_and_false_file_bounds(self):
        manifest_path = self.fixtures / "manifest.json"
        original_manifest = manifest_path.read_bytes()
        manifest = json.loads(original_manifest)
        source_file = self.fixtures / manifest["sources"][0]["path"] / manifest["sources"][0]["files"][0]["path"]
        original_file = source_file.read_bytes()
        try:
            source_file.write_bytes(b"BAD!" + original_file[4:])
            with self.assertRaisesRegex(ValueError, "checksum"):
                oracle.prepare(self.fixtures, "li.clustered.empty", self.root / "bad-source")
        finally:
            source_file.write_bytes(original_file)

        table = next(t for t in manifest["tables"] if t["id"] == "li.clustered")
        log = self.fixtures / table["path"] / table["delta_log"]["path"]
        original_log = log.read_bytes()
        try:
            actions = [json.loads(line) for line in original_log.splitlines()]
            adds = {a["add"]["path"]: a["add"] for a in actions if "add" in a}
            for item in table["files"]:
                for bounds in ("minValues", "maxValues"):
                    item["delta_stats"][bounds]["l_shipdate"] = "1990-01-01"
                adds[item["path"]]["stats"] = json.dumps(item["delta_stats"])
            log.write_text("\n".join(json.dumps(a) for a in actions) + "\n")
            table["delta_log"].update(bytes=log.stat().st_size, sha256=oracle.digest_file(log))
            write_json(manifest_path, manifest)
            with self.assertRaisesRegex(ValueError, "statistics exclude a matching file"):
                oracle.prepare(self.fixtures, "li.clustered.date7-full", self.root / "bad-bounds")
            self.assertFalse((self.root / "bad-bounds/reference.json").exists())
        finally:
            manifest_path.write_bytes(original_manifest)
            log.write_bytes(original_log)


if __name__ == "__main__":
    unittest.main()

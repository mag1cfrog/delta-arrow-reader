"""Independent, untimed reference for the public selective-read cases.

Python evaluates predicates and payloads independently. PyArrow stores typed
references in compressed Parquet; DuckDB only sorts actual Arrow results.
"""

import argparse
import base64
from contextlib import contextmanager
from datetime import date
from decimal import Decimal
import hashlib
import io
from itertools import islice
import json
from pathlib import Path
import platform
import re
import resource
import shutil
import signal
import sys
import tempfile
import uuid
import zlib

import pyarrow as pa
import pyarrow.parquet as pq
import duckdb

sys.path.insert(0, str(Path(__file__).resolve().parent / "runners"))
from run import digest as digest_file, file_identity

PROTOCOL = Path(__file__).resolve().parents[2] / "docs/content/benchmarks/selective-read-protocol.md"
REVISION = 2
BATCH_ROWS = 8192
REFERENCE_ROWS = 131072
SORT_MEMORY_BYTES = 512 * 1024**2
REQUIREMENTS = Path(__file__).with_name("oracle-requirements.txt")
READERS = {"delta-arrow-reader", "delta-rs", "duckdb", "polars", "daft"}
KEYS = ("l_orderkey", "l_linenumber")
FIELDS = (
    ("l_orderkey", "int64"), ("l_partkey", "int64"), ("l_suppkey", "int64"),
    ("l_linenumber", "int32"),
    ("l_quantity", "decimal(15,2)"), ("l_extendedprice", "decimal(15,2)"),
    ("l_discount", "decimal(15,2)"), ("l_tax", "decimal(15,2)"),
    ("l_returnflag", "string"), ("l_linestatus", "string"),
    ("l_shipdate", "date32"), ("l_commitdate", "date32"), ("l_receiptdate", "date32"),
    ("l_shipinstruct", "string"), ("l_shipmode", "string"), ("l_comment", "string"),
)
ORIGINAL = tuple(name for name, _ in FIELDS)
PAYLOADS = tuple(f"payload_{i:02}" for i in range(64))
CONTROL_PAYLOADS = tuple(f"payload_{i:03}" for i in range(16))
CONTROL_PROJECTION = ("row_id",) + CONTROL_PAYLOADS
CONTROL_CASES = {"row-groups.select": "row-groups", "pages.localized": "pages.localized", "pages.scattered": "pages.scattered"}
DV_CASES = ("li.clustered.date7-full", "li.shuffled.date7-full", "wide.clustered.eq2-in20",
            "li.clustered.date7-limit", "row-groups.select", "pages.localized")
TYPES = dict(FIELDS) | dict.fromkeys(PAYLOADS, "int64") | dict.fromkeys(CONTROL_PAYLOADS, "string") | {"row_id": "int32", "event_id": "string"}
WIDE69 = KEYS + ("l_shipdate", "l_shipmode", "l_partkey") + PAYLOADS
# Projection, predicate, output limit. SQL is checked independently of its scalar evaluation.
ORIGINAL_CASES = {
    "all-keys": (KEYS, "all", None),
    "all-full": (ORIGINAL, "all", None),
    "empty": (ORIGINAL, "empty", None),
    "date7-full": (ORIGINAL, "date7", None),
    "date7-keys": (KEYS, "date7", None),
    "date7-limit": (ORIGINAL, "date7", 100),
    "q6-scan": (KEYS + ("l_extendedprice", "l_discount"), "q6", None),
    "eq2-in1": (ORIGINAL, "eq2-in1", None),
    "eq2-in20": (ORIGINAL, "eq2-in20", None),
}
WIDE_CASES = {
    **{name: (WIDE69, name, None) for name in ("eq1", "eq2", "eq2-in1", "eq2-in20")},
    "eq2-in20-keys": (KEYS, "eq2-in20", None),
    "all-wide": (WIDE69, "all", None),
}


def require(condition, message):
    if not condition:
        raise ValueError(message)


def digest_bytes(data):
    return hashlib.sha256(data).hexdigest()


def load_json(path):
    return json.loads(Path(path).read_text(), parse_float=Decimal)


def json_bytes(value):
    return json.dumps(value, ensure_ascii=False, separators=(",", ":")).encode()


def inside(root, relative):
    root = Path(root).resolve()
    require(isinstance(relative, str) and not Path(relative).is_absolute(), "expected relative object path")
    path = (root / relative).resolve()
    require(path.is_relative_to(root), "object path escapes fixture directory")
    return path


def verify_object(root, item):
    path = inside(root, item["path"])
    require(path.stat().st_size == item["bytes"], f"object size changed: {path}")
    require(digest_file(path) == item["sha256"], f"object checksum changed: {path}")
    return path


def logical_type(kind):
    if pa.types.is_dictionary(kind):
        kind = kind.value_type
    if pa.types.is_string(kind) or pa.types.is_large_string(kind) or pa.types.is_string_view(kind):
        return "string"
    if pa.types.is_decimal(kind):
        return f"decimal({kind.precision},{kind.scale})"
    return str(kind).replace("date32[day]", "date32")


def check_schema(schema, projection):
    require(schema.names == list(projection), "output field names/order differ from projection")
    for field in schema:
        require(logical_type(field.type) == TYPES[field.name], f"wrong logical type for {field.name}: {field.type}")


def full_rows(path, projection):
    # No dataset filter, statistics predicate, row-group selection, or Delta reader.
    parquet = pq.ParquetFile(path)
    check_schema(parquet.schema_arrow, projection)
    for batch in parquet.iter_batches(batch_size=BATCH_ROWS):
        for row in batch.to_pylist():
            required = ("row_id", "event_id") if "row_id" in projection else ORIGINAL
            require(all(row[name] is not None for name in required), "null in non-null field")
            yield row


def payload(order, line, column):
    raw = hashlib.sha256(f"dar-wide-v1/{order}/{line}/{column:02}".encode()).digest()
    value = int.from_bytes(raw[:8], "little")
    return None if value % 17 == 0 else (value & 0x7FFFFFFFFFFFFFFF) - 0x4000000000000000


def conditions(predicate, literals):
    if predicate == "control-match":
        return [("event_id", "=", "match")]
    if predicate == "all":
        return []
    if predicate == "empty":
        return [("l_shipdate", "<", date(1990, 1, 1))]
    if predicate == "date7":
        return [("l_shipdate", ">=", date(1995, 3, 15)), ("l_shipdate", "<", date(1995, 3, 22))]
    if predicate == "date30":
        return [("l_shipdate", ">=", date(1995, 3, 1)), ("l_shipdate", "<", date(1995, 3, 31))]
    if predicate == "q6":
        return [
            ("l_shipdate", ">=", date(1994, 1, 1)), ("l_shipdate", "<", date(1995, 1, 1)),
            ("l_discount", ">=", Decimal("0.05")), ("l_discount", "<=", Decimal("0.07")),
            ("l_quantity", "<", Decimal("24.00")),
        ]
    result = [("l_shipdate", "=", date(1995, 3, 15))]
    if predicate != "eq1":
        result.append(("l_shipmode", "=", "AIR"))
    if predicate in ("eq2-in1", "eq2-in20"):
        result.append(("l_partkey", "IN", frozenset(literals[:1] if predicate == "eq2-in1" else literals)))
    return result


def prefix_matches(row, predicates):
    matched = 0
    for name, op, value in predicates:
        actual = row[name]
        if op == "=":
            ok = actual == value
        elif op == "<":
            ok = actual < value
        elif op == "<=":
            ok = actual <= value
        elif op == ">=":
            ok = actual >= value
        else:
            ok = actual in value
        if not ok:
            break
        matched += 1
    return matched


def candidate(stats, predicates):
    for name, op, value in predicates:
        if stats["nullCount"][name] == stats["numRecords"]:
            return False
        low, high = stats["minValues"][name], stats["maxValues"][name]
        require(low is not None and high is not None, f"missing fixture bounds: {name}")
        if TYPES[name] == "date32":
            low, high = date.fromisoformat(low), date.fromisoformat(high)
        elif TYPES[name].startswith("decimal"):
            low, high = Decimal(str(low)), Decimal(str(high))
        if op == "=":
            possible = low <= value <= high
        elif op == "<":
            possible = low < value
        elif op == "<=":
            possible = low <= value
        elif op == ">=":
            possible = high >= value
        else:
            possible = any(low <= v <= high for v in value)
        if not possible:
            return False
    return True


def sql_for(projection, predicate, limit, literals):
    eq1 = "l_shipdate = DATE '1995-03-15'"
    eq2 = f"{eq1} AND l_shipmode = 'AIR'"
    filters = {
        "all": "", "empty": "l_shipdate < DATE '1990-01-01'",
        "date7": "l_shipdate >= DATE '1995-03-15' AND l_shipdate < DATE '1995-03-22'",
        "date30": "l_shipdate >= DATE '1995-03-01' AND l_shipdate < DATE '1995-03-31'",
        "eq1": eq1, "eq2": eq2,
        "eq2-in1": f"{eq2} AND l_partkey IN ({literals[0]})",
        "eq2-in20": f"{eq2} AND l_partkey IN ({', '.join(map(str, literals))})",
        "q6": "l_shipdate >= DATE '1994-01-01' AND l_shipdate < DATE '1995-01-01' AND l_discount BETWEEN CAST('0.05' AS DECIMAL(15,2)) AND CAST('0.07' AS DECIMAL(15,2)) AND l_quantity < CAST('24.00' AS DECIMAL(15,2))",
    }
    sql = f"SELECT {', '.join(projection)} FROM bench"
    if filters[predicate]:
        sql += " WHERE " + filters[predicate]
    if limit is not None:
        sql += f" LIMIT {limit}"
    return sql


def base_case(case_id, large=False):
    for suffix in (".dv", ".feature-only"):
        if case_id.endswith(suffix):
            case = case_id.removesuffix(suffix)
            require((case in DV_CASES or case == "wide.files4096.eq2-in20" or large and case in ("wide.clustered.date30-wide", "wide.shuffled.date30-wide"))
                    if suffix == ".dv" else case == "row-groups.select", "unknown DV case")
            return case, suffix
    return case_id, ""


def case_input(fixtures, case_id, duplicate_literal=False, workload=None):
    require(pa.__version__ == "25.0.1" and duckdb.__version__ == "1.5.5", "oracle dependency version changed")
    manifest = load_json(Path(fixtures) / "manifest.json")
    require(manifest["status"] == "complete" and manifest["protocol"] == "selective-read-v1", "incomplete or unknown fixtures")
    row = None
    if workload is not None:
        import large_workloads
        row = large_workloads.binding(large_workloads.load(workload), fixtures, case_id)
        require(not duplicate_literal, "large query variants need a separate workload identity")
    else:
        require(manifest["profile"] != "large", "large fixtures require a workload manifest")
    case, suffix = base_case(row["query_case_id"] if row else case_id, row is not None)
    fixture_id = CONTROL_CASES[case] if case in CONTROL_CASES else case.rsplit(".", 1)[0]
    table = next(t for t in manifest["tables"] if t["id"] == fixture_id + suffix)
    require(type(table["snapshot_version"]) is int and table["snapshot_version"] == bool(suffix)
            and table["deletion_vectors"] is (suffix == ".dv"), "unexpected snapshot version or DV flag")
    if suffix:
        require(table["base_fixture_id"] == fixture_id and table["variant"] == suffix[1:]
                and table["dv_features"] is True, "incorrect variant identity")
        base = next(t for t in manifest["tables"] if t["id"] == fixture_id)
        require(table["schema"] == base["schema"] and table["rows"] == base["rows"], "DV pair changed schema or physical rows")
        files = []
        for item in table["files"]:
            item = dict(item)
            if item.pop("deletion_vector", None):
                item["delta_stats"] = {k: v for k, v in item["delta_stats"].items() if k != "tightBounds"}
            files.append(item)
        require(files == base["files"], "DV pair changed Parquet objects, statistics, or geometry")
    if case in CONTROL_CASES:
        require(not duplicate_literal, "control predicate has no IN literals")
        sql = f"SELECT {', '.join(CONTROL_PROJECTION)} FROM bench WHERE event_id = 'match'"
        require(table["queries"][case_id] == sql, "control SQL differs from protocol")
        return manifest, table, None, CONTROL_PROJECTION, "control-match", None, [], sql
    fixture_id, query = case.rsplit(".", 1)
    wide = fixture_id.startswith("wide.")
    require(fixture_id in ("li.clustered", "li.shuffled", "wide.clustered", "wide.shuffled", "wide.files4096", "files64", "files4096"),
            "unsupported fixture")
    if fixture_id.startswith("files"):
        require(query in ("empty", "eq2-in20"), "unknown file-organization query")
    source = next(s for s in manifest["sources"] if s["scale_factor"] == table["scale_factor"])
    shapes = large_workloads.shapes() if row else WIDE_CASES if wide else ORIGINAL_CASES
    require(query in shapes, "unknown public query")
    projection, predicate, limit = shapes[query]
    literals = source["in_literals"]
    require(literals and all(type(v) is int for v in literals) and literals == sorted(set(literals)), "invalid frozen IN literals")
    require(len(literals) == 20 or (manifest["profile"] == "smoke" and len(literals) < 20), "wrong frozen literal count")
    canonical = sql_for(projection, predicate, limit, literals)
    require(canonical == (row["canonical_sql"] if row else source["wide_queries" if wide else "queries"][query]), "manifest SQL differs from frozen case")
    if suffix:
        require(table["queries"][row["query_case_id"] if row else case_id] == canonical, "variant SQL changed")
    if duplicate_literal:
        require(predicate in ("eq2-in1", "eq2-in20"), "duplicate literal requires an IN case")
        selected = literals[:1] if predicate == "eq2-in1" else literals
        canonical = canonical.rsplit(")", 1)[0] + f", {selected[0]})"
    return manifest, table, source, projection, predicate, limit, literals, canonical


def record(row, projection, derive_payloads=False, logical_bytes=None):
    values = {}
    for name in projection:
        if derive_payloads and name.startswith("payload_"):
            value = payload(row[KEYS[0]], row[KEYS[1]], int(name[-2:]))
        else:
            value = row[name]
        require(value is not None or name in PAYLOADS + CONTROL_PAYLOADS, f"unexpected null: {name}")
        if logical_bytes is not None and value is not None:
            kind = TYPES[name]
            logical_bytes[0] += len(value.encode()) if kind == "string" else 8 if kind == "int64" else 4 if kind in ("int32", "date32") else 16
        values[name] = value
    return values


def result_schema(projection):
    types = {"int64": pa.int64(), "int32": pa.int32(), "date32": pa.date32(),
             "string": pa.string(), "decimal(15,2)": pa.decimal128(15, 2)}
    return pa.schema([pa.field(name, types[TYPES[name]], nullable=name in PAYLOADS + CONTROL_PAYLOADS)
                      for name in projection])


def row_batches(rows, projection):
    rows = iter(rows)
    schema = result_schema(projection)
    while batch := list(islice(rows, BATCH_ROWS)):
        yield pa.RecordBatch.from_pylist(batch, schema=schema)


def normalized_batches(batches, projection):
    schema = result_schema(projection)
    for batch in batches:
        check_schema(batch.schema, projection)
        for offset in range(0, batch.num_rows, BATCH_ROWS):
            part = batch.slice(offset, BATCH_ROWS)
            for field, column in zip(schema, part.columns):
                require(field.nullable or column.null_count == 0, f"unexpected null: {field.name}")
            yield part.cast(schema)


def ordered_batches(batches, projection):
    keys = ("row_id",) if "row_id" in projection else KEYS
    previous = None
    for batch in batches:
        for key in zip(*(batch.column(name).to_pylist() for name in keys)):
            require(previous is None or previous < key, "duplicated key or unordered result")
            previous = key
        yield batch


def same_rows(left, right):
    """Exact ordered Arrow values, bounded by two batches despite different boundaries."""
    left, right = iter(left), iter(right)
    a, b = next(left, None), next(right, None)
    rows = 0
    while a is not None and b is not None:
        count = min(a.num_rows, b.num_rows)
        require(count > 0, "unexpected empty batch")
        require(a.slice(0, count).equals(b.slice(0, count)), f"repacked values/order differ at ordinal {rows}")
        rows += count
        a = next(left, None) if count == a.num_rows else a.slice(count)
        b = next(right, None) if count == b.num_rows else b.slice(count)
    require(a is None and b is None, "repacked row count differs")
    return rows


class LimitedWriter(io.BufferedWriter):
    def __init__(self, path, max_bytes):
        super().__init__(open(path, "xb", buffering=0))
        self.max_bytes = max_bytes

    def write(self, data):
        require(self.max_bytes is None or self.tell() + len(data) <= self.max_bytes,
                "reference Parquet exceeds its disk allowance")
        return super().write(data)


def write_reference(path, rows, projection, max_bytes=None):
    """The independent source already emits unique keys in ascending order."""
    schema = result_schema(projection)
    pending, count = [], 0
    with LimitedWriter(path, max_bytes) as sink, pq.ParquetWriter(sink, schema, compression="zstd") as writer:
        for batch in ordered_batches(normalized_batches(row_batches(rows, projection), projection), projection):
            pending.append(batch)
            count += batch.num_rows
            if count >= REFERENCE_ROWS:
                writer.write_table(pa.Table.from_batches(pending), row_group_size=REFERENCE_ROWS)
                pending, count = [], 0
        if pending:
            writer.write_table(pa.Table.from_batches(pending), row_group_size=REFERENCE_ROWS)


def compare(batches, reference, projection, expected_count, max_bytes=None, subset=False):
    """Sort only actual rows; Python/Arrow checks their values against the reference."""
    input_count = 0

    def inputs():
        nonlocal input_count
        for batch in normalized_batches(batches, projection):
            input_count += batch.num_rows
            yield batch

    keys = ("row_id",) if "row_id" in projection else KEYS
    with tempfile.TemporaryDirectory(prefix="oracle-sort-", dir=reference.parent) as scratch:
        config = {"threads": 1, "memory_limit": f"{SORT_MEMORY_BYTES}B", "temp_directory": scratch,
                  "autoload_known_extensions": False, "autoinstall_known_extensions": False}
        with duckdb.connect(config=config) as db, pq.ParquetFile(reference) as expected:
            if max_bytes is not None:
                # In the pinned build the startup config reports this quota but does
                # not enforce it on spill; setting it after connect does enforce it.
                db.execute("SET max_temp_directory_size = ?", [f"{max_bytes}B"])
            with pa.RecordBatchReader.from_batches(result_schema(projection), inputs()) as source:
                db.register("actual", source)
                # No predicate, projection, aggregation, Delta extension or reference query.
                query = "SELECT * FROM actual ORDER BY " + ", ".join(keys)
                try:
                    result = db.execute(query).to_arrow_reader(BATCH_ROWS)
                except duckdb.Error as error:
                    raise ValueError(f"oracle sort failed: {error}") from error
                with result:
                    actual = ordered_batches(normalized_batches(result, projection), projection)
                    wanted = ordered_batches(normalized_batches(expected.iter_batches(batch_size=BATCH_ROWS), projection), projection)
                    if subset:
                        # LIMIT accepts different valid subsets, with exact values and unique keys.
                        reference_rows = (row for batch in wanted for row in batch.to_pylist())
                        current = next(reference_rows, None)
                        count = 0
                        for batch in actual:
                            for row in batch.to_pylist():
                                key = tuple(row[k] for k in keys)
                                while current is not None and tuple(current[k] for k in keys) < key:
                                    current = next(reference_rows, None)
                                require(current == row, f"wrong row membership or value at key {key}")
                                count += 1
                    else:
                        try:
                            count = same_rows(actual, wanted)
                        except ValueError as error:
                            raise ValueError(f"wrong row membership or value: {error}") from error
    require(input_count == count == expected_count,
            f"wrong row count: input {input_count}, sorted {count}, expected {expected_count}")
    return count


def check_reference_build(metadata):
    require(metadata["format"] == "selective-read-reference-v2" and metadata["status"] == "complete", "incomplete reference")
    if metadata["comparison_revision"] in (5, 6):
        import production_workloads
        require(metadata["production_oracle_sha256"] == digest_file(production_workloads.__file__), "stale production oracle")
    require(metadata["oracle_sha256"] == digest_file(__file__)
            and metadata["oracle_dependencies_sha256"] == digest_file(REQUIREMENTS)
            and metadata["pyarrow"] == pa.__version__ == "25.0.1"
            and metadata["duckdb_sort"] == duckdb.__version__ == "1.5.5", "stale oracle build")


def objects(fixtures, table, source):
    verified = []
    for group in ([source] if source is not None else []) + [table]:
        logs = group.get("delta_logs", [group["delta_log"]] if group["delta_log"] else [])
        dvs = [f["deletion_vector"] for f in group["files"] if "deletion_vector" in f]
        geometries = [f["geometry"] for f in group["files"] if "geometry" in f]
        for item in group["files"] + logs + dvs + geometries:
            descriptor = {k: item[k] for k in ("path", "bytes", "sha256")}
            descriptor["path"] = str(Path(group["path"]) / item["path"])
            verify_object(fixtures, descriptor)
            verified.append(descriptor)
        for item in group["files"]:
            if "geometry" in item:
                require(item["geometry"]["path"] == item["path"] + ".geometry.json", "invalid geometry sidecar path")
                details = load_json(inside(fixtures, str(Path(group["path"]) / item["geometry"]["path"])))
                require(details["parquet_sha256"] == item["sha256"]
                        and [{k: g[k] for k in ("first_row", "rows")} for g in details["row_groups"]] == item["row_groups"]
                        and all([c["column"] for c in g["columns"]] == [f["name"] for f in group["schema"]["fields"]]
                                for g in details["row_groups"]), "geometry sidecar differs from Parquet identity or groups")
    logs = table.get("delta_logs", [table["delta_log"]])
    require([item["path"] for item in logs] == [f"_delta_log/{v:020}.json" for v in range(table["snapshot_version"] + 1)]
            and logs[-1] == table["delta_log"], "wrong snapshot logs")
    by_path, metadata = {}, None
    for version, log in enumerate(logs):
        path = inside(fixtures, str(Path(table["path"]) / log["path"]))
        actions = [json.loads(line, parse_float=Decimal) for line in path.read_text().splitlines()]
        protocol = {"minReaderVersion": 1, "minWriterVersion": 2} if version == 0 else {
            "minReaderVersion": 3, "minWriterVersion": 7, "readerFeatures": ["deletionVectors"],
            "writerFeatures": ["deletionVectors", "invariants", "appendOnly"]}
        require([a["protocol"] for a in actions if "protocol" in a] == [protocol], "unexpected Delta protocol")
        for action in actions:
            if "metaData" in action:
                current = action["metaData"]
                if version:
                    require(metadata is not None and current == metadata | {"configuration": metadata["configuration"] | {"delta.enableDeletionVectors": "true"}}, "DV metadata changed unrelated fields")
                metadata = current
            if "remove" in action:
                removed = action["remove"]
                require(version == 1 and removed["path"] in by_path and not removed.get("deletionVector"), "unexpected removed file identity")
                del by_path[removed["path"]]
            if "add" in action:
                added = action["add"]
                require(added["path"] not in by_path and (version or not added.get("deletionVector")), "duplicate Add or DV in base snapshot")
                by_path[added["path"]] = added
    require(set(by_path) == {f["path"] for f in table["files"]}, "Delta file inventory differs from manifest")
    if table["snapshot_version"]:
        profile = load_json(Path(fixtures) / 'manifest.json').get('profile', 'large')
        if profile == "large":
            profile = f"large-sf{int(table['scale_factor'])}"
        table_uuid = uuid.uuid5(uuid.NAMESPACE_URL, f"https://github.com/mag1cfrog/delta-arrow-reader/selective-read-v1/{profile}/{table['id']}")
        require(metadata is not None and metadata["id"] == str(table_uuid)
                and json.loads(metadata["schemaString"]) == table["schema"]
                and metadata["configuration"]["delta.enableDeletionVectors"] == "true", "invalid DV table metadata")
    for item in table["files"]:
        action = by_path[item["path"]]
        dv = item.get("deletion_vector")
        require(action.get("deletionVector") == (dv["descriptor"] if dv else None) and not action.get("partitionValues"), "unexpected DV or partitions")
        require(action["size"] == item["bytes"] and json.loads(action["stats"], parse_float=Decimal) == item["delta_stats"], "Delta Add differs from manifest")
        require(item["delta_stats"]["numRecords"] == item["rows"], "numRecords must retain physical rows")
        if dv:
            descriptor = dv["descriptor"]
            ordinals = dv["physical_ordinals"]
            require(table["deletion_vectors"] and item["delta_stats"]["tightBounds"] is False
                    and ordinals and all(type(n) is int and 0 <= n < item["rows"] for n in ordinals)
                    and ordinals == sorted(set(ordinals)) and len(ordinals) == len(dv["logical_ids"]) == descriptor["cardinality"], "invalid deletion coordinates")
            dv_uuid = uuid.uuid5(table_uuid, item["path"])
            require(dv["path"] == f"deletion_vector_{dv_uuid}.bin" and descriptor["storageType"] == "u"
                    and descriptor["pathOrInlineDv"] == base64.z85encode(dv_uuid.bytes).decode()
                    and descriptor["offset"] == 1, "invalid deterministic DV path/offset")
            data = inside(fixtures, str(Path(table["path"]) / dv["path"])).read_bytes()
            require(data[0] == 1 and len(data) == descriptor["sizeInBytes"] + 9
                    and int.from_bytes(data[1:5], "big") == descriptor["sizeInBytes"]
                    and int.from_bytes(data[5:9], "little") == 1681511377
                    and zlib.crc32(data[5:-4]) == int.from_bytes(data[-4:], "big"), "invalid DV envelope/checksum")
    return verified


def deleted(row):
    if "row_id" in row:
        return row["event_id"] == "other" and row["row_id"] % 1000 == 0
    value = hashlib.sha256(f"{row['l_orderkey']}/{row['l_linenumber']}".encode()).digest()
    return int.from_bytes(value[:8], "little") % 1000 == 0


def control_matches(case_id, row_id):
    if case_id == "row-groups.select":
        return row_id // 4096 % 16 == 7
    if case_id == "pages.localized":
        return row_id % 4096 < 32
    require(case_id == "pages.scattered", "unknown control layout")
    return row_id % 128 == 0


def control_record(row_id):
    values = [row_id] + [None if (row_id + j) % 17 == 0 else
                        f"payload-{j:03}-{row_id:08}-" + "abcdefghijklmnopqrstuvwxyz0123456789" * 12
                        for j in range(16)]
    return dict(zip(CONTROL_PROJECTION, values))


def control_geometry(fixtures, table, case_id):
    """Check upper-level survival and actual footer/index geometry before timing."""
    row_groups = case_id == "row-groups.select"
    files, groups = (16, 16) if row_groups else (1, 2)
    require(table["file_count"] == len(table["files"]) == files and table["rows"] == files * groups * 4096, "wrong control dimensions")
    candidate_groups = candidate_pages = pages = 0
    for item in table["files"]:
        stats = item["delta_stats"]
        require(stats["numRecords"] == item["rows"] == groups * 4096 and stats["nullCount"]["event_id"] == 0
                and stats["minValues"]["event_id"] == "match" and stats["maxValues"]["event_id"] == "other", "control file can be pruned")
        footer = pq.ParquetFile(inside(fixtures, str(Path(table["path"]) / item["path"]))).metadata
        require(footer.num_row_groups == len(item["row_groups"]) == groups, "wrong control row groups")
        for g, group in enumerate(item["row_groups"]):
            require(group["first_row"] == g * 4096 and group["rows"] == footer.row_group(g).num_rows == 4096, "wrong control group boundary")
            low = "match" if not row_groups or g == 7 else "other"
            high = low if row_groups else "other"
            expected = {"min_hex": low.encode().hex(), "max_hex": high.encode().hex()}
            candidate_groups += low == "match"
            require([c["column"] for c in group["columns"]] == ["row_id", "event_id", *CONTROL_PAYLOADS], "predicate/payload columns changed")
            for c, column in enumerate(group["columns"]):
                actual = footer.row_group(g).column(c)
                require(actual.has_column_index and actual.has_offset_index and column["column_index"]["length"] > 0
                        and column["offset_index"]["length"] > 0, "control index missing")
                if not row_groups:
                    require(actual.compression == column["compression"] == "UNCOMPRESSED" and actual.dictionary_page_offset is None, "page writer settings changed")
                    require([(p["first_row"], p["rows"]) for p in column["pages"]] == [(p * 128, 128) for p in range(32)], "wrong 128-row page boundaries")
                if c != 1:
                    continue
                require(actual.statistics.min == low and actual.statistics.max == high and actual.statistics.null_count == 0
                        and column["statistics"] == {"nulls": 0, **expected}, "unexpected predicate group pruning")
                for page in column["pages"]:
                    page_low = low if row_groups else "match" if case_id == "pages.scattered" or page["first_row"] == 0 else "other"
                    require(page["bounds"] == {"min_hex": page_low.encode().hex(), "max_hex": high.encode().hex()}
                            and page["nulls"] == 0 and page["all_null"] is False, "unexpected predicate page pruning")
                    pages += 1
                    candidate_pages += page_low == "match"
    return {"row_groups": files * groups, "candidate_row_groups": candidate_groups,
            "predicate_pages": pages, "candidate_predicate_pages": candidate_pages,
            "predicate_columns": ["event_id"], "output_columns": list(CONTROL_PROJECTION),
            "decoded_row_groups": None, "decoded_pages": None,
            "counter_unavailable_reason": "geometry describes opportunities; common adapters do not expose comparable decoded group/page counters"}


@contextmanager
def bounded(root, limits):
    """Native process deadline and storage quotas for explicit large workloads."""
    if limits is None:
        yield None
        return
    require(sys.platform == "linux", "bounded large oracle requires Linux")
    root = Path(root).resolve()
    root.mkdir(parents=True, exist_ok=True)
    require(shutil.disk_usage(root).free >= limits["disk_bytes"] + 512 * 1024**2,
            "oracle disk allowance exceeds available space")
    old_memory = resource.getrlimit(resource.RLIMIT_AS)
    old_handler = signal.getsignal(signal.SIGALRM)
    require(signal.getitimer(signal.ITIMER_REAL) == (0.0, 0.0), "oracle cannot replace another process deadline")
    memory = min(limits["memory_bytes"], old_memory[0]) if old_memory[0] != resource.RLIM_INFINITY else limits["memory_bytes"]
    resource.setrlimit(resource.RLIMIT_AS, (memory, old_memory[1]))
    signal.signal(signal.SIGALRM, signal.SIG_DFL)
    signal.alarm(limits["elapsed_seconds"])
    try:
        yield limits["disk_bytes"] - 8 * 1024**2  # Completion metadata allowance.
    finally:
        signal.alarm(0)
        signal.signal(signal.SIGALRM, old_handler)
        resource.setrlimit(resource.RLIMIT_AS, old_memory)


def prepare(fixtures, case_id, output, duplicate_literal=False, workload=None):
    limits = None
    if workload is not None:
        import large_workloads
        limits = large_workloads.load(workload)["oracle_limits"]
    with bounded(Path(output).parent, limits) as quota:
        if workload is not None and large_workloads.identity(workload)["comparison_revision"] in (5, 6):
            require(not duplicate_literal, "production variants need a new workload identity")
            import production_workloads
            return production_workloads.prepare_reference(fixtures, case_id, output, workload, quota)
        return prepare_rows(fixtures, case_id, output, duplicate_literal, workload, quota)


def prepare_rows(fixtures, case_id, output, duplicate_literal, workload, quota):
    fixtures, output = Path(fixtures).resolve(), Path(output)
    manifest, table, source, projection, predicate, limit, literals, sql = case_input(fixtures, case_id, duplicate_literal, workload)
    verified = objects(fixtures, table, source)
    query_case = case_id
    if workload is not None:
        import large_workloads
        query_case = large_workloads.binding(large_workloads.load(workload), fixtures, case_id)["query_case_id"]
    case, suffix = base_case(query_case, workload is not None)
    geometry = control_geometry(fixtures, table, case) if source is None else None
    saved_keys = [tuple(key) for item in table["files"] for key in item.get("deletion_vector", {}).get("logical_ids", [])]
    require(len(saved_keys) == len(set(saved_keys)), "duplicate deleted logical ID")
    saved_keys = set(saved_keys)
    extra = set()
    if "wide_file_pair" in manifest:
        import wide_files
        extra = wide_files.extra_keys(fixtures, manifest)
    expected_deletions = set()
    deleted_qualifying = 0
    output.mkdir(parents=True, exist_ok=False)
    reference_path = output / "reference.parquet"
    predicates = conditions(predicate, literals)
    counts = [0] * (len(predicates) + 1)
    found_literals = set()
    previous_key = None
    projected_bytes = [0]

    def expected_rows():
        nonlocal previous_key, deleted_qualifying
        if source is None:
            counts[0] = table["rows"]
            for row_id in range(table["rows"]):
                matches = control_matches(case, row_id)
                if table["deletion_vectors"] and deleted({"row_id": row_id, "event_id": "match" if matches else "other"}):
                    expected_deletions.add((row_id,))
                if matches:
                    counts[-1] += 1
                    yield control_record(row_id)
            require(counts[-1] == (65536 if case == "row-groups.select" else 64), "wrong qualifying control geometry")
            return
        for item in source["files"]:
            path = inside(fixtures, str(Path(source["path"]) / item["path"]))
            file_rows = 0
            for row in full_rows(path, ORIGINAL):
                file_rows += 1
                key = tuple(row[name] for name in KEYS)
                require(previous_key is None or previous_key < key, "source keys are duplicated or out of generator order")
                previous_key = key
                if row["l_shipdate"] == date(1995, 3, 15) and row["l_shipmode"] == "AIR":
                    found_literals.add(row["l_partkey"])
                    if len(found_literals) > 20:
                        found_literals.remove(max(found_literals))
                passed = prefix_matches(row, predicates)
                for index in range(passed + 1):
                    counts[index] += 1
                if table["deletion_vectors"] and (deleted(row) or key in extra):
                    expected_deletions.add(key)
                    deleted_qualifying += passed == len(predicates)
                    continue
                if passed == len(predicates):
                    yield record(row, projection, derive_payloads=True, logical_bytes=projected_bytes)
            require(file_rows == item["rows"], "reference file row count changed")

    write_reference(reference_path, expected_rows(), projection, quota // 4 if quota is not None else None)
    require(counts[0] == (source or table)["rows"] == table["rows"], "source/table row counts disagree")
    require(sorted(found_literals) == literals, "frozen IN literals disagree with full source")
    require(expected_deletions == saved_keys, "saved logical deletions disagree with the independent source rule")
    live_qualifying = counts[-1] - deleted_qualifying
    if workload is not None and table["deletion_vectors"]:
        require(deleted_qualifying > 0 and live_qualifying > 0, "large DV anchor needs qualifying deletions and survivors")
    if suffix:
        summary = table["deletion_summary"]
        affected = sum("deletion_vector" in item for item in table["files"])
        require(summary["physical_rows"] == counts[0] and summary["deleted_rows"] == len(saved_keys)
                and summary["live_rows"] == counts[0] - len(saved_keys) and summary["dv_files"] == affected
                and abs(float(summary["density"]) - len(saved_keys) / counts[0]) < 1e-15
                and abs(float(summary["file_coverage"]) - affected / len(table["files"])) < 1e-15, "wrong deletion summary")

    matching, candidates = [], []
    actual_rows = 0

    def fixture_rows():
        nonlocal actual_rows
        for item in table["files"]:
            path = inside(fixtures, str(Path(table["path"]) / item["path"]))
            if candidate(item["delta_stats"], predicates):
                candidates.append(item["path"])
            matched, count = False, 0
            dv = item.get("deletion_vector", {})
            by_ordinal = dict(zip(dv.get("physical_ordinals", []), map(tuple, dv.get("logical_ids", []))))
            columns = ("row_id", "event_id") + CONTROL_PAYLOADS if source is None else ORIGINAL + PAYLOADS if table["id"].startswith("wide.") else ORIGINAL
            for row in full_rows(path, columns):
                if source is None:
                    require(row["row_id"] == actual_rows + count and row["event_id"] == ("match" if control_matches(case, row["row_id"]) else "other"), "control row order or match placement changed")
                is_deleted = count in by_ordinal
                require(is_deleted == (table["deletion_vectors"] and (deleted(row) or bool(extra) and tuple(row[name] for name in KEYS) in extra)), "physical deletion ordinal differs from rule")
                if is_deleted:
                    key = (row["row_id"],) if source is None else tuple(row[name] for name in KEYS)
                    require(by_ordinal[count] == key, "physical ordinal refers to the wrong logical row")
                count += 1
                if is_deleted:
                    continue
                if prefix_matches(row, predicates) == len(predicates):
                    matched = True
                    yield record(row, projection)
            require(count == item["rows"], "fixture file row count changed")
            actual_rows += count
            if matched:
                matching.append(item["path"])

    # Validate all qualifying fixture rows even for a LIMIT case.
    compare(row_batches(fixture_rows(), projection), reference_path, projection, live_qualifying,
            quota // 4 if quota is not None else None)
    require(actual_rows == (source or table)["rows"], "fixture lost source rows")
    require(set(matching) <= set(candidates), "file statistics exclude a matching file")
    metadata = {
        "format": "selective-read-reference-v2", "status": "complete",
        "comparison_revision": REVISION, "protocol_sha256": digest_file(PROTOCOL),
        "oracle_sha256": digest_file(__file__), "python": platform.python_version(),
        "pyarrow": pa.__version__, "duckdb_sort": duckdb.__version__,
        "oracle_dependencies_sha256": digest_file(REQUIREMENTS),
        "reference_storage": {"format": "parquet", "compression": "zstd", "row_group_rows": REFERENCE_ROWS},
        "sort_memory_bytes": SORT_MEMORY_BYTES, "sort_threads": 1,
        "fixture_manifest_sha256": digest_file(fixtures / "manifest.json"),
        "generator_protocol_sha256": manifest["protocol_sha256"],
        "case_id": case_id, "snapshot_version": table["snapshot_version"],
        "profile": manifest["profile"], "query_variant": "duplicate-in" if duplicate_literal else "canonical",
        "canonical_sql": sql, "canonical_sql_sha256": digest_bytes(sql.encode()),
        "in_literals": literals, "projection": list(projection), "limit": limit,
        "source_rows": counts[0], "qualifying_rows": live_qualifying,
        "physical_qualifying_rows": counts[-1], "deleted_qualifying_rows": deleted_qualifying,
        "deleted_rows": len(saved_keys), "live_rows": counts[0] - len(saved_keys),
        "output_rows": min(limit, live_qualifying) if limit is not None else live_qualifying,
        "predicate_step_population": "physical_rows_before_deletions",
        "predicate_steps": [{"predicate": f"{name} {op} {sorted(value) if op == 'IN' else value}",
                             "rows": count, "selectivity": count / counts[0]}
                            for (name, op, value), count in zip(predicates, counts[1:])],
        "qualifying_selectivity": live_qualifying / counts[0],
        "active_files": len(table["files"]), "candidate_files": candidates, "matching_files": matching,
        "verified_objects": verified, "reference_sha256": digest_file(reference_path),
        **({"within_file_geometry": geometry} if geometry else {}),
    }
    if "wide_file_pair" in manifest:
        require(live_qualifying > 0 and (not table["deletion_vectors"] or deleted_qualifying > 0), "wide pair needs a deleted match and a survivor")
        metadata["wide_file_geometry"] = {"file_bytes_sorted": sorted(f["bytes"] for f in table["files"]),
            "excluded_files": sorted({f["path"] for f in table["files"]} - set(candidates)),
            "dv_coverage": wide_files.coverage(table, candidates, matching)}
    if workload is not None:
        import large_workloads
        frozen = large_workloads.load(workload)
        row = large_workloads.binding(frozen, fixtures, case_id)
        metadata.update(large_workloads.identity(workload), workload_manifest=str(Path(workload).resolve()),
                        oracle_limits=frozen["oracle_limits"], native_expression_sha256=row["native_expression_sha256"],
                        projected_logical_bytes=projected_bytes[0],
                        projected_logical_bytes_definition="non-null fixed-width values and actual UTF-8 string bytes; excludes null bitmaps, offsets and IPC framing")
    # Completion marker last. Partial preparations cannot be accepted by check().
    (output / "reference.json").write_bytes(json_bytes(metadata))
    return metadata


IDENTITY_FIELDS = ("comparison_revision", "protocol_sha256", "fixture_manifest_sha256",
                   "case_id", "snapshot_version", "canonical_sql_sha256")


def check(reference, fixtures, result, identity):
    metadata = load_json(Path(reference) / "reference.json")
    limits = metadata.get("oracle_limits") if metadata.get("comparison_revision") in (3, 4, 5, 6) else None
    if metadata.get("comparison_revision") in (3, 4, 5, 6):
        import large_workloads
        frozen = large_workloads.load(Path(metadata["workload_manifest"]))
        require(limits == frozen["oracle_limits"], "oracle limits differ from the workload")
    with bounded(Path(reference).parent, limits) as quota:
        return check_rows(reference, fixtures, result, identity, quota)


def check_rows(reference, fixtures, result, identity, quota):
    reference, fixtures, result, identity = map(Path, (reference, fixtures, result, identity))
    metadata = load_json(reference / "reference.json")
    check_reference_build(metadata)
    sys.path.insert(0, str(Path(__file__).resolve().parent / "runners"))
    from run import comparison_identity, reader_roster
    comparison = comparison_identity(metadata)
    if metadata["comparison_revision"] in (3, 4, 5, 6):
        import large_workloads
        path = Path(metadata["workload_manifest"])
        require(comparison == large_workloads.identity(path), "stale workload manifest")
        row = large_workloads.binding(large_workloads.load(path), fixtures, metadata["case_id"])
        require(metadata["canonical_sql"] == row["canonical_sql"] and metadata["projection"] == row["projection"], "reference workload query changed")
    require(metadata["fixture_manifest_sha256"] == digest_file(fixtures / "manifest.json"), "stale fixture manifest")
    require(metadata["reference_sha256"] == digest_file(reference / "reference.parquet"), "reference Parquet changed")
    for item in metadata["verified_objects"]:
        verify_object(fixtures, item)
    provenance = load_json(identity)
    require(provenance["reader_id"] in (set(reader_roster(metadata)) if metadata["comparison_revision"] == 6 else READERS), "unknown reader identity")
    identity_fields = tuple(dict.fromkeys((*IDENTITY_FIELDS, *comparison)))
    for name in identity_fields:
        require(type(provenance[name]) is type(metadata[name]) and provenance[name] == metadata[name], f"result identity mismatch: {name}")
    for name in ("reader_build_sha256", "reader_config_sha256", "result_sha256"):
        require(re.fullmatch(r"[0-9a-f]{64}", provenance[name]) is not None, f"invalid {name}")
    expression = provenance["native_expression_sha256"]
    require(expression is None or re.fullmatch(r"[0-9a-f]{64}", expression) is not None, "invalid native expression hash")
    require(provenance["reader_id"] not in ("polars", "daft") or expression is not None, "missing native expression identity")
    if metadata["comparison_revision"] in (3, 4, 5, 6):
        require(expression == row["native_expression_sha256"].get(provenance["reader_id"]), "native translation differs from workload")
    require(digest_file(result) == provenance["result_sha256"], "result checksum mismatch")
    projection = metadata["projection"]
    if quota is not None:
        exports = list(result.parent.glob("query-*.arrow")) if re.fullmatch(r"query-\d+\.arrow", result.name) else [result]
        require((reference / "reference.parquet").stat().st_size <= quota // 4
                and sum(p.stat().st_size for p in exports) <= quota // 2,
                "reference/session exports exhaust the oracle disk allowance")
        quota //= 4
    with result.open("rb") as source, pa.ipc.open_stream(source) as stream:
        check_schema(stream.schema, projection)
        reported_nullability = {field.name: field.nullable for field in stream.schema}
        count = compare(stream, reference / "reference.parquet", projection, metadata["output_rows"],
                        quota, subset=metadata["limit"] is not None)
        require(not source.read(1), "trailing bytes after Arrow stream")
    return {
        "status": "passed", **{name: metadata[name] for name in identity_fields},
        **{name: provenance[name] for name in ("reader_id", "reader_build_sha256", "reader_config_sha256", "native_expression_sha256", "result_sha256")},
        "oracle_sha256": metadata["oracle_sha256"], "reference_sha256": metadata["reference_sha256"],
        "identity_sha256": digest_file(identity), "output_rows": count,
        "qualifying_rows": metadata["qualifying_rows"], "reported_nullability": reported_nullability,
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    prepare_parser = commands.add_parser("prepare", help="full-read a fixture and save an independent reference")
    prepare_parser.add_argument("--fixtures", type=Path, required=True)
    prepare_parser.add_argument("--case", action="append", required=True,
                                help="repeat to prepare cases together and reuse unchanged inputs")
    prepare_parser.add_argument("--output", type=Path, required=True)
    prepare_parser.add_argument("--duplicate-in-literal", action="store_true")
    prepare_parser.add_argument("--workload", type=Path)
    check_parser = commands.add_parser("check", help="validate an untimed Arrow IPC stream and identity JSON")
    for name in ("reference", "fixtures", "result", "identity"):
        check_parser.add_argument(f"--{name}", type=Path, required=True)
    args = parser.parse_args()
    try:
        if args.command == "prepare":
            require(len(set(args.case)) == len(args.case), "duplicate preparation case")
            if len(args.case) == 1:
                output = prepare(args.fixtures, args.case[0], args.output, args.duplicate_in_literal, args.workload)
            else:
                require(all(re.fullmatch(r"[a-zA-Z0-9_.-]+", case) and case not in (".", "..") for case in args.case),
                        "invalid batch preparation case")
                args.output.mkdir()
                output = [prepare(args.fixtures, case, args.output / case, args.duplicate_in_literal, args.workload)
                          for case in args.case]
        else:
            output = check(args.reference, args.fixtures, args.result, args.identity)
    except (ValueError, KeyError, TypeError, StopIteration, OSError, duckdb.Error, pa.ArrowException) as error:
        print(json.dumps({"status": "validation_failed", "error": str(error), "operation": args.command}))
        return 1
    print(json.dumps(output))
    return 0


if __name__ == "__main__":
    sys.exit(main())

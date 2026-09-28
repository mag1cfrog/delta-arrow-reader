"""Independent, untimed reference for the public selective-read cases.

PyArrow only decodes full Parquet/IPC batches. Python evaluates predicates and
payloads; disk-backed SQLite orders exact rows and checks multiplicity.
"""

import argparse
from contextlib import closing
from datetime import date
from decimal import Decimal
import hashlib
import json
from pathlib import Path
import platform
import re
import sqlite3
import sys
import tempfile

import pyarrow as pa
import pyarrow.parquet as pq


PROTOCOL = Path(__file__).resolve().parents[2] / "docs/content/benchmarks/selective-read-protocol.md"
REVISION = 2
BATCH_ROWS = 8192
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
TYPES = dict(FIELDS) | dict.fromkeys(PAYLOADS, "int64")
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


def digest_file(path):
    with Path(path).open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


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
            require(all(row[name] is not None for name in ORIGINAL), "null in original non-null field")
            yield row


def payload(order, line, column):
    raw = hashlib.sha256(f"dar-wide-v1/{order}/{line}/{column:02}".encode()).digest()
    value = int.from_bytes(raw[:8], "little")
    return None if value % 17 == 0 else (value & 0x7FFFFFFFFFFFFFFF) - 0x4000000000000000


def conditions(predicate, literals):
    if predicate == "all":
        return []
    if predicate == "empty":
        return [("l_shipdate", "<", date(1990, 1, 1))]
    if predicate == "date7":
        return [("l_shipdate", ">=", date(1995, 3, 15)), ("l_shipdate", "<", date(1995, 3, 22))]
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


def case_input(fixtures, case_id, duplicate_literal=False):
    require(pa.__version__ == "25.0.1", "oracle requires pyarrow==25.0.1")
    manifest = load_json(Path(fixtures) / "manifest.json")
    require(manifest["status"] == "complete" and manifest["protocol"] == "selective-read-v1", "incomplete or unknown fixtures")
    parts = case_id.split(".")
    require(len(parts) == 3 and parts[0] in ("li", "wide") and parts[1] in ("clustered", "shuffled"), "unsupported case; controls and DVs follow in later slices")
    fixture_id, query = ".".join(parts[:2]), parts[2]
    table = next(t for t in manifest["tables"] if t["id"] == fixture_id)
    require(type(table["snapshot_version"]) is int and table["snapshot_version"] == 0 and table["deletion_vectors"] is False, "oracle slice requires no-DV snapshot 0")
    source = next(s for s in manifest["sources"] if s["scale_factor"] == table["scale_factor"])
    shapes = WIDE_CASES if parts[0] == "wide" else ORIGINAL_CASES
    require(query in shapes, "unknown public query")
    projection, predicate, limit = shapes[query]
    literals = source["in_literals"]
    require(literals and all(type(v) is int for v in literals) and literals == sorted(set(literals)), "invalid frozen IN literals")
    require(len(literals) == 20 or (manifest["profile"] == "smoke" and len(literals) < 20), "wrong frozen literal count")
    canonical = sql_for(projection, predicate, limit, literals)
    require(canonical == source["wide_queries" if parts[0] == "wide" else "queries"][query], "manifest SQL differs from frozen case")
    if duplicate_literal:
        require(predicate in ("eq2-in1", "eq2-in20"), "duplicate literal requires an IN case")
        selected = literals[:1] if predicate == "eq2-in1" else literals
        canonical = canonical.rsplit(")", 1)[0] + f", {selected[0]})"
    return manifest, table, source, projection, predicate, limit, literals, canonical


def record(row, projection, derive_payloads=False):
    values = []
    for name in projection:
        if derive_payloads and name.startswith("payload_"):
            value = payload(row[KEYS[0]], row[KEYS[1]], int(name[-2:]))
        else:
            value = row[name]
        require(value is not None or name in PAYLOADS, f"unexpected null: {name}")
        if isinstance(value, Decimal):
            value = format(value, ".2f")
        elif isinstance(value, date):
            value = value.isoformat()
        values.append(value)
    return row[KEYS[0]], row[KEYS[1]], json_bytes(values)


def database(path):
    db = sqlite3.connect(path, uri=True)
    db.execute("PRAGMA cache_size=-65536")
    db.execute("PRAGMA mmap_size=0")
    db.execute("PRAGMA temp_store=FILE")
    # These databases are disposable until their completion manifest is written.
    db.execute("PRAGMA journal_mode=OFF")
    db.execute("CREATE TABLE rows (order_key INTEGER NOT NULL, line_number INTEGER NOT NULL, value BLOB NOT NULL, PRIMARY KEY (order_key, line_number)) WITHOUT ROWID")
    return db


def insert_rows(db, rows):
    # Consume a bounded batch at a time. The primary key rejects duplicate output.
    batch = []
    for row in rows:
        batch.append(row)
        if len(batch) == BATCH_ROWS:
            db.executemany("INSERT INTO rows VALUES (?, ?, ?)", batch)
            batch.clear()
    db.executemany("INSERT INTO rows VALUES (?, ?, ?)", batch)
    db.commit()


def compare(db, reference_db, expected_count):
    db.execute("ATTACH DATABASE ? AS expected", (reference_db.resolve().as_uri() + "?mode=ro",))
    count = db.execute("SELECT count(*) FROM rows").fetchone()[0]
    require(count == expected_count, f"wrong row count: {count}, expected {expected_count}")
    mismatch = db.execute("""SELECT a.order_key, a.line_number FROM rows a
        LEFT JOIN expected.rows e USING (order_key, line_number)
        WHERE e.order_key IS NULL OR a.value != e.value LIMIT 1""").fetchone()
    require(mismatch is None, f"wrong row membership or value at key {mismatch}")
    return count


def objects(fixtures, table, source):
    verified = []
    for group in (source, table):
        for item in group["files"] + ([group["delta_log"]] if group["delta_log"] else []):
            descriptor = {k: item[k] for k in ("path", "bytes", "sha256")}
            descriptor["path"] = str(Path(group["path"]) / item["path"])
            verify_object(fixtures, descriptor)
            verified.append(descriptor)
    require(table["delta_log"]["path"] == "_delta_log/00000000000000000000.json", "wrong snapshot log")
    log = inside(fixtures, str(Path(table["path"]) / table["delta_log"]["path"]))
    actions = [json.loads(line, parse_float=Decimal) for line in log.read_text().splitlines()]
    protocols = [action["protocol"] for action in actions if "protocol" in action]
    require(protocols == [{"minReaderVersion": 1, "minWriterVersion": 2}], "unexpected Delta protocol")
    adds = [action["add"] for action in actions if "add" in action]
    by_path = {action["path"]: action for action in adds}
    require(len(adds) == len(by_path) == len(table["files"]), "Delta file inventory differs from manifest")
    require(not any("remove" in action for action in actions), "unexpected remove in base snapshot")
    for item in table["files"]:
        action = by_path[item["path"]]
        require(action.get("deletionVector") is None and not action.get("partitionValues"), "unexpected DV or partitions")
        require(action["size"] == item["bytes"] and json.loads(action["stats"], parse_float=Decimal) == item["delta_stats"], "Delta Add differs from manifest")
    return verified


def prepare(fixtures, case_id, output, duplicate_literal=False):
    fixtures, output = Path(fixtures).resolve(), Path(output)
    manifest, table, source, projection, predicate, limit, literals, sql = case_input(fixtures, case_id, duplicate_literal)
    verified = objects(fixtures, table, source)
    output.mkdir(parents=True, exist_ok=False)
    reference_db = output / "reference.sqlite"
    predicates = conditions(predicate, literals)
    counts = [0] * (len(predicates) + 1)
    found_literals = set()
    previous_key = None

    def expected_rows():
        nonlocal previous_key
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
                if passed == len(predicates):
                    yield record(row, projection, derive_payloads=True)
            require(file_rows == item["rows"], "reference file row count changed")

    with closing(database(reference_db)) as db:
        insert_rows(db, expected_rows())
    require(counts[0] == source["rows"] == table["rows"], "source/table row counts disagree")
    require(sorted(found_literals) == literals, "frozen IN literals disagree with full source")

    matching, candidates = [], []
    actual_rows = 0

    def fixture_rows():
        nonlocal actual_rows
        for item in table["files"]:
            path = inside(fixtures, str(Path(table["path"]) / item["path"]))
            if candidate(item["delta_stats"], predicates):
                candidates.append(item["path"])
            matched, count = False, 0
            for row in full_rows(path, ORIGINAL + PAYLOADS if table["id"].startswith("wide.") else ORIGINAL):
                count += 1
                if prefix_matches(row, predicates) == len(predicates):
                    matched = True
                    yield record(row, projection)
            require(count == item["rows"], "fixture file row count changed")
            actual_rows += count
            if matched:
                matching.append(item["path"])

    with tempfile.TemporaryDirectory(prefix="oracle-", dir=output) as scratch:
        with closing(database(Path(scratch) / "actual.sqlite")) as db:
            insert_rows(db, fixture_rows())
            # Validate all qualifying fixture rows even for a LIMIT case.
            compare(db, reference_db, counts[-1])
    require(actual_rows == source["rows"], "fixture lost source rows")
    require(set(matching) <= set(candidates), "file statistics exclude a matching file")
    metadata = {
        "format": "selective-read-reference-v1", "status": "complete",
        "comparison_revision": REVISION, "protocol_sha256": digest_file(PROTOCOL),
        "oracle_sha256": digest_file(__file__), "python": platform.python_version(),
        "pyarrow": pa.__version__, "sqlite": sqlite3.sqlite_version,
        "fixture_manifest_sha256": digest_file(fixtures / "manifest.json"),
        "generator_protocol_sha256": manifest["protocol_sha256"],
        "case_id": case_id, "snapshot_version": table["snapshot_version"],
        "profile": manifest["profile"], "query_variant": "duplicate-in" if duplicate_literal else "canonical",
        "canonical_sql": sql, "canonical_sql_sha256": digest_bytes(sql.encode()),
        "in_literals": literals, "projection": list(projection), "limit": limit,
        "source_rows": counts[0], "qualifying_rows": counts[-1],
        "output_rows": min(limit, counts[-1]) if limit is not None else counts[-1],
        "predicate_steps": [{"predicate": f"{name} {op} {sorted(value) if op == 'IN' else value}",
                             "rows": count, "selectivity": count / counts[0]}
                            for (name, op, value), count in zip(predicates, counts[1:])],
        "qualifying_selectivity": counts[-1] / counts[0],
        "active_files": len(table["files"]), "candidate_files": candidates, "matching_files": matching,
        "verified_objects": verified, "reference_sha256": digest_file(reference_db),
    }
    # Completion marker last. Partial preparations cannot be accepted by check().
    (output / "reference.json").write_bytes(json_bytes(metadata))
    return metadata


IDENTITY_FIELDS = ("comparison_revision", "protocol_sha256", "fixture_manifest_sha256",
                   "case_id", "snapshot_version", "canonical_sql_sha256")


def check(reference, fixtures, result, identity):
    reference, fixtures, result, identity = map(Path, (reference, fixtures, result, identity))
    metadata = load_json(reference / "reference.json")
    require(metadata["format"] == "selective-read-reference-v1" and metadata["status"] == "complete", "incomplete reference")
    require(metadata["comparison_revision"] == REVISION and metadata["protocol_sha256"] == digest_file(PROTOCOL), "stale comparison protocol")
    require(metadata["oracle_sha256"] == digest_file(__file__) and metadata["pyarrow"] == pa.__version__ == "25.0.1", "stale oracle build")
    require(metadata["fixture_manifest_sha256"] == digest_file(fixtures / "manifest.json"), "stale fixture manifest")
    require(metadata["reference_sha256"] == digest_file(reference / "reference.sqlite"), "reference database changed")
    for item in metadata["verified_objects"]:
        verify_object(fixtures, item)
    provenance = load_json(identity)
    require(provenance["reader_id"] in READERS, "unknown reader identity")
    for name in IDENTITY_FIELDS:
        require(type(provenance[name]) is type(metadata[name]) and provenance[name] == metadata[name], f"result identity mismatch: {name}")
    for name in ("reader_build_sha256", "reader_config_sha256", "result_sha256"):
        require(re.fullmatch(r"[0-9a-f]{64}", provenance[name]) is not None, f"invalid {name}")
    expression = provenance["native_expression_sha256"]
    require(expression is None or re.fullmatch(r"[0-9a-f]{64}", expression) is not None, "invalid native expression hash")
    require(provenance["reader_id"] not in ("polars", "daft") or expression is not None, "missing native expression identity")
    require(digest_file(result) == provenance["result_sha256"], "result checksum mismatch")
    projection = metadata["projection"]
    with result.open("rb") as source, pa.ipc.open_stream(source) as stream:
        check_schema(stream.schema, projection)
        reported_nullability = {field.name: field.nullable for field in stream.schema}
        with tempfile.TemporaryDirectory(prefix="oracle-check-", dir=reference.parent) as scratch:
            with closing(database(Path(scratch) / "actual.sqlite")) as db:
                insert_rows(db, (record(row, projection) for batch in stream
                                for offset in range(0, batch.num_rows, BATCH_ROWS)
                                for row in batch.slice(offset, BATCH_ROWS).to_pylist()))
                count = compare(db, reference / "reference.sqlite", metadata["output_rows"])
        require(not source.read(1), "trailing bytes after Arrow stream")
    return {
        "status": "passed", **{name: metadata[name] for name in IDENTITY_FIELDS},
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
    prepare_parser.add_argument("--case", required=True)
    prepare_parser.add_argument("--output", type=Path, required=True)
    prepare_parser.add_argument("--duplicate-in-literal", action="store_true")
    check_parser = commands.add_parser("check", help="validate an untimed Arrow IPC stream and identity JSON")
    for name in ("reference", "fixtures", "result", "identity"):
        check_parser.add_argument(f"--{name}", type=Path, required=True)
    args = parser.parse_args()
    try:
        if args.command == "prepare":
            output = prepare(args.fixtures, args.case, args.output, args.duplicate_in_literal)
        else:
            output = check(args.reference, args.fixtures, args.result, args.identity)
    except (ValueError, KeyError, TypeError, StopIteration, OSError, sqlite3.Error, pa.ArrowException) as error:
        print(json.dumps({"status": "validation_failed", "error": str(error), "operation": args.command}))
        return 1
    print(json.dumps(output))
    return 0


if __name__ == "__main__":
    sys.exit(main())

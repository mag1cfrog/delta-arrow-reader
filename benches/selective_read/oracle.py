"""Independent, untimed reference for the public selective-read cases.

PyArrow only decodes full Parquet/IPC batches. Python evaluates predicates and
payloads; disk-backed SQLite orders exact rows and checks multiplicity.
"""

import argparse
import base64
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
import uuid
import zlib

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


def base_case(case_id):
    for suffix in (".dv", ".feature-only"):
        if case_id.endswith(suffix):
            case = case_id.removesuffix(suffix)
            require(case in DV_CASES if suffix == ".dv" else case == "row-groups.select", "unknown DV case")
            return case, suffix
    return case_id, ""


def case_input(fixtures, case_id, duplicate_literal=False):
    require(pa.__version__ == "25.0.1", "oracle requires pyarrow==25.0.1")
    manifest = load_json(Path(fixtures) / "manifest.json")
    require(manifest["status"] == "complete" and manifest["protocol"] == "selective-read-v1", "incomplete or unknown fixtures")
    case, suffix = base_case(case_id)
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
    require(fixture_id in ("li.clustered", "li.shuffled", "wide.clustered", "wide.shuffled", "files64", "files4096"),
            "unsupported fixture")
    if fixture_id.startswith("files"):
        require(query in ("empty", "eq2-in20"), "unknown file-organization query")
    source = next(s for s in manifest["sources"] if s["scale_factor"] == table["scale_factor"])
    shapes = WIDE_CASES if wide else ORIGINAL_CASES
    require(query in shapes, "unknown public query")
    projection, predicate, limit = shapes[query]
    literals = source["in_literals"]
    require(literals and all(type(v) is int for v in literals) and literals == sorted(set(literals)), "invalid frozen IN literals")
    require(len(literals) == 20 or (manifest["profile"] == "smoke" and len(literals) < 20), "wrong frozen literal count")
    canonical = sql_for(projection, predicate, limit, literals)
    require(canonical == source["wide_queries" if wide else "queries"][query], "manifest SQL differs from frozen case")
    if suffix:
        require(table["queries"][case_id] == canonical, "variant SQL changed")
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
        require(value is not None or name in PAYLOADS + CONTROL_PAYLOADS, f"unexpected null: {name}")
        if isinstance(value, Decimal):
            value = format(value, ".2f")
        elif isinstance(value, date):
            value = value.isoformat()
        values.append(value)
    key = (row["row_id"], 0) if "row_id" in projection else (row[KEYS[0]], row[KEYS[1]])
    return *key, json_bytes(values)


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
    for group in ([source] if source is not None else []) + [table]:
        logs = group.get("delta_logs", [group["delta_log"]] if group["delta_log"] else [])
        dvs = [f["deletion_vector"] for f in group["files"] if "deletion_vector" in f]
        for item in group["files"] + logs + dvs:
            descriptor = {k: item[k] for k in ("path", "bytes", "sha256")}
            descriptor["path"] = str(Path(group["path"]) / item["path"])
            verify_object(fixtures, descriptor)
            verified.append(descriptor)
    logs = table.get("delta_logs", [table["delta_log"]])
    require([item["path"] for item in logs] == [f"_delta_log/{v:020}.json" for v in range(table["snapshot_version"] + 1)]
            and logs[-1] == table["delta_log"], "wrong snapshot logs")
    by_path, metadata = {}, None
    for version, log in enumerate(logs):
        path = inside(fixtures, str(Path(table["path"]) / log["path"]))
        actions = [json.loads(line, parse_float=Decimal) for line in path.read_text().splitlines()]
        protocol = {"minReaderVersion": 1, "minWriterVersion": 2} if version == 0 else {
            "minReaderVersion": 3, "minWriterVersion": 7, "readerFeatures": ["deletionVectors"], "writerFeatures": ["deletionVectors"]}
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
        table_uuid = uuid.uuid5(uuid.NAMESPACE_URL, f"https://github.com/mag1cfrog/delta-arrow-reader/selective-read-v1/{load_json(Path(fixtures) / 'manifest.json')['profile']}/{table['id']}")
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
    return row_id, 0, json_bytes(values)


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


def prepare(fixtures, case_id, output, duplicate_literal=False):
    fixtures, output = Path(fixtures).resolve(), Path(output)
    manifest, table, source, projection, predicate, limit, literals, sql = case_input(fixtures, case_id, duplicate_literal)
    verified = objects(fixtures, table, source)
    case, suffix = base_case(case_id)
    geometry = control_geometry(fixtures, table, case) if source is None else None
    saved_keys = [tuple(key) for item in table["files"] for key in item.get("deletion_vector", {}).get("logical_ids", [])]
    require(len(saved_keys) == len(set(saved_keys)), "duplicate deleted logical ID")
    saved_keys = set(saved_keys)
    expected_deletions = set()
    deleted_qualifying = 0
    output.mkdir(parents=True, exist_ok=False)
    reference_db = output / "reference.sqlite"
    predicates = conditions(predicate, literals)
    counts = [0] * (len(predicates) + 1)
    found_literals = set()
    previous_key = None

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
                if table["deletion_vectors"] and deleted(row):
                    expected_deletions.add(key)
                    deleted_qualifying += passed == len(predicates)
                    continue
                if passed == len(predicates):
                    yield record(row, projection, derive_payloads=True)
            require(file_rows == item["rows"], "reference file row count changed")

    with closing(database(reference_db)) as db:
        insert_rows(db, expected_rows())
    require(counts[0] == (source or table)["rows"] == table["rows"], "source/table row counts disagree")
    require(sorted(found_literals) == literals, "frozen IN literals disagree with full source")
    require(expected_deletions == saved_keys, "saved logical deletions disagree with the independent source rule")
    live_qualifying = counts[-1] - deleted_qualifying
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
                require(is_deleted == (table["deletion_vectors"] and deleted(row)), "physical deletion ordinal differs from rule")
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

    with tempfile.TemporaryDirectory(prefix="oracle-", dir=output) as scratch:
        with closing(database(Path(scratch) / "actual.sqlite")) as db:
            insert_rows(db, fixture_rows())
            # Validate all qualifying fixture rows even for a LIMIT case.
            compare(db, reference_db, live_qualifying)
    require(actual_rows == (source or table)["rows"], "fixture lost source rows")
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
        "verified_objects": verified, "reference_sha256": digest_file(reference_db),
        **({"within_file_geometry": geometry} if geometry else {}),
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

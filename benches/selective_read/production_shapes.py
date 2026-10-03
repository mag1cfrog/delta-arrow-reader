"""Plan Q2/Q4-derived queries and file membership from the real SF10 source.

This is untimed input preparation. It writes no Delta fixture and cannot produce
a campaign manifest or certify Parquet page geometry or reader performance.
"""

import argparse
from datetime import date
import json
from pathlib import Path
import resource
import signal
import tempfile

import duckdb
import pyarrow.compute as pc
import pyarrow.parquet as pq

from oracle import ORIGINAL, PAYLOADS, WIDE69, digest_file, inside, require

CONTRACT = Path(__file__).resolve().parents[2] / "docs/content/benchmarks/selective-read-production-shapes.md"
PREDICATE = "l_shipdate = DATE '1995-03-15' AND l_shipmode = 'AIR' AND l_linenumber IN (1)"
SHAPES = {
    "q2": {"files": 130, "stripes": 5, "extra_numeric_columns": 336, "projection": WIDE69},
    "q4": {"files": 60, "stripes": 6, "extra_numeric_columns": 10,
           "projection": WIDE69 + ("l_suppkey", "l_quantity")},
}
INPUT_COLUMNS = ["l_orderkey", "l_linenumber", "l_shipdate", "l_shipmode"]


def definitions(file_target_mib=512, page_rows=20000, page_bytes=1048576, write_batch_rows=1024):
    require(file_target_mib in (256, 512) and page_rows in (2048, 20000)
            and page_bytes in (8192, 65536, 1048576) and write_batch_rows in (128, 1024),
            "unsupported production layout")
    return {name: {**shape, "files": shape["files"] * (512 // file_target_mib),
                   "file_target_mib": file_target_mib, "row_group_rows": 131072,
                   "data_page_rows": page_rows, "data_page_bytes": page_bytes,
                   "write_batch_rows": write_batch_rows, "dictionary": False,
                   "projection": list(shape["projection"]),
                   "stored_columns": len(ORIGINAL) + len(PAYLOADS) + shape["extra_numeric_columns"],
                   "canonical_sql": "SELECT " + ", ".join(shape["projection"]) + " FROM bench WHERE " + PREDICATE}
            for name, shape in SHAPES.items()}


def cases():
    return {f"production.{shape}.{layout}" + (".dv" if dv else ""):
            {"shape": shape, "layout": layout, "deletion_vectors": dv}
            for shape in SHAPES for layout in ("localized", "scattered") for dv in (False, True)}


def source_counts(paths):
    """Count each predicate stage with Arrow, independently of the layout SQL."""
    counts = [0, 0, 0, 0]
    for path in paths:
        for batch in pq.ParquetFile(path).iter_batches(columns=INPUT_COLUMNS):
            masks = [pc.equal(batch.column("l_shipdate"), date(1995, 3, 15))]
            masks.append(pc.and_(masks[-1], pc.equal(batch.column("l_shipmode"), "AIR")))
            masks.append(pc.and_(masks[-1], pc.equal(batch.column("l_linenumber"), 1)))
            counts[0] += batch.num_rows
            for index, mask in enumerate(masks, 1):
                counts[index] += pc.sum(pc.cast(mask, "int64")).as_py()
    return counts


def file_geometry(connection, shape):
    """Predict file membership only; actual writer metadata must check it again."""
    files, stripes = shape["files"], shape["stripes"]
    require(type(files) is int and type(stripes) is int and files >= stripes > 0, "invalid file/stripe counts")
    sql = f"""
        WITH striped AS (
            SELECT *, l_orderkey % {stripes} AS stripe FROM source
        ), ordered AS (
            SELECT *, row_number() OVER (
                PARTITION BY stripe ORDER BY l_shipdate, l_shipmode, l_linenumber, l_orderkey
            ) - 1 AS ordinal, count(*) OVER (PARTITION BY stripe) AS stripe_rows
            FROM striped
        ), assigned AS (
            SELECT *, (ordinal * ({files // stripes} + CAST(stripe < {files % stripes} AS BIGINT)))
                // stripe_rows AS file_index FROM ordered
        )
        SELECT stripe, file_index, count(*) AS rows,
            min(l_shipdate) AS date_min, max(l_shipdate) AS date_max,
            min(l_shipmode) AS mode_min, max(l_shipmode) AS mode_max,
            min(l_linenumber) AS line_min, max(l_linenumber) AS line_max,
            count(*) FILTER (WHERE {PREDICATE}) AS matching_rows
        FROM assigned GROUP BY stripe, file_index ORDER BY stripe, file_index
    """
    rows = connection.execute(sql).fetchall()
    require(len(rows) == files, "source cannot populate the declared file count")
    result = []
    for stripe, index, count, dmin, dmax, mmin, mmax, lmin, lmax, matched in rows:
        candidate = dmin <= date(1995, 3, 15) <= dmax and mmin <= "AIR" <= mmax and lmin <= 1 <= lmax
        require(not matched or candidate, "file bounds exclude qualifying rows")
        result.append({"stripe": stripe, "file_index": index, "rows": count,
                       "candidate": candidate, "matching_rows": matched})
    return result


def plan(fixtures, output, file_target_mib=512, page_rows=20000, page_bytes=1048576, write_batch_rows=1024):
    shapes = definitions(file_target_mib, page_rows, page_bytes, write_batch_rows)
    manifest_path = fixtures / "manifest.json"
    manifest_hash = digest_file(manifest_path)
    manifest = json.loads(manifest_path.read_text())
    require(manifest["status"] == "complete", "incomplete source fixture")
    sources = [s for s in manifest["sources"] if s["scale_factor"] == 10]
    require(len(sources) == 1, "one verified SF10 source is required")
    source = sources[0]
    paths = []
    for item in source["files"]:
        path = inside(fixtures, str(Path(source["path"]) / item["path"]))
        require(path.stat().st_size == item["bytes"] and digest_file(path) == item["sha256"], "source object changed")
        paths.append(str(path))
    require(len(set(paths)) == len(paths) == source["file_count"], "source inventory differs")
    counts = source_counts(paths)
    require(counts[0] == source["rows"], "source row count differs")
    require(all(a > b > 0 for a, b in zip(counts, counts[1:])), "each predicate must reduce the source")
    require(300 <= counts[-1] <= 2000, "query does not have the declared hundreds-of-rows shape")
    output.mkdir()
    with tempfile.TemporaryDirectory(prefix="shape-sort-", dir=output) as temporary:
        with duckdb.connect(config={"memory_limit": "2GiB", "threads": "4",
                                    "temp_directory": temporary, "max_temp_directory_size": "16GiB"}) as connection:
            connection.read_parquet(paths).project(", ".join(INPUT_COLUMNS)).create_view("source")
            for name, shape in shapes.items():
                print("checking logical file geometry for " + name, flush=True)
                geometry = file_geometry(connection, shape)
                candidates = [f for f in geometry if f["candidate"]]
                matching = [f for f in geometry if f["matching_rows"]]
                (output / (name + "-files.json")).write_text(json.dumps(geometry, indent=2) + "\n")
                require(sum(f["rows"] for f in geometry) == counts[0]
                        and sum(f["matching_rows"] for f in geometry) == counts[-1], "layout lost source or matching rows")
                require(shape["stripes"] <= len(candidates) <= 3 * shape["stripes"]
                        and len(matching) >= shape["stripes"], "query does not select a few files across every stripe")
                shape.update(candidate_files=len(candidates), matching_files=len(matching), output_rows=counts[-1],
                             file_geometry_sha256=digest_file(output / (name + "-files.json")))
    require(digest_file(manifest_path) == manifest_hash, "source manifest changed")
    sql = {name: shape["canonical_sql"] for name, shape in shapes.items()}
    (output / "sql.json").write_text(json.dumps(sql, indent=2) + "\n")
    result = {"format": "selective-read-production-shape-plan-v1", "status": "logical_geometry_checked",
              "publication_ready": False, "native_campaign_ready": False,
              "contract_sha256": digest_file(CONTRACT), "planner_sha256": digest_file(Path(__file__)),
              "source_manifest_sha256": manifest_hash, "source_scale": 10, "predicate_stage_rows": counts,
              "file_target_mib": file_target_mib, "data_page_rows": page_rows,
              "data_page_bytes": page_bytes, "write_batch_rows": write_batch_rows,
              "duckdb": duckdb.__version__, "shapes": shapes, "cases": cases(),
              "scope": "Source predicates and proposed file membership only. No Delta generation, physical page or performance acceptance.",
              "pending": ["bounded compression/capacity probe", "Delta fixtures and actual group/page geometry",
                          "independent exact oracle and real DV pairs", "five-reader pilot and recorded storage transport"]}
    (output / "plan.json").write_text(json.dumps(result, indent=2) + "\n")
    return result


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--file-target-mib", type=int, choices=(256, 512), default=512)
    parser.add_argument("--page-rows", type=int, choices=(2048, 20000), default=20000)
    parser.add_argument("--page-bytes", type=int, choices=(8192, 65536, 1048576), default=1048576)
    parser.add_argument("--write-batch-rows", type=int, choices=(128, 1024), default=1024)
    args = parser.parse_args()
    # Only preparation is bounded here; these settings never govern native timings.
    resource.setrlimit(resource.RLIMIT_AS, (16 * 1024**3, 16 * 1024**3))
    signal.signal(signal.SIGALRM, signal.SIG_DFL)
    signal.alarm(600)
    result = plan(args.source.resolve(), args.output.resolve(), args.file_target_mib, args.page_rows,
                  args.page_bytes, args.write_batch_rows)
    print(json.dumps({"status": result["status"], "predicate_stage_rows": result["predicate_stage_rows"],
                      "shapes": {name: {k: shape[k] for k in ("files", "candidate_files", "matching_files", "output_rows")}
                                 for name, shape in result["shapes"].items()}}))

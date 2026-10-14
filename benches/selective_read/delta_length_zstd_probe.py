"""Measure sequential raw-block discovery against complete payload reads, not query execution."""

import argparse
from concurrent.futures import ThreadPoolExecutor
import hashlib
import json
from pathlib import Path
import resource
import time
from urllib.parse import urlsplit

import pyarrow.parquet as pq

import network
import storage
from storage import digest, save


def raw_headers(page, fetch):
    """Discover every next header from bytes returned by the preceding request."""
    size = page["page_bytes"]
    assert 8192 <= size <= 8 * 1024**2
    prefix = fetch(0, 8192)
    header_bytes = page["zstd"]["page_header_bytes"]
    frame = prefix[header_bytes:]
    assert frame[:4] == b"\x28\xb5\x2f\xfd" and len(frame) >= 18
    descriptor = frame[4]
    assert descriptor & 15 == 0, "requires raw frames without dictionary or checksum"
    single = bool(descriptor & 32)
    size_bytes = [int(single), 2, 4, 8][descriptor >> 6]
    size_start = 5 + int(not single)
    if size_bytes:
        decoded = int.from_bytes(frame[size_start:size_start + size_bytes], "little")
        assert decoded + (256 if size_bytes == 2 else 0) == page["body_bytes"]
    offset = header_bytes + size_start + size_bytes
    blocks = []
    decoded = 0
    while len(blocks) < 128:
        assert offset + 3 <= size, "truncated block header"
        header = prefix[offset:offset + 3] if offset + 3 <= len(prefix) else fetch(offset, offset + 3)
        assert len(header) == 3
        header = int.from_bytes(header, "little")
        length = header >> 3
        assert (header >> 1) & 3 == 0 and length <= 131072, "not a bounded raw block"
        assert offset + 3 + length <= size
        blocks.append({"header_offset": offset - header_bytes, "kind": "raw",
            "stored_bytes": length, "decoded_bytes": length})
        offset += 3 + length
        decoded += length
        assert decoded <= page["body_bytes"]
        if header & 1:
            assert offset == size and decoded == page["body_bytes"]
            return blocks
    raise ValueError("raw-block discovery exceeds 128 blocks")


def batches(fixture):
    manifest = json.loads((fixture / "manifest.json").read_text())
    assert manifest["case"] == "long" and manifest["compression"] == "zstd"
    for item in manifest["objects"]:
        assert digest(fixture / item["path"]) == item["sha256"]
    prefix = digest(fixture / "manifest.json")
    result = []
    for layout in manifest["layout"]:
        path = fixture / layout["file"]
        metadata = pq.ParquetFile(path).metadata
        with path.open("rb") as source:
            for group in range(metadata.num_row_groups):
                columns, pages = [], []
                for index in range(3, metadata.num_columns):
                    column = metadata.row_group(group).column(index)
                    assert column.dictionary_page_offset is None and column.compression == "ZSTD"
                    start, size = column.data_page_offset, column.total_compressed_size
                    source.seek(start)
                    columns.append({"start": start, "end": start + size,
                        "sha256": hashlib.sha256(source.read(size)).hexdigest()})
                    offset = start
                    for page in layout["pages"]:
                        if page["row_group"] == group and page["column"] == column.path_in_schema:
                            if page["zstd"]["all_raw_without_checksum_or_dictionary"]:
                                pages.append(dict(page, offset=offset))
                            offset += page["page_bytes"]
                    assert offset == start + size
                assert pages
                result.append({"key": prefix + "/" + path.name, "row_group": group,
                    "columns": columns, "raw_pages": pages})
    return result


def measure(args):
    args.output.mkdir()
    groups = batches(args.fixture)
    profile = network.config(args.state)
    assert profile is not None
    upstream = urlsplit(storage.state(args.state)["endpoint"])
    proxy = urlsplit(profile["endpoint"])
    connection = f"{upstream.hostname}:{upstream.port}:{proxy.hostname}:{proxy.port}"
    save(args.output / "experiment.json", {"fixture_sha256": digest(args.fixture / "manifest.json"),
        "source_sha256": digest(Path(__file__)), "groups": groups, "network": profile,
        "scope": "Optimistic discovery diagnostic, not queries. Offline inspection supplies raw-page eligibility and page-header lengths; block positions must be discovered over the network. Length/value reads and reconstruction are omitted.",
        "client": "Same curl SigV4 client in both modes; 32 worker threads; curl process/connection overhead is included.",
        "acceptance": "Three alternating pairs. If discovery alone consistently exceeds complete payload reads, do not implement this sequential strategy for these fixtures.",
        "memory": "Process RSS is a cumulative high-water mark, not a per-mode comparison."})
    samples = []
    with storage.exclusive(args.state), ThreadPoolExecutor(max_workers=32) as pool:
        storage.verify_server(args.state)
        for repetition in range(3):
            order = ("complete", "headers") if repetition % 2 == 0 else ("headers", "complete")
            for mode in order:
                slot = args.output / f"{repetition}-{mode}"
                slot.mkdir()
                network.control(args.state, reset=True, traced=True)
                cpu = sum(r.ru_utime + r.ru_stime for r in (resource.getrusage(resource.RUSAGE_SELF), resource.getrusage(resource.RUSAGE_CHILDREN)))
                started = time.perf_counter()
                for group in groups:
                    def fetch(start, end):
                        body = storage.request(args.state, "/" + storage.BUCKET + "/" + group["key"],
                            "--connect-to", connection, "--range", f"{start}-{end - 1}")
                        assert len(body) == end - start, "short range response"
                        return body
                    def read(item):
                        if mode == "complete":
                            assert hashlib.sha256(fetch(item["start"], item["end"])).hexdigest() == item["sha256"]
                        else:
                            actual = raw_headers(item, lambda start, end: fetch(item["offset"] + start, item["offset"] + end))
                            assert actual == item["zstd"]["blocks"]
                    list(pool.map(read, group["columns"] if mode == "complete" else group["raw_pages"]))
                elapsed = time.perf_counter() - started
                used_cpu = sum(r.ru_utime + r.ru_stime for r in (resource.getrusage(resource.RUSAGE_SELF), resource.getrusage(resource.RUSAGE_CHILDREN))) - cpu
                capture = network.finish(args.state, slot)
                sample = {"mode": mode, "repetition": repetition, "seconds": elapsed,
                    "client_cpu_seconds": used_cpu, "process_peak_rss_kib": resource.getrusage(resource.RUSAGE_SELF).ru_maxrss,
                    "network": capture}
                save(slot / "sample.json", sample)
                samples.append(sample)
                print(mode, repetition, round(elapsed, 3), capture["requests"], capture["response_bytes"], flush=True)
    save(args.output / "summary.json", samples)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ("fixture", "state", "output"):
        parser.add_argument("--" + name, type=lambda p: Path(p).resolve(), required=True)
    measure(parser.parse_args())

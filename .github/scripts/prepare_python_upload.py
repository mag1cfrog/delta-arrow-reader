"""Stage missing wheels only after checking the files already published."""

import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import sys
from urllib.error import HTTPError
from urllib.parse import quote
from urllib.request import Request, urlopen


INDEX_URLS = {"testpypi": "https://test.pypi.org", "pypi": "https://pypi.org"}
WHEEL_CHECK = (
    Path(__file__).resolve().parents[2]
    / "crates/delta-arrow-reader-python/tests/check_wheel.py"
)


def published_files(index, version):
    url = f"{INDEX_URLS[index]}/pypi/delta-arrow-reader/{quote(version, safe='')}/json"
    request = Request(url, headers={"Accept": "application/json", "Cache-Control": "no-cache"})
    try:
        with urlopen(request, timeout=30) as response:
            release = json.load(response)
    except HTTPError as error:
        error.close()
        if error.code == 404:
            return {}
        raise

    try:
        name = re.sub(r"[-_.]+", "-", release["info"]["name"]).lower()
        if name != "delta-arrow-reader" or release["info"]["version"] != version:
            raise ValueError("index response does not match the requested package and version")
        if not isinstance(release["urls"], list):
            raise ValueError("index response must contain a list of files")
        files = {}
        for entry in release["urls"]:
            filename = entry["filename"]
            digest = entry["digests"]["sha256"]
            if not isinstance(filename, str) or not filename:
                raise ValueError("published file has no filename")
            if filename in files:
                raise ValueError(f"duplicate published filename: {filename}")
            if not re.fullmatch(r"[0-9a-f]{64}", digest):
                raise ValueError(f"invalid SHA256 for {filename}")
            if entry["yanked"] is not False:
                raise ValueError(f"cannot resume a release with yanked files: {filename}")
            files[filename] = digest
        return files
    except (KeyError, TypeError) as error:
        raise ValueError("invalid release metadata from the index") from error


def stage_upload(artifacts, version, index, destination):
    result = subprocess.run(
        [sys.executable, str(WHEEL_CHECK), str(artifacts),
         "--artifact-set", "--expected-version", version],
        capture_output=True, text=True,
    )
    if result.returncode != 0:
        raise ValueError(result.stderr.strip())

    wheels = {path.name: path for path in artifacts.glob("*/*.whl")}
    published = published_files(index, version)
    unexpected = published.keys() - wheels.keys()
    if unexpected:
        raise ValueError(f"unexpected published files: {sorted(unexpected)}")
    for filename, expected_hash in published.items():
        with wheels[filename].open("rb") as source:
            actual_hash = hashlib.file_digest(source, "sha256").hexdigest()
        if actual_hash != expected_hash:
            raise ValueError(f"SHA256 mismatch for {filename}; changed binaries need a new version")

    # Validate every published file before staging any pending upload.
    missing = sorted(wheels.keys() - published.keys())
    destination.mkdir()
    for filename in missing:
        shutil.copyfile(wheels[filename], destination / filename)
    return len(missing)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("artifacts", type=Path)
    parser.add_argument("--expected-version", required=True)
    parser.add_argument("--index", choices=INDEX_URLS, required=True)
    parser.add_argument("--destination", type=Path, required=True)
    args = parser.parse_args()
    try:
        count = stage_upload(args.artifacts, args.expected_version, args.index, args.destination)
        if output := os.environ.get("GITHUB_OUTPUT"):
            with Path(output).open("a") as stream:
                stream.write(f"has-files={'true' if count else 'false'}\n")
    except (OSError, ValueError) as error:
        parser.exit(1, f"Python upload check failed: {error}\n")
    print(f"Staged {count} missing wheel(s) for {args.index}")


if __name__ == "__main__":
    main()

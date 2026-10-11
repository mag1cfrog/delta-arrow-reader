import json
import os
import subprocess


PREFIX = "v0-rust-core-ci-targets-Windows_NT-x64-"
REF = "refs/heads/main"


def stale_caches(caches):
    targets = sorted(
        (
            cache for cache in caches
            if cache["ref"] == REF and cache["key"].startswith(PREFIX)
        ),
        key=lambda cache: cache["last_accessed_at"],
        reverse=True,
    )
    return targets[2:]


def main():
    endpoint = f"repos/{os.environ['GH_REPO']}/actions/caches"
    pages = json.loads(subprocess.check_output([
        "gh", "api", "--paginate", "--slurp",
        f"{endpoint}?key={PREFIX}&ref={REF}&per_page=100",
    ]))
    caches = [cache for page in pages for cache in page["actions_caches"]]
    for cache in stale_caches(caches):
        print(f"Deleting superseded Windows target cache: {cache['key']}", flush=True)
        subprocess.run(["gh", "api", "--method", "DELETE", f"{endpoint}/{cache['id']}"], check=True)


if __name__ == "__main__":
    main()

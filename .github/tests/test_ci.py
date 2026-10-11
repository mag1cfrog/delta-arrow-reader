import fnmatch
import importlib.util
import itertools
import json
import os
from pathlib import Path
import re
import subprocess
import tempfile
import unittest
from unittest.mock import patch

import yaml


ROOT = Path(__file__).resolve().parents[2]
spec = importlib.util.spec_from_file_location(
    "prune_ci_caches", ROOT / ".github/scripts/prune_ci_caches.py"
)
prune = importlib.util.module_from_spec(spec)
spec.loader.exec_module(prune)
spec = importlib.util.spec_from_file_location(
    "check_sccache_writes", ROOT / ".github/scripts/check_sccache_writes.py"
)
cache_writes = importlib.util.module_from_spec(spec)
spec.loader.exec_module(cache_writes)


def workflow(name):
    return yaml.load(
        (ROOT / f".github/workflows/{name}.yml").read_text(), Loader=yaml.BaseLoader
    )


class CIRoutingTests(unittest.TestCase):
    def test_python_filter_covers_inputs_without_cli_benchmark_or_docs(self):
        patterns = json.loads((ROOT / ".github/path-filters.json").read_text())["python"]
        paths = {
            "src/reader/scan.rs": True,
            "Cargo.lock": True,
            "Cargo.toml": True,
            "crates/delta-arrow-reader-cli/Cargo.toml": True,
            "crates/delta-arrow-reader-python/src/lib.rs": True,
            "crates/delta-arrow-reader-python/tests/test_table.py": True,
            "tests/reader/https.py": True,
            "tests/reader/fixtures/external_writer/corpus/table/data.parquet": True,
            "pyproject.toml": True,
            "delta_arrow_reader.pyi": True,
            "README.md": True,
            "LICENSE": True,
            ".cargo/config.toml": True,
            ".github/workflows/cache-warm.yml": True,
            ".github/workflows/pypi-build.yml": True,
            ".github/workflows/pypi.yml": True,
            "crates/delta-arrow-reader-cli/src/main.rs": False,
            "crates/delta-arrow-reader-cli/tests/scan.py": False,
            "benches/selective_read/src/main.rs": False,
            "tests/reader/direct.rs": False,
            "docs/content/contributing.md": False,
        }
        for path, expected in paths.items():
            with self.subTest(path=path):
                self.assertEqual(any(fnmatch.fnmatchcase(path, p) for p in patterns), expected)

    def test_summary_requires_python_only_when_requested(self):
        script = workflow("ci")["jobs"]["ci"]["steps"][0]["run"]
        for code, python, package, wheels, validated in itertools.product(
            ("true", "false"), ("true", "false"),
            ("success", "failure", "skipped", "cancelled"),
            ("success", "failure", "skipped", "cancelled"), ("true", "false", ""),
        ):
            values = {
                "changes.outputs.code": code,
                "changes.outputs.python": python,
                "changes.outputs.selective_read": "false",
                "python-package.result": package,
                "python-wheels.result": wheels,
                "python-wheels.outputs.validated": validated,
                "cli-binaries.outputs.validated": "true",
            }
            rendered = re.sub(
                r"\$\{\{ needs\.([\w.-]+) \}\}",
                lambda match: values.get(match[1], "success"), script,
            )
            result = subprocess.run(["bash", "-e", "-c", rendered], capture_output=True)
            expected = python != "true" or package == wheels == "success" and validated == "true"
            with self.subTest(code=code, python=python, package=package, wheels=wheels, validated=validated):
                self.assertEqual(result.returncode == 0, expected, result.stdout)

    def test_cache_warming_requires_the_triggering_main_commit(self):
        steps = workflow("pypi-build")["jobs"]["build"]["steps"]
        script = next(step["run"] for step in steps if step.get("id") == "versions")
        sha = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=ROOT, text=True).strip()
        version = re.search(r'^version = "([^"]+)"', (ROOT / "Cargo.toml").read_text(), re.M)[1]
        with tempfile.TemporaryDirectory() as directory:
            for ref, event, same_commit in itertools.product(
                ("refs/heads/main", "refs/heads/fix/example", "refs/pull/1/merge"),
                ("push", "workflow_dispatch", "pull_request", "pull_request_target"),
                (True, False),
            ):
                result = subprocess.run(
                    ["bash", "-e", "-c", script], cwd=ROOT, capture_output=True,
                    env={
                        **os.environ, "CACHE_ONLY": "true", "SOURCE_REF": sha,
                        "GITHUB_SHA": sha if same_commit else "0" * 40,
                        "GITHUB_REF": ref, "GITHUB_EVENT_NAME": event,
                        "EXPECTED_VERSION": version, "GITHUB_OUTPUT": f"{directory}/output",
                    },
                )
                expected = ref == "refs/heads/main" and event in ("push", "workflow_dispatch") and same_commit
                with self.subTest(ref=ref, event=event, same_commit=same_commit):
                    self.assertEqual(result.returncode == 0, expected, result.stdout)


class CacheRetentionTests(unittest.TestCase):
    def cache(self, number, *, ref=prune.REF, prefix=prune.PREFIX):
        return {
            "id": number, "key": f"{prefix}{number}", "ref": ref,
            "created_at": f"2026-10-{number:02}T00:00:00Z",
            "last_accessed_at": f"2026-10-{number:02}T00:00:00Z",
        }

    def test_keeps_two_recently_used_and_leaves_other_cache_families_and_refs(self):
        caches = [self.cache(n) for n in (3, 1, 4, 2)]
        caches += [self.cache(5, prefix="sccache-"), self.cache(6, ref="refs/pull/1/merge")]
        self.assertEqual([cache["id"] for cache in prune.stale_caches(caches)], [2, 1])
        for count in range(3):
            self.assertEqual(prune.stale_caches([self.cache(n) for n in range(1, count + 1)]), [])

    def test_keeps_an_old_cache_reused_after_a_dependency_revert(self):
        reused = self.cache(1)
        reused["last_accessed_at"] = "2026-10-05T00:00:00Z"
        caches = [reused, self.cache(2), self.cache(3), self.cache(4)]
        self.assertEqual([cache["id"] for cache in prune.stale_caches(caches)], [3, 2])

    def test_prunes_across_pages_by_id(self):
        pages = [{"actions_caches": [self.cache(4), self.cache(3)]},
                 {"actions_caches": [self.cache(2), self.cache(1)]}]
        with patch.dict(os.environ, {"GH_REPO": "owner/repo"}), \
             patch.object(prune.subprocess, "check_output", return_value=json.dumps(pages)) as read, \
             patch.object(prune.subprocess, "run") as delete:
            prune.main()
        self.assertIn("--paginate", read.call_args.args[0])
        self.assertEqual([call.args[0][-1] for call in delete.call_args_list],
                         ["repos/owner/repo/actions/caches/2", "repos/owner/repo/actions/caches/1"])


class CacheWriteTests(unittest.TestCase):
    def test_accepts_success_and_existing_entries(self):
        cache_writes.check_writes({"cache_write_errors": 0}, "")
        cache_writes.check_writes(
            {"cache_write_errors": 1},
            "Error executing cache write: AlreadyExists: immutable entry",
        )

    def test_rejects_backend_errors_and_missing_diagnostics(self):
        for log in (
            "", "Error executing cache write: RateLimited",
            "Error executing cache write: AlreadyExists\nError executing cache write: PermissionDenied",
        ):
            with self.subTest(log=log), self.assertRaisesRegex(RuntimeError, "cache write errors"):
                cache_writes.check_writes({"cache_write_errors": 1}, log)


if __name__ == "__main__":
    unittest.main()

import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest


SCRIPT = Path(__file__).resolve().parents[1] / "scripts/prepare_python_release.py"


class PythonReleaseTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory(prefix="python-release-request-")
        self.addCleanup(temporary.cleanup)
        self.directory = Path(temporary.name)
        self.environment = {
            **os.environ,
            "GIT_CONFIG_GLOBAL": os.devnull,
            "GIT_CONFIG_NOSYSTEM": "1",
            "GITHUB_EVENT_PATH": str(self.directory / "event.json"),
            "GITHUB_OUTPUT": str(self.directory / "outputs"),
        }
        self.git("init", "--initial-branch=main")
        self.git("config", "user.name", "Release request test")
        self.git("config", "user.email", "release-test@example.invalid")
        (self.directory / "Cargo.toml").write_text('[workspace.package]\nversion = "1.2.3"\n')
        self.git("add", "Cargo.toml")
        self.git("commit", "-m", "chore: release fixture")
        self.sha = self.git("rev-parse", "HEAD").strip()
        self.git("tag", "-a", "v1.2.3", "-m", "Release fixture")
        self.release_event = {
            "action": "published",
            "release": {"tag_name": "v1.2.3", "draft": False, "prerelease": False},
        }

    def git(self, *args):
        return subprocess.check_output(
            ["git", *args], cwd=self.directory, env=self.environment,
            text=True, stderr=subprocess.STDOUT,
        )

    def check(self, event, *, event_name="workflow_dispatch", ref="refs/heads/main", sha=None):
        Path(self.environment["GITHUB_EVENT_PATH"]).write_text(json.dumps(event))
        output = Path(self.environment["GITHUB_OUTPUT"])
        output.write_text("")
        result = subprocess.run(
            [sys.executable, str(SCRIPT)], cwd=self.directory, text=True,
            capture_output=True,
            env={
                **self.environment,
                "GITHUB_EVENT_NAME": event_name,
                "GITHUB_REF": ref,
                "GITHUB_SHA": self.sha if sha is None else sha,
            },
        )
        values = dict(line.split("=", 1) for line in output.read_text().splitlines())
        if result.returncode != 0:
            self.assertEqual(values, {}, "failed checks must not emit builder inputs")
        return result, values

    def test_manual_build_only_is_default(self):
        result, values = self.check({"inputs": {"expected-version": "1.2.3"}})
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(values, {
            "source-ref": self.sha, "expected-version": "1.2.3", "publish-to": "none",
        })

    def test_manual_modes_accept_matching_tags(self):
        for mode in ("none", "testpypi", "pypi"):
            with self.subTest(mode=mode):
                result, values = self.check(
                    {"inputs": {"publish-to": mode, "expected-version": "1.2.3"}},
                    ref="refs/tags/v1.2.3",
                )
                self.assertEqual(result.returncode, 0, result.stderr)
                self.assertEqual(values["publish-to"], mode)

    def test_branch_rehearsal_is_only_allowed_for_testpypi(self):
        for mode in ("testpypi", "pypi"):
            with self.subTest(mode=mode):
                result, _ = self.check(
                    {"inputs": {"publish-to": mode, "expected-version": "1.2.3"}},
                )
                self.assertEqual(result.returncode == 0, mode == "testpypi", result.stderr)

    def test_stable_published_release_selects_pypi(self):
        result, values = self.check(
            self.release_event, event_name="release", ref="refs/tags/v1.2.3",
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(values, {
            "source-ref": self.sha, "expected-version": "1.2.3", "publish-to": "pypi",
        })

    def test_published_release_requires_matching_tag_ref(self):
        for ref in ("refs/heads/main", "refs/tags/other-v1.2.3", "refs/tags/v1.2.4"):
            with self.subTest(ref=ref):
                result, _ = self.check(self.release_event, event_name="release", ref=ref)
                self.assertNotEqual(result.returncode, 0)

    def test_rejects_unsupported_release_events(self):
        for change in (
            {"draft": True}, {"prerelease": True}, {"draft": None},
            {"prerelease": None}, {"tag_name": "other-v1.2.3"},
            {"tag_name": "v1.2.4"}, {"tag_name": "v1.2.3-rc.1"},
        ):
            with self.subTest(change=change):
                event = {
                    **self.release_event,
                    "release": {**self.release_event["release"], **change},
                }
                result, _ = self.check(event, event_name="release", ref="refs/tags/v1.2.3")
                self.assertNotEqual(result.returncode, 0)
        for event_name, action in (
            ("release", "edited"), ("push", "published"), ("pull_request", "published"),
        ):
            with self.subTest(event_name=event_name, action=action):
                result, _ = self.check(
                    {**self.release_event, "action": action},
                    event_name=event_name, ref="refs/tags/v1.2.3",
                )
                self.assertNotEqual(result.returncode, 0)

    def test_rejects_invalid_manual_inputs(self):
        for inputs in (
            {}, {"expected-version": "1.2.4"}, {"expected-version": "v1.2.3"},
            {"expected-version": "1.2.3\npublish-to=pypi"},
            {"expected-version": "1.2.3", "publish-to": "unknown"},
        ):
            with self.subTest(inputs=inputs):
                result, _ = self.check({"inputs": inputs})
                self.assertNotEqual(result.returncode, 0)

    def test_rejects_mismatched_refs_and_commits(self):
        for options in (
            {"ref": "refs/tags/v1.2.4"}, {"ref": "refs/tags/other-v1.2.3"},
            {"ref": "refs/pull/1/merge"}, {"sha": "0" * 40}, {"sha": self.sha[:7]},
        ):
            with self.subTest(options=options):
                result, _ = self.check({"inputs": {"expected-version": "1.2.3"}}, **options)
                self.assertNotEqual(result.returncode, 0)
        self.git("commit", "--allow-empty", "-m", "chore: move past release tag")
        self.sha = self.git("rev-parse", "HEAD").strip()
        result, _ = self.check(self.release_event, event_name="release", ref="refs/tags/v1.2.3")
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("tag does not point to the event commit", result.stderr)

    def test_reads_version_from_the_commit(self):
        (self.directory / "Cargo.toml").write_text('[workspace.package]\nversion = "9.9.9"\n')
        result, values = self.check({"inputs": {"expected-version": "1.2.3"}})
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(values["expected-version"], "1.2.3")

    def test_rejects_nonstable_cargo_versions(self):
        for version in ("1.2.3-rc.1", "1.2.3+build", "01.2.3"):
            with self.subTest(version=version):
                (self.directory / "Cargo.toml").write_text(
                    f'[workspace.package]\nversion = "{version}"\n'
                )
                self.git("add", "Cargo.toml")
                self.git("commit", "-m", "test: change Cargo version")
                self.sha = self.git("rev-parse", "HEAD").strip()
                result, _ = self.check({"inputs": {"expected-version": version}})
                self.assertNotEqual(result.returncode, 0)
                self.assertIn("stable X.Y.Z Cargo version", result.stderr)


if __name__ == "__main__":
    unittest.main()

"""Check shared release planning in a tiny, offline Cargo workspace."""

import json
import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import tomllib
import unittest


ROOT = Path(__file__).resolve().parents[2]


class ReleasePlanningTests(unittest.TestCase):
    def test_python_crate_release_policy(self):
        config = tomllib.loads((ROOT / "release-plz.toml").read_text())
        packages = {package["name"]: package for package in config["package"]}
        python = packages["delta-arrow-reader-python"]
        self.assertTrue(python["git_only"])
        for option in (
            "publish", "git_tag_enable", "git_release_enable", "changelog_update",
        ):
            with self.subTest(option=option):
                self.assertFalse(python[option])
        self.assertFalse(packages["delta-arrow-reader-cli"]["release"])
        for option in ("git_tag_name", "git_release_name"):
            self.assertEqual(config["workspace"][option], "v{{ version }}")
        manifest = ROOT / "crates/delta-arrow-reader-python/Cargo.toml"
        self.assertFalse(tomllib.loads(manifest.read_text())["package"]["publish"])

    def test_python_only_change_updates_shared_release(self):
        self.assert_python_change_updates_shared_release(
            "crates/python/src/lib.rs",
            "pub fn python() {}\npub fn python_only_feature() {}\n",
        )

    def test_python_metadata_only_change_updates_shared_release(self):
        self.assert_python_change_updates_shared_release(
            "pyproject.toml",
            '[project]\nname = "delta-arrow-reader"\nrequires-python = ">=3.11"\n',
        )

    def test_python_stubs_only_change_updates_shared_release(self):
        self.assert_python_change_updates_shared_release(
            "delta_arrow_reader.pyi",
            "def read() -> int: ...\ndef version() -> str: ...\n",
        )

    def assert_python_change_updates_shared_release(self, changed_path, contents):
        with tempfile.TemporaryDirectory(prefix="python-release-test-") as temporary:
            project = Path(temporary) / "project"
            project.mkdir()
            environment = {
                **os.environ,
                "CARGO_NET_OFFLINE": "true",
                "CARGO_TARGET_DIR": str(Path(temporary) / "target"),
                "GIT_CONFIG_GLOBAL": os.devnull,
                "GIT_CONFIG_NOSYSTEM": "1",
            }

            def run(*command):
                result = subprocess.run(
                    command, cwd=project, env=environment, text=True,
                    stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                )
                self.assertEqual(result.returncode, 0, result.stdout)
                return result.stdout

            root_manifest = tomllib.loads((ROOT / "Cargo.toml").read_text())
            (project / "Cargo.toml").write_text(f'''\
[workspace]
members = ["crates/python", "crates/cli"]
default-members = ["."]
resolver = "3"

[workspace.package]
version = "0.6.2"

[package]
name = "delta-arrow-reader"
version.workspace = true
edition = "2024"
description = "Release configuration fixture"
license = "Apache-2.0"
repository = "https://github.com/mag1cfrog/delta-arrow-reader"
exclude = {json.dumps(root_manifest["package"]["exclude"])}
''')
            (project / "src").mkdir()
            (project / "src/lib.rs").write_text("pub fn read() {}\n")
            (project / "pyproject.toml").write_text(
                '[project]\nname = "delta-arrow-reader"\nrequires-python = ">=3.10"\n'
            )
            (project / "delta_arrow_reader.pyi").write_text("def read() -> int: ...\n")
            for name, source, code in (
                ("python", "lib.rs", "pub fn python() {}\n"),
                ("cli", "main.rs", "fn main() {}\n"),
            ):
                member = project / "crates" / name
                (member / "src").mkdir(parents=True)
                (member / "src" / source).write_text(code)
                (member / "Cargo.toml").write_text(f'''\
[package]
name = "delta-arrow-reader-{name}"
version.workspace = true
edition = "2024"
publish = false

[dependencies]
delta-arrow-reader = {{ version = "0.6.2", path = "../.." }}
''')
            shutil.copyfile(ROOT / "release-plz.toml", project / "release-plz.toml")
            run("git", "init", "--initial-branch=main")
            run("git", "config", "user.name", "Release configuration test")
            run("git", "config", "user.email", "release-test@example.invalid")
            run("cargo", "generate-lockfile")
            run("git", "add", ".")
            run("git", "commit", "-m", "chore: initial release")
            run("git", "tag", "v0.6.2")
            run("git", "clone", "--quiet", "--no-hardlinks", ".", "../published")

            (project / changed_path).write_text(contents)
            run("git", "add", changed_path)
            run("git", "commit", "-m", "fix(python): update Python-only package input")
            self.assertEqual(
                run("git", "diff", "--name-only", "v0.6.2", "HEAD").splitlines(),
                [changed_path],
            )
            # The local baseline avoids contacting a registry or publishing anything.
            run(
                "release-plz", "update",
                "--registry-manifest-path", "../published/Cargo.toml",
                "--repo-url", "https://github.com/mag1cfrog/delta-arrow-reader",
                "--release-date", "2026-01-01",
            )

            metadata = json.loads(run("cargo", "metadata", "--no-deps", "--format-version=1"))
            for package in metadata["packages"]:
                with self.subTest(package=package["name"]):
                    self.assertEqual(package["version"], "0.6.3")
            changelog = project / "CHANGELOG.md"
            self.assertEqual(list(project.rglob("CHANGELOG.md")), [changelog])
            self.assertIn("update Python-only package input", changelog.read_text())
            self.assertIn("/compare/v0.6.2...v0.6.3", changelog.read_text())


if __name__ == "__main__":
    unittest.main()

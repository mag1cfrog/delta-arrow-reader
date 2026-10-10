import contextlib
import os
import subprocess
import tempfile
import unittest
from pathlib import Path
from unittest import mock

import build


class BuildTests(unittest.TestCase):
    def test_build_flags_cannot_inherit_host_cpu_or_compiler_overrides(self):
        overrides = {
            "PATH": "/bin",
            "HOME": "/test",
            "CARGO_BUILD_JOBS": "2",
            "RUSTFLAGS": "-Ctarget-cpu=native",
            "CARGO_ENCODED_RUSTFLAGS": "-Ctarget-cpu=native",
            "CARGO_TARGET_X86_64_UNKNOWN_LINUX_GNU_RUSTFLAGS": "-Ctarget-cpu=native",
            "CFLAGS": "-march=native",
            "CFLAGS_aarch64_unknown_linux_gnu": "-mcpu=native",
            "RUSTC": "/wrong/compiler",
            "RUSTC_WRAPPER": "/wrong/wrapper",
            "CARGO_PROFILE_RELEASE_OPT_LEVEL": "0",
            "CARGO_BUILD_TARGET": "wrong-target",
            "RUSTUP_TOOLCHAIN": "nightly",
            "GITHUB_TOKEN": "not-a-real-token",
        }
        with mock.patch.dict(os.environ, overrides, clear=True):
            for architecture in build.BASELINE_FLAGS:
                with self.subTest(architecture=architecture):
                    environment = build.build_environment(architecture, "1.94")
                    self.assertEqual(environment["RUSTUP_TOOLCHAIN"], "1.94")
                    self.assertEqual(environment["CARGO_BUILD_JOBS"], "2")
                    self.assertNotIn("native", " ".join(environment.values()))
                    for key in overrides:
                        if key not in {
                            "PATH",
                            "HOME",
                            "CARGO_BUILD_JOBS",
                            "RUSTFLAGS",
                            "CFLAGS",
                            "RUSTUP_TOOLCHAIN",
                        }:
                            self.assertNotIn(key, environment)

    def test_source_must_be_the_exact_clean_commit(self):
        with tempfile.TemporaryDirectory() as temporary, contextlib.chdir(temporary):
            subprocess.run(["git", "init", "--quiet"], check=True)
            source = Path("source.rs")
            source.write_text("original\n")
            Path(".gitignore").write_text("/target/\n")
            subprocess.run(["git", "add", "."], check=True)
            subprocess.run(
                [
                    "git",
                    "-c",
                    "user.name=Test",
                    "-c",
                    "user.email=test@example.invalid",
                    "-c",
                    "commit.gpgsign=false",
                    "commit",
                    "--quiet",
                    "-m",
                    "test: source fixture",
                ],
                check=True,
            )
            commit = subprocess.check_output(
                ["git", "rev-parse", "HEAD"], text=True
            ).strip()
            build.check_source(commit)
            for incorrect in ("main", commit[:12], "f" * 40):
                with self.subTest(commit=incorrect), self.assertRaises(ValueError):
                    build.check_source(incorrect)
            source.write_text("modified\n")
            with self.assertRaisesRegex(ValueError, "modified or untracked"):
                build.check_source(commit)
            source.write_text("original\n")
            extra = Path("untracked.rs")
            extra.touch()
            with self.assertRaisesRegex(ValueError, "modified or untracked"):
                build.check_source(commit)
            extra.unlink()
            Path("target").mkdir()
            Path("target/build-info.json").write_text("{}")
            build.check_source(commit)


if __name__ == "__main__":
    unittest.main()

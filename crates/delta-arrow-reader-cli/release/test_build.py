import contextlib
import os
import subprocess
import tempfile
import unittest
from pathlib import Path
from unittest import mock

import build
import build_archive
from artifacts import RUST_TARGETS, build_identity, file_sha256


class BuildTests(unittest.TestCase):
    def test_archive_rejects_unexpected_build_identity_before_packaging(self):
        identity = build_identity("1.2.3", "a" * 40, "1.94.1")
        with tempfile.TemporaryDirectory() as temporary:
            output = Path(temporary) / "artifacts"
            for field, value in (
                ("version", "1.2.4"),
                ("source_commit", "b" * 40),
                ("rust_toolchain", "1.94.2"),
            ):
                with (
                    self.subTest(field=field),
                    mock.patch(
                        "build_archive.build",
                        return_value=dict(identity, **{field: value}),
                    ),
                    mock.patch("build_archive.generate_notices") as notices,
                    self.assertRaisesRegex(ValueError, "differs"),
                ):
                    build_archive.build_archive(
                        identity, "amd64", Path(temporary), output
                    )
                notices.assert_not_called()
                self.assertFalse(output.exists())

    def test_archive_rejects_binary_replacement_after_build_validation(self):
        identity = build_identity("1.2.3", "a" * 40, "1.94.1")
        with tempfile.TemporaryDirectory() as temporary:
            target = Path(temporary)
            binary = target / RUST_TARGETS["amd64"] / "release/dar"
            binary.parent.mkdir(parents=True)
            binary.write_bytes(b"validated binary")
            metadata = dict(
                identity,
                dar_sha256=file_sha256(binary),
                runtime_dependencies=["libc.so.6"],
            )

            def replace_binary(_architecture, *, rust_toolchain):
                self.assertEqual(rust_toolchain, identity["rust_toolchain"])
                binary.write_bytes(b"replacement binary")
                return "Dependency license and copyright.\n"

            with (
                mock.patch("build_archive.build", return_value=metadata),
                mock.patch("build_archive.check_source"),
                mock.patch(
                    "build_archive.generate_notices", side_effect=replace_binary
                ),
                self.assertRaisesRegex(ValueError, "differs from the validated build"),
            ):
                build_archive.build_archive(
                    identity, "amd64", target, target / "artifacts"
                )

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

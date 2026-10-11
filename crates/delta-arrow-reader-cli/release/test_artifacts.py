import copy
import gzip
import hashlib
import io
import json
import os
import subprocess
import sys
import tarfile
import tempfile
import unittest
from pathlib import Path

import artifacts


class ArtifactTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.inputs = self.root / "inputs"
        self.output = self.root / "output"
        self.identity = artifacts.build_identity("1.2.3", "a" * 40, "1.94.0")
        self.dependencies = ["libc.so.6", "libgcc_s.so.1"]
        self.notices = self.root / "dependency-notices.txt"
        self.notices.write_bytes(b"Test dependency license and copyright.\n")
        # These bytes exercise packaging only; they are not release executables.
        for architecture in artifacts.RUST_TARGETS:
            binary = self.root / f"dar-{architecture}"
            binary.write_bytes(f"test {architecture} binary\n".encode())
            artifacts.package(
                binary,
                self.notices,
                self.inputs / architecture,
                self.identity,
                architecture,
                self.dependencies,
            )

    def assemble(self):
        artifacts.assemble(self.inputs, self.output, self.identity)

    def read_manifest(self, directory):
        return json.loads((directory / artifacts.MANIFEST).read_text())

    def write_manifest(self, directory, manifest):
        (directory / artifacts.MANIFEST).write_text(json.dumps(manifest))

    def test_complete_set_preserves_bytes_and_matches_sha256sum(self):
        self.assemble()
        artifacts.verify(self.output, self.identity)
        manifest = self.read_manifest(self.output)
        self.assertEqual(
            {key: value for key, value in manifest.items() if key != "targets"},
            self.identity,
        )
        self.assertEqual(
            [target["architecture"] for target in manifest["targets"]],
            ["amd64", "arm64"],
        )
        self.assertEqual(
            manifest["targets"][0]["rust_target"], "x86_64-unknown-linux-gnu"
        )
        self.assertEqual(
            manifest["targets"][1]["rust_target"], "aarch64-unknown-linux-gnu"
        )
        for target in manifest["targets"]:
            self.assertEqual(target["os"], "linux")
            self.assertEqual(target["libc"], "glibc")
            self.assertEqual(target["min_glibc"], "2.36")
            self.assertEqual(target["runtime_dependencies"], self.dependencies)
            name = f"dar-v1.2.3-linux-{target['architecture']}-gnu.tar.gz"
            self.assertEqual(target["archive"], name)
            archive_bytes = (self.output / name).read_bytes()
            self.assertEqual(
                archive_bytes,
                (self.inputs / target["architecture"] / name).read_bytes(),
            )
            self.assertEqual(
                hashlib.sha256(archive_bytes).hexdigest(), target["archive_sha256"]
            )
            with tarfile.open(self.output / name) as archive:
                self.assertEqual(archive.getnames(), ["dar", "LICENSE", "NOTICE"])
                self.assertEqual(
                    [
                        (member.mode, member.mtime, member.uid, member.gid)
                        for member in archive
                    ],
                    [(0o755, 0, 0, 0), (0o644, 0, 0, 0), (0o644, 0, 0, 0)],
                )
                dar = archive.extractfile("dar").read()
                self.assertEqual(
                    dar, (self.root / f"dar-{target['architecture']}").read_bytes()
                )
                self.assertEqual(hashlib.sha256(dar).hexdigest(), target["dar_sha256"])
                self.assertEqual(
                    archive.extractfile("LICENSE").read(),
                    (artifacts.REPOSITORY / "LICENSE").read_bytes(),
                )
                self.assertEqual(
                    archive.extractfile("NOTICE").read(),
                    (artifacts.REPOSITORY / "NOTICE").read_bytes()
                    + b"\n"
                    + self.notices.read_bytes(),
                )
        subprocess.run(
            ["sha256sum", "--check", "SHA256SUMS"],
            cwd=self.output,
            check=True,
            capture_output=True,
        )

    def test_archives_are_reproducible_from_identical_inputs(self):
        binary = self.root / "dar-amd64"
        binary.chmod(0o700)
        os.utime(binary, (123456, 123456))
        other = self.root / "other"
        artifacts.package(
            binary, self.notices, other, self.identity, "amd64", self.dependencies
        )
        for path in other.iterdir():
            self.assertEqual(
                path.read_bytes(), (self.inputs / "amd64" / path.name).read_bytes()
            )

    def test_extract_checks_identity_preserves_bytes_and_never_overwrites(self):
        for architecture in artifacts.RUST_TARGETS:
            destination = self.root / f"extracted-{architecture}"
            source = self.inputs / architecture
            wrong_identity = dict(self.identity, source_commit="b" * 40)
            with self.assertRaises(ValueError):
                artifacts.extract(source, destination, wrong_identity, architecture)
            self.assertFalse(destination.exists())
            artifacts.extract(source, destination, self.identity, architecture)
            artifacts.check_files(destination, artifacts.MEMBER_MODES)
            self.assertEqual(
                (destination / "dar").read_bytes(),
                (self.root / f"dar-{architecture}").read_bytes(),
            )
            for name, mode in artifacts.MEMBER_MODES.items():
                self.assertEqual((destination / name).stat().st_mode & 0o7777, mode)
            with self.assertRaises(FileExistsError):
                artifacts.extract(source, destination, self.identity, architecture)

    def test_outputs_are_never_overwritten(self):
        self.assemble()
        existing = (self.output / artifacts.MANIFEST).read_bytes()
        with self.assertRaises(FileExistsError):
            self.assemble()
        self.assertEqual((self.output / artifacts.MANIFEST).read_bytes(), existing)
        with self.assertRaises(FileExistsError):
            artifacts.package(
                self.root / "dar-amd64",
                self.notices,
                self.inputs / "amd64",
                self.identity,
                "amd64",
                self.dependencies,
            )

    def test_binary_larger_than_io_buffers_preserves_its_hash(self):
        payload = bytes(range(256)) * 8193
        binary = self.root / "large-dar"
        binary.write_bytes(payload)
        output = self.root / "large-package"
        artifacts.package(
            binary, self.notices, output, self.identity, "amd64", self.dependencies
        )
        target = self.read_manifest(output)["targets"][0]
        self.assertEqual(target["dar_sha256"], hashlib.sha256(payload).hexdigest())

    def test_missing_or_extra_architecture_fails_before_output(self):
        arm64 = self.inputs / "arm64"
        moved = self.root / "saved-arm64"
        arm64.rename(moved)
        with self.assertRaisesRegex(ValueError, "exactly the amd64 and arm64"):
            self.assemble()
        moved.rename(arm64)
        (self.inputs / "extra").mkdir()
        with self.assertRaisesRegex(ValueError, "exactly the amd64 and arm64"):
            self.assemble()
        self.assertFalse(self.output.exists())

    def test_manifest_rejects_mismatched_identity_and_target_metadata(self):
        directory = self.inputs / "amd64"
        original = self.read_manifest(directory)
        changes = [
            (None, "schema_version", True),
            (None, "schema_version", 1.0),
            (None, "source_commit", "b" * 40),
            (None, "version", "1.2.4"),
            (None, "rust_toolchain", "1.95.0"),
            (None, "extra", "value"),
            (0, "architecture", "arm64"),
            (0, "rust_target", "aarch64-unknown-linux-gnu"),
            (0, "min_glibc", "2.39"),
            (0, "archive", "../dar.tar.gz"),
            (0, "archive_sha256", "0" * 64),
            (0, "dar_sha256", "0" * 64),
            (0, "runtime_dependencies", []),
            (0, "runtime_dependencies", "libc.so.6"),
            (0, "runtime_dependencies", ["libc.so.6", "libc.so.6"]),
            (0, "runtime_dependencies", [None]),
        ]
        for target, key, value in changes:
            with self.subTest(key=key, value=value):
                manifest = copy.deepcopy(original)
                (manifest if target is None else manifest["targets"][target])[key] = (
                    value
                )
                self.write_manifest(directory, manifest)
                with self.assertRaises(ValueError):
                    self.assemble()
                self.assertFalse(self.output.exists())
        self.write_manifest(directory, original)

    def test_manifest_rejects_malformed_shapes_and_duplicate_fields(self):
        path = self.inputs / "amd64" / artifacts.MANIFEST
        original = path.read_text()
        for text in (
            "{",
            "[]",
            "null",
            "{}",
            original.replace(
                '"schema_version": 1', '"schema_version": 1, "schema_version": 1'
            ),
            json.dumps({**self.identity, "targets": []}),
            json.dumps({**self.identity, "targets": {}}),
            json.dumps({**self.identity, "targets": [None]}),
        ):
            with self.subTest(text=text[:80]):
                path.write_text(text)
                with self.assertRaises(ValueError):
                    self.assemble()

    def test_incomplete_or_unexpected_public_files_are_rejected(self):
        self.assemble()
        for name in [path.name for path in self.output.iterdir()]:
            with self.subTest(missing=name):
                saved = self.root / "saved"
                (self.output / name).rename(saved)
                with self.assertRaises(ValueError):
                    artifacts.verify(self.output, self.identity)
                saved.rename(self.output / name)
        (self.output / "debug-symbols").write_bytes(b"extra")
        with self.assertRaises(ValueError):
            artifacts.verify(self.output, self.identity)

    def test_symlink_artifacts_and_directories_are_rejected(self):
        directory = self.inputs / "amd64"
        for path in [directory / artifacts.MANIFEST, directory]:
            with self.subTest(path=path):
                saved = self.root / "saved"
                path.rename(saved)
                path.symlink_to(saved)
                with self.assertRaises(ValueError):
                    self.assemble()
                path.unlink()
                saved.rename(path)

    def test_corrupted_archives_and_checksum_files_are_rejected(self):
        self.assemble()
        archive = self.output / "dar-v1.2.3-linux-amd64-gnu.tar.gz"
        original = archive.read_bytes()
        for data in (b"not gzip", original[: len(original) // 2], original + b"extra"):
            with self.subTest(data=data[:10]):
                archive.write_bytes(data)
                with self.assertRaises(
                    (OSError, ValueError, tarfile.TarError, EOFError)
                ):
                    artifacts.verify(self.output, self.identity)
        archive.write_bytes(original)
        sums = self.output / "SHA256SUMS"
        original = sums.read_text()
        for text in ("", original.upper(), original + "0" * 64 + "  SHA256SUMS\n"):
            with self.subTest(text=text[:10]):
                sums.write_text(text)
                with self.assertRaisesRegex(ValueError, "SHA256SUMS"):
                    artifacts.verify(self.output, self.identity)

    def test_gzip_footer_and_tar_end_blocks_are_checked(self):
        directory = self.inputs / "amd64"
        path = directory / "dar-v1.2.3-linux-amd64-gnu.tar.gz"
        original = path.read_bytes()
        bad_crc = bytearray(original)
        bad_crc[-8] ^= 1
        bad_size = bytearray(original)
        bad_size[-4] ^= 1
        uncompressed = gzip.decompress(original)
        with tarfile.open(path) as archive:
            last = archive.getmembers()[-1]
            content_end = last.offset_data + last.size
        for name, data in (
            ("bad CRC", bad_crc),
            ("bad size", bad_size),
            ("missing gzip footer", original[:-8]),
            ("trailing bytes", original + b"extra"),
            ("hidden gzip member", original + gzip.compress(b"hidden tar payload")),
            ("missing tar end blocks", gzip.compress(uncompressed[:content_end])),
        ):
            with self.subTest(name=name):
                path.write_bytes(data)
                with self.assertRaises(
                    (OSError, ValueError, tarfile.TarError, EOFError)
                ):
                    artifacts.describe_archive(
                        directory, self.identity, "amd64", self.dependencies
                    )

    def test_unsafe_or_incomplete_archive_members_are_rejected(self):
        directory = self.inputs / "amd64"
        path = directory / "dar-v1.2.3-linux-amd64-gnu.tar.gz"
        with tarfile.open(path) as archive:
            originals = [
                (copy.copy(member), archive.extractfile(member).read())
                for member in archive
            ]
        mutations = [
            ("name", "../dar"),
            ("name", "/dar"),
            ("name", "folder/dar"),
            ("type", tarfile.SYMTYPE),
            ("type", tarfile.LNKTYPE),
            ("type", tarfile.DIRTYPE),
            ("type", tarfile.FIFOTYPE),
            ("mode", 0o644),
            ("mode", 0o4755),
            ("size", 0),
        ]
        for field, value in mutations:
            with self.subTest(field=field, value=value):
                members = copy.deepcopy(originals)
                setattr(members[0][0], field, value)
                with tarfile.open(path, "w:gz") as archive:
                    for member, data in members:
                        archive.addfile(member, io.BytesIO(data))
                with self.assertRaises(ValueError):
                    artifacts.describe_archive(
                        directory, self.identity, "amd64", self.dependencies
                    )
        for members in (originals[:2], originals + originals[:1], originals[::-1]):
            with self.subTest(names=[member.name for member, _ in members]):
                with tarfile.open(path, "w:gz") as archive:
                    for member, data in members:
                        archive.addfile(member, io.BytesIO(data))
                with self.assertRaises(ValueError):
                    artifacts.describe_archive(
                        directory, self.identity, "amd64", self.dependencies
                    )

    def test_repository_license_and_dependency_notices_are_required(self):
        directory = self.inputs / "amd64"
        path = directory / "dar-v1.2.3-linux-amd64-gnu.tar.gz"
        with tarfile.open(path) as archive:
            originals = [
                (copy.copy(member), archive.extractfile(member).read())
                for member in archive
            ]
        for name, replacement in (
            ("LICENSE", b"wrong license"),
            ("NOTICE", self.notices.read_bytes()),
            ("NOTICE", (artifacts.REPOSITORY / "NOTICE").read_bytes() + b"\n"),
        ):
            with self.subTest(name=name, replacement=replacement):
                with tarfile.open(path, "w:gz") as archive:
                    for original, data in originals:
                        member = copy.copy(original)
                        if member.name == name:
                            data = replacement
                        member.size = len(data)
                        archive.addfile(member, io.BytesIO(data))
                with self.assertRaises(ValueError):
                    artifacts.describe_archive(
                        directory, self.identity, "amd64", self.dependencies
                    )

    def test_cli_validates_identity_and_reports_failures_without_tracebacks(self):
        command = [sys.executable, str(Path(artifacts.__file__))]
        identity = [
            "--version",
            "1.2.3",
            "--source-commit",
            "a" * 40,
            "--rust-toolchain",
            "1.94.0",
        ]
        for args in (
            [
                "package",
                str(self.root / "cli-package"),
                "--binary",
                str(self.root / "dar-amd64"),
                "--dependency-notices",
                str(self.notices),
                "--architecture",
                "amd64",
                "--runtime-dependency",
                "libc.so.6",
            ],
            ["assemble", str(self.output), "--inputs", str(self.inputs)],
            ["verify", str(self.output)],
        ):
            result = subprocess.run(
                command + args + identity, check=False, capture_output=True, text=True
            )
            self.assertEqual(result.returncode, 0, result.stderr)
        for flag, value in (
            ("--version", "../1.2.3"),
            ("--source-commit", "main"),
            ("--rust-toolchain", "stable"),
        ):
            args = identity.copy()
            args[args.index(flag) + 1] = value
            result = subprocess.run(
                command + ["verify", str(self.output)] + args,
                check=False,
                capture_output=True,
                text=True,
            )
            self.assertEqual(result.returncode, 1)
            self.assertIn("invalid", result.stderr)
            self.assertNotIn("Traceback", result.stderr)


if __name__ == "__main__":
    unittest.main()

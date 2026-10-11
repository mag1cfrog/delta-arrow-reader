import os
import platform
import struct
import subprocess
import tempfile
import unittest
from pathlib import Path
from unittest import mock

import check_binary
from artifacts import file_sha256


def readelf_output(architecture="amd64"):
    interpreter = check_binary.ELF_PLATFORMS[architecture]["interpreter"]
    return f"""
      [Requesting program interpreter: {interpreter}]
 0x0000000000000001 (NEEDED)             Shared library: [libc.so.6]
 0x0010:   Name: GLIBC_2.17  Flags: none  Version: 2
 0x0020:   Name: GLIBC_2.36  Flags: none  Version: 3
"""


class ElfTests(unittest.TestCase):
    def test_supported_headers_and_runtime_requirements(self):
        for architecture, settings in check_binary.ELF_PLATFORMS.items():
            with self.subTest(architecture=architecture):
                machine = settings["elf_machine"]
                header = (
                    b"\x7fELF\x02\x01\x01" + bytes(9) + struct.pack("<HH", 3, machine)
                )
                check_binary.check_elf_header(header, architecture)
                metadata = check_binary.check_readelf_output(
                    readelf_output(architecture), architecture
                )
                self.assertEqual(
                    metadata,
                    {
                        "interpreter": settings["interpreter"],
                        "runtime_dependencies": ["libc.so.6"],
                        "max_glibc": "2.36",
                    },
                )
                for invalid in (
                    header[:19],
                    b"not an ELF executable",
                    header[:4] + b"\x01" + header[5:],
                    header[:5] + b"\x02" + header[6:],
                    header[:16] + struct.pack("<HH", 1, machine),
                ):
                    with self.assertRaises(ValueError):
                        check_binary.check_elf_header(invalid, architecture)
                other = "arm64" if architecture == "amd64" else "amd64"
                with self.assertRaises(ValueError):
                    check_binary.check_elf_header(header, other)

    def test_unsafe_loader_paths_dependencies_and_symbol_requirements_fail(self):
        original = readelf_output()
        for name, output in (
            (
                "wrong interpreter",
                original.replace("/lib64/ld-linux-x86-64.so.2", "/tmp/loader"),
            ),
            ("missing interpreter", original.splitlines()[2:]),
            ("RPATH", original + "0x001d (RPATH) Library rpath: [/tmp]"),
            ("RUNPATH", original + "0x001d (RUNPATH) Library runpath: [$ORIGIN]"),
            ("unexpected library", original.replace("libc.so.6", "libssl.so.3")),
            ("library path", original.replace("libc.so.6", "/tmp/libc.so.6")),
            ("missing library", original.replace("(NEEDED)", "(UNUSED)")),
            ("duplicate library", original + "(NEEDED) Shared library: [libc.so.6]"),
            ("newer glibc", original.replace("GLIBC_2.36", "GLIBC_2.39")),
            (
                "glibc patch above baseline",
                original.replace("GLIBC_2.36", "GLIBC_2.36.1"),
            ),
            ("private ABI", original.replace("GLIBC_2.36", "GLIBC_PRIVATE")),
            ("unknown ABI", original.replace("GLIBC_2.36", "GLIBC_FUTURE")),
            ("missing symbol requirements", original.replace("Name:", "Other:")),
            (
                "higher CPU requirement",
                original + "Properties: x86 ISA needed: x86-64-v2",
            ),
        ):
            with self.subTest(name=name):
                if isinstance(output, list):
                    output = "\n".join(output)
                with self.assertRaises(ValueError):
                    check_binary.check_readelf_output(output, "amd64")

    def test_glibc_versions_are_compared_numerically(self):
        for version in ("2.9", "2.2.5", "2.35", "2.36.0"):
            with self.subTest(version=version):
                check_binary.check_readelf_output(
                    readelf_output().replace("2.36", version), "amd64"
                )

    def test_environment_must_match_the_supported_platform(self):
        with (
            mock.patch.object(
                platform,
                "freedesktop_os_release",
                return_value={"ID": "debian", "VERSION_ID": "12"},
            ) as release,
            mock.patch.object(os, "confstr", return_value="glibc 2.36") as libc,
            mock.patch.object(platform, "machine", return_value="x86_64") as machine,
        ):
            check_binary.check_environment("amd64")
            for patched, value in (
                (release, {"ID": "ubuntu", "VERSION_ID": "24.04"}),
                (release, {"ID": "debian", "VERSION_ID": "13"}),
                (libc, "glibc 2.39"),
                (machine, "aarch64"),
            ):
                with self.subTest(value=value):
                    original = patched.return_value
                    patched.return_value = value
                    with self.assertRaises(ValueError):
                        check_binary.check_environment("amd64")
                    patched.return_value = original


class ExecutableTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        temporary = tempfile.TemporaryDirectory()
        cls.addClassCleanup(temporary.cleanup)
        cls.binary = Path(temporary.name) / "dar"
        cls.architecture = {"x86_64": "amd64", "aarch64": "arm64"}[platform.machine()]
        subprocess.run(
            ["cc", "-x", "c", "-", "-o", str(cls.binary)],
            input=(
                b'#include <stdio.h>\nint main(void) { puts("dar 1.2.3"); return 0; }\n'
            ),
            check=True,
            capture_output=True,
        )
        # Drop distro-specific compiler notes from this synthetic test executable.
        subprocess.run(
            [
                "objcopy",
                "--remove-section=.gnu.build.attributes",
                "--remove-section=.note.gnu.property",
                str(cls.binary),
            ],
            check=True,
            capture_output=True,
        )

    def test_real_elf_inspection_and_version_execution(self):
        expected_hash = file_sha256(self.binary)
        with mock.patch.object(check_binary, "check_environment"):
            metadata = check_binary.check_binary(
                self.binary, self.architecture, "1.2.3", expected_hash
            )
            self.assertEqual(metadata["dar_sha256"], expected_hash)
            self.assertIn("libc.so.6", metadata["runtime_dependencies"])
            with self.assertRaisesRegex(ValueError, "does not report exactly"):
                check_binary.check_binary(
                    self.binary, self.architecture, "1.2.4", expected_hash
                )

    def test_hash_mismatch_prevents_execution(self):
        with (
            mock.patch.object(check_binary, "check_environment"),
            mock.patch.object(subprocess, "run", wraps=subprocess.run) as run,
        ):
            with self.assertRaisesRegex(ValueError, "SHA-256 differs"):
                check_binary.check_binary(
                    self.binary, self.architecture, "1.2.3", "0" * 64
                )
            self.assertEqual(len(run.call_args_list), 1)
            self.assertEqual(run.call_args.args[0][0], "readelf")

    def test_unexpected_permission_bits_and_symlinks_fail(self):
        original = self.binary.stat().st_mode
        try:
            for mode in (0o644, 0o4755):
                self.binary.chmod(mode)
                with self.assertRaises(ValueError):
                    check_binary.inspect_elf(self.binary, self.architecture)
        finally:
            self.binary.chmod(original)
        link = self.binary.with_name("link")
        link.symlink_to(self.binary)
        with self.assertRaises(ValueError):
            check_binary.inspect_elf(link, self.architecture)


if __name__ == "__main__":
    unittest.main()

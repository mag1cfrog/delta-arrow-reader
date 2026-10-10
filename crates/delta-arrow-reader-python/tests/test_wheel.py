from pathlib import Path
import tempfile
import unittest
from zipfile import ZipFile

from check_wheel import ARTIFACT_PLATFORM_TAGS, check_wheel, check_wheel_set


class WheelTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.directory = Path(temporary.name)
        self.platform_tag = "manylinux_2_28_x86_64"
        self.path = self.directory / (
            "delta_arrow_reader-1.2.3-cp310-abi3-manylinux_2_28_x86_64.whl"
        )
        self.metadata_path = "delta_arrow_reader-1.2.3.dist-info/METADATA"
        self.wheel_path = "delta_arrow_reader-1.2.3.dist-info/WHEEL"
        self.files = {
            self.metadata_path: (
                "Metadata-Version: 2.4\nName: delta-arrow-reader\nVersion: 1.2.3\n"
            ),
            self.wheel_path: (
                "Wheel-Version: 1.0\nRoot-Is-Purelib: false\n"
                "Tag: cp310-abi3-manylinux_2_28_x86_64\n"
            ),
        }

    def write_wheel(self, path, files):
        with ZipFile(path, "w") as wheel:
            for name, content in files:
                wheel.writestr(name, content)

    def test_accepts_expected_platform_tags(self):
        for platform_tag in (
            "manylinux_2_28_x86_64",
            "win_amd64",
            "macosx_12_0_arm64",
            "macosx_12_0_x86_64",
        ):
            with self.subTest(platform_tag=platform_tag):
                path = self.directory / (
                    f"delta_arrow_reader-1.2.3-cp310-abi3-{platform_tag}.whl"
                )
                files = dict(self.files)
                files[self.wheel_path] = files[self.wheel_path].replace(
                    self.platform_tag, platform_tag
                )
                self.write_wheel(path, files.items())
                check_wheel(path, "1.2.3", platform_tag)

    def test_rejects_unexpected_filename(self):
        for old, new in (
            ("delta_arrow_reader", "other_package"),
            ("1.2.3", "1.2.4"),
            ("cp310", "cp311"),
            ("abi3", "cp310"),
            (self.platform_tag, "win_amd64"),
        ):
            with self.subTest(old=old, new=new):
                path = self.path.with_name(self.path.name.replace(old, new))
                self.write_wheel(path, self.files.items())
                with self.assertRaisesRegex(ValueError, "expected wheel"):
                    check_wheel(path, "1.2.3", self.platform_tag)

    def test_rejects_inconsistent_headers(self):
        tag = "Tag: cp310-abi3-manylinux_2_28_x86_64\n"
        for member, original, replacement in (
            (self.metadata_path, "Name: delta-arrow-reader\n", "Name: other-package\n"),
            (self.metadata_path, "Version: 1.2.3\n", "Version: 1.2.4\n"),
            (
                self.metadata_path,
                "Version: 1.2.3\n",
                "Version: 1.2.3\nVersion: 1.2.3\n",
            ),
            (self.wheel_path, "Root-Is-Purelib: false\n", "Root-Is-Purelib: true\n"),
            (self.wheel_path, tag, "Tag: cp310-abi3-win_amd64\n"),
            (self.wheel_path, tag, ""),
            (self.wheel_path, tag, tag + "Tag: cp310-abi3-win_amd64\n"),
        ):
            with self.subTest(member=member, replacement=replacement):
                files = dict(self.files)
                files[member] = files[member].replace(original, replacement)
                self.write_wheel(self.path, files.items())
                with self.assertRaisesRegex(ValueError, "expected one"):
                    check_wheel(self.path, "1.2.3", self.platform_tag)

    def test_rejects_missing_or_foreign_metadata(self):
        for member in self.files:
            missing = dict(self.files)
            del missing[member]
            foreign = dict(self.files)
            foreign[member.replace("1.2.3", "1.2.4")] = self.files[member]
            for problem, files in (("missing", missing), ("foreign", foreign)):
                with self.subTest(member=member, problem=problem):
                    self.write_wheel(self.path, files.items())
                    with self.assertRaisesRegex(ValueError, "expected exactly one"):
                        check_wheel(self.path, "1.2.3", self.platform_tag)

    def test_rejects_duplicate_metadata_files(self):
        for member, content in self.files.items():
            with self.subTest(member=member):
                files = list(self.files.items()) + [(member, content)]
                with self.assertWarns(UserWarning):
                    self.write_wheel(self.path, files)
                with self.assertRaisesRegex(ValueError, "expected exactly one"):
                    check_wheel(self.path, "1.2.3", self.platform_tag)

    def write_artifact_set(self):
        paths = []
        for artifact, platform_tag in ARTIFACT_PLATFORM_TAGS.items():
            directory = self.directory / artifact
            directory.mkdir()
            path = directory / f"delta_arrow_reader-1.2.3-cp310-abi3-{platform_tag}.whl"
            files = dict(self.files)
            files[self.wheel_path] = files[self.wheel_path].replace(
                self.platform_tag, platform_tag
            )
            self.write_wheel(path, files.items())
            paths.append(path)
        return paths

    def test_accepts_complete_artifact_set(self):
        self.write_artifact_set()
        check_wheel_set(self.directory, "1.2.3")

    def test_rejects_missing_platform_artifact(self):
        for path in self.write_artifact_set():
            with self.subTest(artifact=path.parent.name):
                contents = path.read_bytes()
                path.unlink()
                path.parent.rmdir()
                with self.assertRaisesRegex(ValueError, "expected artifacts"):
                    check_wheel_set(self.directory, "1.2.3")
                path.parent.mkdir()
                path.write_bytes(contents)

    def test_rejects_unexpected_platform_artifact(self):
        self.write_artifact_set()
        (self.directory / "unexpected-artifact").mkdir()
        with self.assertRaisesRegex(ValueError, "expected artifacts"):
            check_wheel_set(self.directory, "1.2.3")

    def test_rejects_empty_or_extra_artifact_contents(self):
        path = self.write_artifact_set()[0]
        contents = path.read_bytes()
        path.unlink()
        with self.assertRaisesRegex(ValueError, "expected one wheel"):
            check_wheel_set(self.directory, "1.2.3")
        path.write_bytes(contents)
        path.with_name("extra.whl").touch()
        with self.assertRaisesRegex(ValueError, "expected one wheel"):
            check_wheel_set(self.directory, "1.2.3")

    def test_rejects_wheel_in_wrong_artifact(self):
        paths = self.write_artifact_set()
        paths[0].rename(paths[0].with_name(paths[1].name))
        with self.assertRaisesRegex(ValueError, "expected wheel"):
            check_wheel_set(self.directory, "1.2.3")

    def test_rejects_wrong_artifact_version_or_metadata(self):
        path = self.write_artifact_set()[0]
        with self.assertRaisesRegex(ValueError, "expected wheel"):
            check_wheel_set(self.directory, "1.2.4")
        files = dict(self.files)
        files[self.metadata_path] = files[self.metadata_path].replace("1.2.3", "1.2.4")
        self.write_wheel(path, files.items())
        with self.assertRaisesRegex(ValueError, "METADATA: expected one Version"):
            check_wheel_set(self.directory, "1.2.3")


if __name__ == "__main__":
    unittest.main()

import hashlib
import importlib.util
import io
import json
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
from urllib.error import HTTPError, URLError
from zipfile import ZipFile


SCRIPT = Path(__file__).resolve().parents[1] / "scripts/prepare_python_upload.py"
SPEC = importlib.util.spec_from_file_location("prepare_python_upload", SCRIPT)
upload = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(upload)


class PythonUploadTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory(prefix="python-upload-recovery-")
        self.addCleanup(temporary.cleanup)
        self.directory = Path(temporary.name)
        self.artifacts = self.directory / "artifacts"
        self.destination = self.directory / "dist"
        self.wheels = []
        for platform, tag in (
            ("linux-x86_64", "manylinux_2_28_x86_64"),
            ("windows-x86_64", "win_amd64"),
            ("macos-arm64", "macosx_12_0_arm64"),
            ("macos-x86_64", "macosx_12_0_x86_64"),
        ):
            artifact = self.artifacts / f"delta-arrow-reader-{platform}-wheel"
            artifact.mkdir(parents=True)
            path = artifact / f"delta_arrow_reader-1.2.3-cp310-abi3-{tag}.whl"
            with ZipFile(path, "w") as wheel:
                wheel.writestr(
                    "delta_arrow_reader-1.2.3.dist-info/METADATA",
                    "Metadata-Version: 2.4\nName: delta-arrow-reader\nVersion: 1.2.3\n",
                )
                wheel.writestr(
                    "delta_arrow_reader-1.2.3.dist-info/WHEEL",
                    f"Wheel-Version: 1.0\nRoot-Is-Purelib: false\nTag: cp310-abi3-{tag}\n",
                )
            self.wheels.append(path)
        self.release = {
            "info": {"name": "delta-arrow-reader", "version": "1.2.3"},
            "urls": [],
        }
        request_patch = patch.object(upload, "urlopen")
        self.request = request_patch.start()
        self.addCleanup(request_patch.stop)
        self.request.side_effect = lambda *args, **kwargs: io.BytesIO(
            json.dumps(self.release).encode()
        )

    def published(self, path):
        return {
            "filename": path.name,
            "digests": {"sha256": hashlib.sha256(path.read_bytes()).hexdigest()},
            "yanked": False,
        }

    def stage(self, index="pypi"):
        return upload.stage_upload(self.artifacts, "1.2.3", index, self.destination)

    def assert_staged(self, wheels):
        self.assertEqual(
            sorted(path.name for path in self.destination.iterdir()),
            sorted(path.name for path in wheels),
        )
        for path in self.wheels:
            self.assertTrue(path.exists(), "recovery must preserve the source artifacts")
        for path in wheels:
            self.assertEqual((self.destination / path.name).read_bytes(), path.read_bytes())

    def test_new_release_stages_all_wheels(self):
        self.request.side_effect = HTTPError("https://pypi.org", 404, "Not Found", {}, None)
        self.assertEqual(self.stage(), 4)
        self.assert_staged(self.wheels)

    def test_partial_release_stages_only_missing_wheels(self):
        self.release["urls"] = [self.published(path) for path in self.wheels[:2]]
        self.assertEqual(self.stage(), 2)
        self.assert_staged(self.wheels[2:])

    def test_complete_release_has_nothing_to_upload(self):
        self.release["urls"] = [self.published(path) for path in self.wheels]
        self.assertEqual(self.stage(), 0)
        self.assert_staged([])

    def test_hash_mismatch_stops_before_staging(self):
        self.release["urls"] = [self.published(path) for path in self.wheels[:2]]
        self.release["urls"][-1]["digests"]["sha256"] = "0" * 64
        with self.assertRaisesRegex(ValueError, "SHA256 mismatch.*new version"):
            self.stage()
        self.assertFalse(self.destination.exists())

    def test_unexpected_published_file_stops_before_staging(self):
        self.release["urls"] = [{**self.published(self.wheels[0]), "filename": "extra.whl"}]
        with self.assertRaisesRegex(ValueError, "unexpected published files"):
            self.stage()
        self.assertFalse(self.destination.exists())

    def test_invalid_remote_metadata_stops_before_staging(self):
        entry = self.published(self.wheels[0])
        for change in (
            {"info": {"name": "other-package", "version": "1.2.3"}},
            {"info": {"name": "delta-arrow-reader", "version": "1.2.4"}},
            {"info": None}, {"urls": {}}, {"urls": None},
            {"urls": [entry, entry]}, {"urls": [{}]},
            {"urls": [{**entry, "filename": None}]},
            {"urls": [{**entry, "digests": {}}]},
            {"urls": [{**entry, "digests": {"sha256": "invalid"}}]},
            {"urls": [{**entry, "digests": {"sha256": None}}]},
            {"urls": [{**entry, "yanked": True}]},
            {"urls": [{**entry, "yanked": None}]},
        ):
            with self.subTest(change=change):
                self.release = {
                    "info": {"name": "delta-arrow-reader", "version": "1.2.3"},
                    "urls": [], **change,
                }
                with self.assertRaises(ValueError):
                    self.stage()
                self.assertFalse(self.destination.exists())

    def test_network_errors_do_not_look_like_an_unpublished_version(self):
        for error in (
            HTTPError("https://pypi.org", 403, "Forbidden", {}, None),
            HTTPError("https://pypi.org", 503, "Unavailable", {}, None),
            URLError("connection failed"), TimeoutError("timed out"),
        ):
            with self.subTest(error=error):
                self.request.side_effect = error
                with self.assertRaises(OSError):
                    self.stage()
                self.assertFalse(self.destination.exists())

    def test_invalid_json_stops_before_staging(self):
        self.request.side_effect = lambda *args, **kwargs: io.BytesIO(b"not JSON")
        with self.assertRaises(ValueError):
            self.stage()
        self.assertFalse(self.destination.exists())

    def test_incomplete_artifacts_stop_before_contacting_index(self):
        self.wheels[0].unlink()
        with self.assertRaisesRegex(ValueError, "expected one wheel"):
            self.stage()
        self.request.assert_not_called()
        self.assertFalse(self.destination.exists())

    def test_existing_destination_is_not_reused(self):
        self.destination.mkdir()
        stale = self.destination / "unvalidated.whl"
        stale.write_bytes(b"stale")
        with self.assertRaises(FileExistsError):
            self.stage()
        self.assertEqual(list(self.destination.iterdir()), [stale])
        self.assertEqual(stale.read_bytes(), b"stale")

    def test_queries_the_selected_index_and_exact_version(self):
        for index, host in (("pypi", "pypi.org"), ("testpypi", "test.pypi.org")):
            with self.subTest(index=index):
                self.assertEqual(upload.published_files(index, "1.2.3"), {})
                request = self.request.call_args.args[0]
                self.assertEqual(
                    request.full_url, f"https://{host}/pypi/delta-arrow-reader/1.2.3/json",
                )
                self.assertEqual(self.request.call_args.kwargs, {"timeout": 30})

    def test_workflow_output_is_emitted_only_after_success(self):
        output = self.directory / "github-output"
        arguments = [
            str(SCRIPT), str(self.artifacts), "--expected-version", "1.2.3",
            "--index", "pypi", "--destination", str(self.destination),
        ]
        with (
            patch("sys.argv", arguments),
            patch.dict(os.environ, {"GITHUB_OUTPUT": str(output)}),
            patch("sys.stderr", new_callable=io.StringIO),
            patch("sys.stdout", new_callable=io.StringIO),
        ):
            self.release["urls"] = [self.published(self.wheels[0])]
            self.release["urls"][0]["digests"]["sha256"] = "0" * 64
            with self.assertRaises(SystemExit) as error:
                upload.main()
            self.assertEqual(error.exception.code, 1)
            self.assertFalse(output.exists())
            for pending in (True, False):
                with self.subTest(pending=pending):
                    self.release["urls"] = (
                        [] if pending else [self.published(path) for path in self.wheels]
                    )
                    upload.main()
                    self.assertEqual(output.read_text(), f"has-files={str(pending).lower()}\n")
                    output.unlink()
                    for path in self.destination.iterdir():
                        path.unlink()
                    self.destination.rmdir()


if __name__ == "__main__":
    unittest.main()

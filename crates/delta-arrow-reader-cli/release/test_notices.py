"""Checks that redistribution text survives collection and incomplete input fails."""

import copy
import hashlib
import io
import json
import subprocess
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import notices


class NoticeTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        crate = self.root / "dependency"
        crate.mkdir()
        self.package = {
            "id": "registry+https://github.com/rust-lang/crates.io-index#dependency@1.0.0",
            "name": "dependency",
            "version": "1.0.0",
            "manifest_path": str(crate / "Cargo.toml"),
            "license_file": None,
            "source": "registry+https://github.com/rust-lang/crates.io-index",
        }
        self.report = {
            "crates": [{"package": self.package}],
            "licenses": [
                {
                    "id": "MIT",
                    "text": "Full license text\n",
                    "used_by": [{"crate": self.package}],
                }
            ],
        }
        (crate / "NOTICE").write_bytes(b"Original copyright attribution\r\n")
        (crate / ".cargo_vcs_info.json").write_text(
            json.dumps(
                {
                    "git": {"sha1": "a" * 40},
                }
            )
        )
        self.rust_docs = self.root / "rust-docs"
        (self.rust_docs / "licenses").mkdir(parents=True)
        (self.rust_docs / "COPYRIGHT-library.html").write_text(
            "<pre>Rust library attribution &amp; terms</pre>\n"
        )
        for name in ("Apache-2.0", "MIT"):
            (self.rust_docs / "licenses" / f"{name}.txt").write_text(f"{name} terms\n")
        self.extra = {
            "package": "dependency",
            "version": "1.0.0",
            "url": "https://raw.githubusercontent.com/owner/project/"
            + "a" * 40
            + "/LICENSE",
            "sha256": hashlib.sha256(b"Original license\n").hexdigest(),
        }

    def test_preserves_original_text_and_nested_files(self):
        crate = Path(self.package["manifest_path"]).parent
        (crate / "vendor" / "licenses").mkdir(parents=True)
        (crate / "vendor" / "licenses" / "BSD.txt").write_text("Vendor terms\n")
        (crate / "special-terms").write_text("Explicit license_file terms\n")
        (crate / "secret-data").write_text("Do not distribute this file\n")
        self.package["license_file"] = "special-terms"
        result = notices.render_notices(self.report, self.rust_docs, {})
        for expected in (
            "Original copyright attribution\r\n",
            "Full license text\n",
            "vendor/licenses/BSD.txt\n\nVendor terms\n",
            "special-terms\n\nExplicit license_file terms\n",
            "<pre>Rust library attribution &amp; terms</pre>\n",
            "Apache-2.0 terms\n",
            "MIT terms\n",
        ):
            self.assertIn(expected, result)
        self.assertNotIn("Do not distribute", result)
        self.assertNotIn(str(self.root), result)
        self.assertEqual(
            result, notices.render_notices(self.report, self.rust_docs, {})
        )

    def test_missing_original_files_require_a_supplement(self):
        (Path(self.package["manifest_path"]).parent / "NOTICE").unlink()
        with self.assertRaisesRegex(ValueError, "no original license files"):
            notices.render_notices(self.report, self.rust_docs, {})
        extra = {self.package["id"]: [(self.extra["url"], "Original license\n")]}
        self.assertIn(
            "Original license\n",
            notices.render_notices(self.report, self.rust_docs, extra),
        )

    def test_rejects_incomplete_license_reports(self):
        reports = [
            {"crates": [], "licenses": []},
            dict(self.report, licenses=[]),
            dict(self.report, licenses=[dict(self.report["licenses"][0], text=" ")]),
            dict(self.report, licenses=[dict(self.report["licenses"][0], used_by=[])]),
        ]
        missing = copy.deepcopy(self.report)
        missing["crates"].append({"package": dict(self.package, id="uncovered")})
        reports.append(missing)
        for report in reports:
            with self.subTest(report=report), self.assertRaises(ValueError):
                notices.render_notices(report, self.rust_docs, {})

    def test_rejects_missing_toolchain_notices_and_empty_or_linked_input(self):
        for name in ("COPYRIGHT-library.html", "licenses/MIT.txt"):
            path = self.rust_docs / name
            original = path.read_bytes()
            path.unlink()
            with self.subTest(name=name), self.assertRaises(ValueError):
                notices.render_notices(self.report, self.rust_docs, {})
            path.write_bytes(original)
        path = Path(self.package["manifest_path"]).parent / "NOTICE"
        path.write_text(" ")
        with self.assertRaisesRegex(ValueError, "empty license or notice"):
            notices.render_notices(self.report, self.rust_docs, {})
        path.unlink()
        path.symlink_to(self.rust_docs / "licenses/MIT.txt")
        with self.assertRaisesRegex(ValueError, "symlink"):
            notices.render_notices(self.report, self.rust_docs, {})

    def test_download_checks_content_version_and_source_commit(self):
        with patch(
            "notices.urllib.request.urlopen",
            return_value=io.BytesIO(b"Original license\n"),
        ):
            self.assertEqual(
                notices.download_extra_notices([self.package], [self.extra]),
                {
                    self.package["id"]: [(self.extra["url"], "Original license\n")],
                },
            )
        with patch(
            "notices.urllib.request.urlopen", return_value=io.BytesIO(b"Changed text")
        ):
            with self.assertRaisesRegex(ValueError, "SHA-256 differs"):
                notices.download_extra_notices([self.package], [self.extra])
        for entry in (
            dict(self.extra, version="2.0.0"),
            dict(self.extra, url=self.extra["url"].replace("a" * 40, "b" * 40)),
            dict(self.extra, url=self.extra["url"].replace("a" * 40, "main")),
        ):
            with (
                self.subTest(entry=entry),
                patch("notices.urllib.request.urlopen") as download,
            ):
                with self.assertRaises(ValueError):
                    notices.download_extra_notices([self.package], [entry])
                download.assert_not_called()
        with patch("notices.urllib.request.urlopen") as download:
            self.assertEqual(notices.download_extra_notices([], [self.extra]), {})
            download.assert_not_called()

    def test_generation_failure_does_not_write_output(self):
        output = self.root / "notices.txt"
        args = ["notices.py", "--architecture", "amd64", "--output", str(output)]
        with (
            patch("sys.argv", args),
            patch("sys.stderr", new_callable=io.StringIO),
            patch(
                "notices.generate_notices",
                side_effect=subprocess.CalledProcessError(1, "cargo-about"),
            ),
            self.assertRaises(SystemExit) as failure,
        ):
            notices.main()
        self.assertEqual(failure.exception.code, 1)
        self.assertFalse(output.exists())
        output.write_text("Previously validated notices\n")
        with (
            patch("sys.argv", args),
            patch("sys.stderr", new_callable=io.StringIO),
            patch("notices.generate_notices", return_value="Replacement notices\n"),
            self.assertRaises(SystemExit),
        ):
            notices.main()
        self.assertEqual(output.read_text(), "Previously validated notices\n")


if __name__ == "__main__":
    unittest.main()

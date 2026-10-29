import importlib
import importlib.machinery
import importlib.metadata
import sysconfig
import unittest
from pathlib import Path

import delta_arrow_reader


class PackageTests(unittest.TestCase):
    def test_installed_package(self):
        distribution = importlib.metadata.distribution("delta-arrow-reader-py")
        self.assertEqual(distribution.metadata["Name"], "delta-arrow-reader-py")
        self.assertEqual(delta_arrow_reader.__version__, distribution.version)
        self.assertEqual(distribution.metadata["Requires-Python"], ">=3.10")
        self.assertIn("pyarrow>=18.0.0", distribution.requires)
        self.assertEqual(distribution.metadata["License-Expression"], "Apache-2.0")

        files = {str(path): path for path in distribution.files}
        for name in (
            "delta_arrow_reader/__init__.pyi",
            "delta_arrow_reader/py.typed",
        ):
            self.assertIn(name, files)
            self.assertTrue(distribution.locate_file(files[name]).is_file())
        self.assertFalse(
            any(
                path.name
                in {"dar", "dar.exe", "delta-arrow-reader", "delta-arrow-reader.exe"}
                for path in files.values()
            )
        )
        self.assertFalse(
            any(path.name == "cli-THIRD_PARTY_NOTICES.txt" for path in files.values())
        )
        for name in ("LICENSE", "NOTICE"):
            self.assertTrue(any(path.name == name for path in files.values()))

        native = importlib.import_module("delta_arrow_reader.delta_arrow_reader")
        self.assertTrue(
            any(
                native.__file__.endswith(suffix)
                for suffix in importlib.machinery.EXTENSION_SUFFIXES
            )
        )

        notices = Path(sysconfig.get_path("data")) / "share/delta-arrow-reader"
        text = (notices / "python-THIRD_PARTY_NOTICES.txt").read_text(encoding="utf-8")
        for dependency in ("pyo3", "Rust standard library", "datafusion"):
            self.assertIn(dependency, text)


if __name__ == "__main__":
    unittest.main()

import importlib
import importlib.machinery
import importlib.metadata
import unittest

import delta_arrow_reader


class PackageTests(unittest.TestCase):
    def test_installed_package(self):
        distribution = importlib.metadata.distribution("delta-arrow-reader")
        self.assertEqual(distribution.metadata["Name"], "delta-arrow-reader")
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
        for name in ("LICENSE", "NOTICE"):
            self.assertTrue(any(path.name == name for path in files.values()))

        native = importlib.import_module("delta_arrow_reader.delta_arrow_reader")
        self.assertTrue(
            any(
                native.__file__.endswith(suffix)
                for suffix in importlib.machinery.EXTENSION_SUFFIXES
            )
        )


if __name__ == "__main__":
    unittest.main()

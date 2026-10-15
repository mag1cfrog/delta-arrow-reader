import ast
import importlib.util
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch


path = Path(__file__).resolve().parents[1] / "generate_api_docs.py"
spec = importlib.util.spec_from_file_location("generate_api_docs", path)
generator = importlib.util.module_from_spec(spec)
spec.loader.exec_module(generator)


class ApiDocsTests(unittest.TestCase):
    def test_native_docs_replace_stub_docs_without_changing_types(self):
        class Table:
            """Native table documentation."""

            @property
            def version(self):
                """The loaded version."""

            def read(self, *, limit=None):
                pass

        Table.read.__doc__ = 'Read rows.\n\nExamples:\n    print("""rows""")\n    path = "a\\b"'
        stub = '''
class Table:
    """Old class documentation."""
    def __init__(self, path: str) -> None: ...
    @property
    def version(self) -> int: ...
    def read(self, *, limit: int | None = None) -> list[str]:
        """Old method documentation."""
        ...
'''
        rendered = ast.parse(generator.render(stub, SimpleNamespace(Table=Table)))
        table = rendered.body[0]
        self.assertEqual(ast.get_docstring(table), Table.__doc__)
        methods = {node.name: node for node in table.body if isinstance(node, ast.FunctionDef)}
        self.assertEqual(ast.get_docstring(methods["read"], clean=False), Table.read.__doc__)
        self.assertEqual(ast.get_docstring(methods["version"]), Table.version.__doc__)
        self.assertIsNone(ast.get_docstring(methods["__init__"]))

        original = ast.parse(stub)
        for tree in (original, rendered):
            for node in ast.walk(tree):
                if isinstance(node, (ast.ClassDef, ast.FunctionDef)):
                    generator.set_docstring(node, None)
        self.assertEqual(ast.dump(rendered), ast.dump(original))

    def test_missing_native_method_fails_instead_of_publishing_stale_docs(self):
        class Table:
            pass

        with self.assertRaises(AttributeError):
            generator.render("class Table:\n    def read(self) -> None: ...", SimpleNamespace(Table=Table))

    def test_check_detects_changed_native_docs_without_rewriting_output(self):
        class Table:
            """Original description."""

        with tempfile.TemporaryDirectory() as temporary:
            source = Path(temporary) / "source.pyi"
            output = Path(temporary) / "output.pyi"
            source.write_text("class Table:\n    version: int\n", encoding="utf-8")
            with (
                patch.object(generator, "SOURCE", source),
                patch.object(generator, "OUTPUT", output),
                patch.dict("sys.modules", {"delta_arrow_reader": SimpleNamespace(Table=Table)}),
                patch("builtins.print"),
            ):
                with patch("sys.argv", ["generate_api_docs.py", "--check"]):
                    with self.assertRaisesRegex(SystemExit, "stale"):
                        generator.main()
                self.assertFalse(output.exists())

                with patch("sys.argv", ["generate_api_docs.py"]):
                    generator.main()
                original = output.read_bytes()
                with patch("sys.argv", ["generate_api_docs.py", "--check"]):
                    generator.main()
                    Table.__doc__ = "Updated description."
                    with self.assertRaisesRegex(SystemExit, "stale"):
                        generator.main()
                self.assertEqual(output.read_bytes(), original)

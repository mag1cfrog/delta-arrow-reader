"""Check Spark workspace Rust formatting in bounded batches."""

from collections import defaultdict
import json
from pathlib import Path
import subprocess


def target_editions(workspace):
    """Collect the same target roots as cargo fmt --all."""
    editions = {}
    visited_dependencies = set()

    def visit(manifest):
        metadata = subprocess.check_output(
            ["cargo", "metadata", "--offline", "--no-deps", "--format-version", "1",
             "--manifest-path", str(manifest)],
            cwd=workspace,
        )
        packages = json.loads(metadata)["packages"]
        manifests = {Path(package["manifest_path"]).resolve() for package in packages}
        for package in packages:
            for target in package["targets"]:
                editions.setdefault(Path(target["src_path"]).resolve(), target["edition"])
            # Follow local workspaces, including those outside the starting workspace.
            for dependency in package["dependencies"]:
                dependency_path = dependency.get("path")
                if dependency_path is None or dependency["name"] in visited_dependencies:
                    continue
                path = Path(dependency_path).resolve() / "Cargo.toml"
                if path.is_file() and path not in manifests:
                    visited_dependencies.add(dependency["name"])
                    visit(path)

    visit(workspace / "Cargo.toml")
    return editions


def check_format(workspace):
    editions = target_editions(workspace)
    if not editions:
        raise SystemExit("No Rust targets found for the Spark workspace")

    paths_by_edition = defaultdict(list)
    for path, edition in sorted(editions.items()):
        paths_by_edition[edition].append(str(path))

    failed = False
    # Windows limits the command line of each child process.
    batch_size = 16
    for edition, paths in sorted(paths_by_edition.items()):
        for start in range(0, len(paths), batch_size):
            batch = paths[start : start + batch_size]
            result = subprocess.run(
                ["rustfmt", *batch, "--edition", edition, "--check"], cwd=workspace
            )
            failed |= result.returncode != 0
    return int(failed)


if __name__ == "__main__":
    raise SystemExit(check_format(Path(__file__).resolve().parents[1]))

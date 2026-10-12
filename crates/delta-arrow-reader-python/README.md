# Delta Arrow Reader for Python

Read Delta Lake tables as PyArrow record batches with the Rust Delta Arrow Reader.
The same package includes the native `delta-arrow-reader` command.

PyPI publication is planned. For now, download a validated wheel for your platform
using the [installation guide](https://mag1cfrog.github.io/delta-arrow-reader/installation/#python).
In an activated virtual environment, replace `/path/to/wheel.whl` with its full
path and original filename:

```console
python -m pip install --only-binary=:all: "/path/to/wheel.whl"
delta-arrow-reader --help
```

For a separate CLI environment, choose a persistent installation or a single run:

```console
uv tool install --no-build "/path/to/wheel.whl"
delta-arrow-reader --version
```

```console
uvx --no-build --from "/path/to/wheel.whl" delta-arrow-reader --help
```

If uv reports that its executable directory is missing from `PATH`, run
`uv tool update-shell` and restart your shell. Tool environments do not install
the Python API into your project's environment.

Wheels require CPython 3.10 or newer and target Linux x86_64 with glibc 2.28 or
newer, Windows x86_64, and macOS arm64 with macOS 12.0 or newer. See the
installation guide for tested Python versions and platform limits. Installing a
wheel needs no Rust toolchain. Every installation includes the Python extension,
native CLI, and PyArrow 18 or newer, even if you only use the command. This uses
more disk space than the standalone Linux `dar` archive.

```python
from delta_arrow_reader import DeltaTable

table = DeltaTable("/path/to/delta-table")
with table.to_reader() as reader:
    for batch in reader:
        print(batch)
```

See the [Python quickstart](https://mag1cfrog.github.io/delta-arrow-reader/python/)
for column selection, filters, and snapshot refresh. The
[contributor guide](https://github.com/mag1cfrog/delta-arrow-reader/blob/main/docs/content/contributing.md)
covers local builds and tests.

# Remove genawaiter from xot (#1558)

MacBook Pro (Mac14,6), Apple M2 Max, native arm64 macOS 26.7.3 (25G342).
Rust 1.96.0.

xot 0.31.2 requires genawaiter in its ordinary dependency list. Its ICU,
proptest and Serde features do not control that dependency. The latest
published release and the upstream manifest both still require it. The Xee
XPath packages also select xot, so changing Sail's direct xot imports would
leave genawaiter in the graph. [Upstream manifest](https://github.com/faassen/xot/blob/4a44505f23844d6101a8da7397e434562b1ac513/Cargo.toml)

The local xot patch replaces the three generator sites with standard
iterators. Level-order traversal keeps its sibling boundaries and terminal
marker. Namespace traversal keeps input order, prefix shadowing and default
namespace undeclarations. Serialization keeps start and end token order,
namespace declarations, attributes and leaf content. The Spark lock
selects one local xot 0.31.2 and has no genawaiter or genawaiter macro
packages. [Upstream and archive provenance](../../source-provenance.json)

The fork passed 483 upstream xot tests, including its serialization snapshots.
The before-and-after probe agreed on 24 XML inputs from the committed XPath
tests and additional namespace, attribute, sibling and malformed XML cases.
It compares parser failures, level-order traversal, namespace order and
serialized subtrees. Run it from the Spark workspace with
`rustup run 1.96.0 python3 ci/check-xot-iterators.py`.

The source inventory records the five modified files and the two excluded
packaging files. The other 77 files match the published archive byte for byte,
including the upstream whitespace and test snapshots.

Formatting, Clippy and the full Spark workspace tests passed on this Mac.
The existing Spark captures and expected outcomes stayed intact.

# Native SQL registry in stock and linked CLI builds (#1559)

MacBook Pro (Mac14,6), Apple M2 Max, native arm64 macOS 26.7.3 (25G342).
Rust 1.96.0. Both CLI binaries use staging `aa44a407` and its native
session constructor. The linked scratch binary retains the Spark entry
point, as verified with `nm`, and runs the probes through the native session.

## Registry difference

| Native registry | Stock names | Linked names |
| --- | ---: | ---: |
| Scalar functions | 133 | 247 |
| Higher-order functions | 0 | 7 |
| Aggregate functions | 45 | 45 |
| Window functions | 11 | 11 |
| Table functions | 2 | 2 |
| Expression planners | 3 | 7 |
| Relation planners | 0 | 0 |
| Type planners | 0 | 0 |
| File formats | 3 | 3 |
| Table provider factories | 5 | 5 |

The CSVs list each registered name and its canonical function name, including
aliases. Linking adds 121 names across 60 canonical functions. It also adds
the nested-expression, field-access, datetime and Unicode expression
planners. The aggregate and window planners follow them in the linked
registration order. The active DataFusion features are in the same files.
[Stock](native-registry-1559-stock.csv), [linked](native-registry-1559-linked.csv) and [difference](native-registry-1559-difference.csv)

## Added-function probes

Each name ran in its own process over four rows. The map column contains a
NULL map, a two-entry map with a NULL value, an empty map and a one-entry map.
112 statements returned rows, eight returned errors and one panicked.
[Statements and outcomes](native-registry-1559-probes.csv)

The seven higher-order registrations returned a missing-field error for the
lambda argument under the CLI's native SQL dialect. The `map` probe with a
literal key array and a column-derived value array returned a length error.
With both arrays derived from the input row, the same function returned four
maps with the expected `"a"` key and corresponding row ID as the value.

`map_entries` was the only panic. It occurred when the Arrow list constructor
compared its declared `key` and `value` children against the data's `keys` and
`values`. The CSV retains the panic message and location.

## Keeping the stock registry in a linked build

The shared build enables both sets of DataFusion features. To keep the
stock native registry, build the native session with explicit registrations
from the stock inventory. Supply its scalar, aggregate, window, higher-order
and table functions and their aliases, its ordered expression planners,
and its file formats and provider factories. Preserve the stock type and
relation planner configuration as well. Test the registry and replay native
queries for values, types and errors, since the enabled expression planners
affect SQL syntax in addition to named calls.

## Build evidence

The stock and linked proof branches include the same registry and query
observer. The linked branch also selects the sixteen staging dependency
patches at the root. Both builds used Cargo's unoptimized development profile
with debug information and incremental compilation disabled. These were
functional probes.
[Stock proof](https://github.com/mag1cfrog/delta-arrow-reader/tree/chore/1559-stock-registry-proof) and [linked proof](https://github.com/mag1cfrog/delta-arrow-reader/tree/chore/1559-linked-registry-proof)

| Executable | SHA-256 |
| --- | --- |
| Stock | `fcd6162617766653026fe5a635afbb2c6614694bb0ec7a32c5fc700d752f3f43` |
| Linked | `194a763f60de9dc5d00d0fcc261daf4b28cca815b5a56348507bd4f1abdd6655` |

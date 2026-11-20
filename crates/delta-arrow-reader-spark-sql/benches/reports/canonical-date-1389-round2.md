# Ten-byte DATE comparison (#1389)

The fixed-position shape check did not improve `to_date(date_s)`.
Both ANSI modes were slower in all three paired processes. Staging
keeps the existing predicate. The tested candidate is preserved at
`d8d4521178a099224292549765295f334d8ae335`.

| Path | Staging ns/input row | Candidate ns/input row | Candidate minus staging | Candidate native gap | Candidate noise floor |
| --- | ---: | ---: | ---: | ---: | ---: |
| Native CAST | 14.167 | 14.188 | 0.021 | | |
| ANSI off | 19.509 | 19.625 | 0.115 | 5.436 | 0.104 |
| ANSI on | 19.450 | 19.666 | 0.216 | 5.477 | 0.085 |

Each value is the median of three process medians of the execution cost
above projection on one million input rows. The same-session
[90 samples](../canonical_date_1389/sql-samples.jsonl.gz) also retain
full execution and projection times. Exact preflight matched both
complete Spark outputs and executed result types to the native CAST in
each build. Both native physical plans and registered function lists
matched. No shapes were excluded.

## Attribution

The [array probe](canonical-date-1389-attribution.md) assigned 1.360 ns per
row to the existing padded shape check in a controlled Spark row loop.
This measures the original cost of checking the input. The failed
candidate changes how it checks the same shape. Its paired SQL
measurements show no improvement in end-to-end execution.
The safe Arrow control in the array probe also has a different row
loop from the strict SQL native reference. It cannot assign the rest
of the strict SQL gap to individual array operations.

Staging still has 5.342 and 5.283 ns per input row above native in the
two ANSI modes. This completes the bounded ten-byte trial and leaves
#1389 open.

## Build identities

Both builds used Rust 1.96.0, locked release Cargo, opt level 3, debug
level 1 and Cargo's default 16 release codegen units. The unchanged
#1391 driver uses the committed seeded table and the original query,
strict native CAST reference and projection.

| Build | Git source | Executable SHA-256 |
| --- | --- | --- |
| Staging | `9c2e5855e9cc6d704c562936e9e517c3f4cd32cb` | `f6ab5a2acce87d3e65bf28998fd9956e83136040c8a08c6aeaabfceee172576f` |
| Candidate | `d8d4521178a099224292549765295f334d8ae335` | `e6ac4b09eca5a8e95377021bf6ba33871b2f91802d00a479e95244b3941580c0` |

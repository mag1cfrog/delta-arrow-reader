# Numeric HEX and bitmap bucket controls (#1412)

Both arms ran the same release executable. HEX uses the locked
Spark formatter, whose numeric selection is required by an existing
planner test. The native reference formats the integer with TO_HEX
and uppercases it. Bitmap buckets use identical signed CASE and
division on both paths.

## Remaining gaps

- HEX exceeds its same-arm native median by 4.368-4.471 ns per
  input row with ANSI off and 4.296-4.468 with ANSI on.
- BITMAP_BUCKET_NUMBER exceeds its same-arm native median by
  1.321-2.795 ns per input row with ANSI off and
  1.321-2.061 with ANSI on. The two Spark repeat medians
  differ by 1.452 ns with ANSI off and 0.717 ns with ANSI on, despite
  running the same executable. Report that variation with the gap.

Spark's bitmap plan adds an outer projection that assigns output
names. The profile shows the shared integer arithmetic and does not
isolate a separate cost for that projection. Both HEX arms retain
the same formatter.

Times are median [min, max] ns per input row above projection from nine
retained samples per arm. Each floor uses three native process medians
per control arm.

| Function | Path | First arm | Second arm | Native A/A floor |
| --- | --- | ---: | ---: | ---: |
| HEX | Native | 8.420 [8.250, 9.076] | 8.587 [8.427, 11.183] | 0.256 |
| HEX | ANSI off | 12.891 [12.796, 13.087] | 12.955 [12.880, 13.242] | 0.256 |
| HEX | ANSI on | 12.888 [12.769, 13.065] | 12.883 [12.714, 13.091] | 0.256 |
| BITMAP_BUCKET_NUMBER | Native | 11.693 [11.643, 12.079] | 11.715 [11.627, 12.152] | 0.017 |
| BITMAP_BUCKET_NUMBER | ANSI off | 14.488 [12.308, 14.642] | 13.036 [12.292, 14.606] | 0.017 |
| BITMAP_BUCKET_NUMBER | ANSI on | 13.754 [11.985, 14.641] | 13.037 [12.698, 14.539] | 0.017 |

Each timed statement matched all 1,000,000 result rows
and their executed Arrow types across both arms, both ANSI modes and native.
The product code has no changes in this comparison. The paired
preflights run the frozen queries on the retained staging product.
All 675 selected workspace tests, formatting and Spark-root Clippy passed.

The [protocol](../arithmetic_1412/numeric-controls-protocol.md) was
committed at `e427f92973a55220e7032dcda415544a81535950` before clocks.
The Linux release session ran 2026-11-21T12:12:07Z to 2026-11-21T12:13:05Z.
The 36 comparison processes and 12 native controls retained 144 pairs.
Local evidence is under `costs-168-2026-11-20/1412/controls`.

| Build | Source | Executable SHA256 |
| --- | --- | --- |
| staging | `30585ed36142c3a41d863e7590e0534f21eb0c54` | `7cd6e92fff997c3be923cbdbbfb579ebb2fa9c3887b51c6966da0669a123d086` |
| candidate | `30585ed36142c3a41d863e7590e0534f21eb0c54` | `7cd6e92fff997c3be923cbdbbfb579ebb2fa9c3887b51c6966da0669a123d086` |

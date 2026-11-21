# FLATTEN inner null bitmap results (#1410)

FLATTEN checks the inner validity bitmap for canonical nested
BIGINT arrays. The original CASE still selects rows for native
flattening. Other leaf types use the original membership probe.

## Remaining gaps

- FLATTEN with ANSI off remains 20.766 ns per row above native. Its floor is 0.033 ns.
- FLATTEN with ANSI on remains 27.267 ns per row above native. Its floor is 0.033 ns.

Spark still builds the selection for NULL inner lists before native
flattening. The matched native plan maps the offsets directly. The
comparison measures overall expression cost.

Times are median [min, max] ns per input row above projection from nine
retained samples per arm. Each floor uses three native process medians
per control arm.

| Function | Path | Staging | Candidate | Native A/A floor |
| --- | --- | ---: | ---: | ---: |
| FLATTEN | Native | 2.260 [2.218, 2.383] | 2.174 [2.160, 2.242] | 0.033 |
| FLATTEN | ANSI off | 52.112 [50.187, 54.164] | 22.940 [19.462, 30.301] | 0.033 |
| FLATTEN | ANSI on | 52.632 [46.942, 55.208] | 29.441 [18.416, 31.301] | 0.033 |

Each timed statement matched all one million result rows and their
executed Arrow types across both builds, both ANSI modes and native.
The differential compares all four inner and outer offset combinations,
NULLs at each nesting level, empty arrays and batches, sliced offsets,
and the case with no NULL inner lists. The Map leaf probe and test
preserve the original membership failure in both ANSI modes.
All 771 selected workspace tests, formatting and Spark-root Clippy passed.

The [protocol](../array_functions_1410/flatten-protocol.md) was
committed at `d5f9bfad8bb158cb997189b57554f57fff4162a9` before clocks.
The Linux release session ran 2026-11-21T06:15:03Z to 2026-11-21T06:15:37Z.
The 18 comparison processes and 6 native controls retained 72 pairs.
Local evidence is under `costs-168-2026-11-20/1410/flatten`.

| Build | Source | Executable SHA256 |
| --- | --- | --- |
| staging | `731ba7b8742ecd196dff6a2dbb70476a15e8afc8` | `162bbdda636a9d07fe8d5bc7a102108310f1e2d2363978a05c1b6ad9e218d9ae` |
| candidate | `8d6adc6ece986b386208ea7fe64a7b1900b50c9d` | `41c6dcc28189b4185b642a319671d7b4a91f20ea8588073261d37364e705022b` |

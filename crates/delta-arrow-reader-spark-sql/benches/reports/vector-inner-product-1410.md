# Typed FLOAT VECTOR_INNER_PRODUCT results (#1410)

VECTOR_INNER_PRODUCT reads nonnull FLOAT child buffers by offset.
It keeps the Spark accumulation order and checks parent validity
and dimensions before doing the arithmetic. A child buffer with
NULLs keeps the original row slices and validity checks.

All measured shapes and ANSI modes meet the native noise target.

Times are median [min, max] ns per input row above projection from nine
retained samples per arm. Each floor uses three native process medians
per control arm.

| Function | Path | Staging | Candidate | Native A/A floor |
| --- | --- | ---: | ---: | ---: |
| VECTOR_INNER_PRODUCT | Native | 34.949 [34.413, 36.712] | 34.783 [34.195, 35.212] | 0.177 |
| VECTOR_INNER_PRODUCT | ANSI off | 64.245 [62.078, 65.133] | 9.421 [9.311, 9.491] | 0.177 |
| VECTOR_INNER_PRODUCT | ANSI on | 64.191 [62.438, 64.687] | 9.375 [9.291, 9.440] | 0.177 |

Each timed statement matched all one million result rows and their
executed Arrow types across both builds, both ANSI modes and native.
The differential compares both offset widths, sliced list and
child buffers, empty rows and batches, NULL parents, child NULL
fallback, dimension errors and exact FLOAT bits for cancellation,
NaN, infinity and signed zero.
All 642 selected workspace tests, formatting and Spark-root Clippy passed.

The [protocol](../array_functions_1410/vector-inner-product-protocol.md) was
committed at `49dbb4002e18a02034f02f31be57664175243a9f` before clocks.
The Linux release session ran 2026-11-21T06:30:47Z to 2026-11-21T06:31:21Z.
The 18 comparison processes and 6 native controls retained 72 pairs.
Local evidence is under `costs-168-2026-11-20/1410/vector-inner-product`.

| Build | Source | Executable SHA256 |
| --- | --- | --- |
| staging | `8d6adc6ece986b386208ea7fe64a7b1900b50c9d` | `41c6dcc28189b4185b642a319671d7b4a91f20ea8588073261d37364e705022b` |
| candidate | `df7c89adf6db55dd796d064e3d528dbd97ec4f19` | `291149dcbe1516ef590787796c56af76a3e248062a5afff2b2d27443889b3670` |

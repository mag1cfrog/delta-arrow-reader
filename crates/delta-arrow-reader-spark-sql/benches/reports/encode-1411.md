# Contiguous UTF-8 ENCODE results (#1411)

ENCODE shares contiguous string buffers for scalar UTF-8 and
preserves Binary or LargeBinary widths. The guard keeps the previous
normalization of NULL slots with hidden bytes.

## Remaining work

The shared-buffer path checks the charset and NULL byte ranges before
building the Binary or LargeBinary view. The valid-fixture native cast
is the cost control. Other forms still perform the original conversion.

Times are median [min, max] ns per input row above projection from nine
retained samples per arm. Each floor uses three native process medians
per control arm.

| Function | Path | Staging | Candidate | Native A/A floor |
| --- | --- | ---: | ---: | ---: |
| ENCODE | Native | 0.076 [0.068, 0.088] | 0.073 [0.063, 0.090] | 0.007 |
| ENCODE | ANSI off | 27.826 [27.605, 28.124] | 0.543 [0.526, 0.570] | 0.007 |
| ENCODE | ANSI on | 27.829 [27.277, 28.050] | 0.544 [0.525, 0.569] | 0.007 |

Each timed statement matched all 1,000,000 result rows
and their executed Arrow types across both builds, both ANSI modes and native.
The differential compares all six charsets, case and whitespace,
unknown charsets, multibyte strings, both offset widths, sliced
and empty batches, NULLs and their stored byte ranges.
All 647 selected workspace tests, formatting and Spark-root Clippy passed.

The [protocol](../generators_text_conditionals_1411/encode-protocol.md) was
committed at `6865f874d596cb5230aba641c3b0f54cd9b8ef41` before clocks.
The Linux release session ran 2026-11-21T08:01:20Z to 2026-11-21T08:01:52Z.
The 18 comparison processes and 6 native controls retained 72 pairs.
Local evidence is under `costs-168-2026-11-20/1411/encode`.

| Build | Source | Executable SHA256 |
| --- | --- | --- |
| staging | `fa4e9d46fa0a52a0ebb4277b4f16c68d577d89dc` | `e0b885a3915c3de94cff8834d8af9172ed977c4bfa315b29c1c00ec6501456c7` |
| candidate | `098189e8053d9395dbc028019f58e1554946e35c` | `72a3391c71e5ce2958b6ba9d018e894d5673e981a7d21feaccad39804ae3d745` |

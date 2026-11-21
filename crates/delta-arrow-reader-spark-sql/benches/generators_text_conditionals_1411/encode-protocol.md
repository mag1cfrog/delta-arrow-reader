# UTF-8 ENCODE comparison protocol (#1411)

The [profile and source trace](../reports/generators-text-conditionals-1411-attribution.md)
identify repeated charset normalization and copying per row. The
candidate shares contiguous string buffers for scalar UTF-8 when NULL
slots already have empty byte ranges. Other inputs retain the original
encoder and output types.

Use [encode.json](encode.json) to pin the executables and select the
frozen one-million-row ENCODE case. Require exact keyed values and
executed Int64/Binary types across both builds, ANSI modes and native
before clocks. Check all admitted charsets, case and whitespace,
slices, empty batches, multibyte strings, NULLs and hidden NULL byte
ranges against the per-row conversion at both offset widths.

Use the [matched Linux release schedule](../window_aggregates_1408/string-aggregation-protocol.md#schedule-and-pass-rule).
Report before and after medians and ranges for both ANSI modes. Keep
the change if both decrease and correctness checks pass. The native
cast is a cost control on this fixture. Record its repeat floor and
the work that the candidate still performs.

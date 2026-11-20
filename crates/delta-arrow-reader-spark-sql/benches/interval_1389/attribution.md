# Interval conversion attribution for #1389

Use the Rust toolchain and Spark workspace lockfile at staging commit
`1ec4b096432007a0d9f0f8f9347cfce5e47cddc4`. Build on Linux with the #1385
release profile (opt level 3, debug 1, 16 codegen units, no LTO or incremental
compilation). Run on CPUs 2-5 with no other local builds or timings in progress.

The strings reproduce the non-null inputs of the three #1389 interval shapes.
Cycle through line numbers 1 to 7 for each shape. The historical inputs carry
no quotes. Add two controls for the year-month shape by wrapping each input in
one or two SQL string literals. Run 7 samples of 8,192 calls per shape after
256 warmups. Start every sample with a fresh per-batch interval parser. Run
the five shapes in rotation to distribute position effects. The analyzer
probe emits one record per sample. Use per-shape medians of the measured totals
divided by 8,192 and report the median absolute deviations. Allow a single
run with a 120-second limit and 4 GiB address space. Each sample checks that
all inputs parse successfully. This experiment has no product acceptance gate.
Name a phase as the main cost only when its median exceeds half of elapsed time.

The launcher runs the analyzer test in a disposable Git archive. Its timing
hooks separate lexer and parser construction, lexing, parsing including stream
handling, and AST-to-interval conversion. The conversion bucket subtracts any
nested construction, lexing, and parsing it triggered, so quoted layers do
not count twice. Count parser constructions too. The probe ends at
`IntervalValue`, before the Arrow batch builder and SQL engine. A before/after
acceptance run will compare uninstrumented release binaries.

Measurement note: Value includes destruction of scoped parsers for decoded
quoted layers. Other includes the top-level batch parser drop, measurement
overhead and the loop.

# Attribute DATE fallback parser reuse (#1389)

The first round compared two separate SF1 campaigns. The reverted reuse
candidate was 97 ms slower with ANSI off and 91 ms slower with it on.
This diagnostic tests the fresh and reused paths together across the exact
non-null string forms used by the SF1 fallback statement.

Link `attribute.rs` against the release analyzer, Chumsky and Arrow compiled
for the preceding interval run. The DATE parser, DATE cast source and those
dependencies match staging `124246cbc61f6b7893df713d18f397bf37e74b6c`. Compile
the harness with Rust 1.96.0, opt level 3, debuginfo 1 and 16 codegen units.
Use Linux, CPUs 2-5, 4 GiB address space and a 60-second process deadline.
Run alone in one session. Use 7 rotations of 16 batches of 8,192 rows for each
case. Run one batch per case before sampling. Use the median and median
absolute deviation of nanoseconds per row. Treat a direction as reproduced
when all seven paired differences agree and their median magnitude exceeds
both variants' median absolute deviations.

The array cases build the same DATE values from the same nullable strings with
original per-row construction and the historical lazy reuse closure. Both array
cases use the existing DATE32 conversion, including the canonical-input test,
and assert equality with precomputed Arrow results. The parser-only cases
measure the existing fresh parser, the old lazy reuse closure and an eagerly
reused parser without its initialization branch. A parser-materialization
control measures construction without parsing. A conversion-only control uses
preparsed values. These controls are compiler-sensitive and are not additive.
Report separately whether the changed array path and parser-only path reproduce
the old regression. Keep an unisolated part of the difference as unexplained.

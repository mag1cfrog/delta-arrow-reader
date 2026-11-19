# NULLIF row-selection attribution (#620)

The baseline spends most of the sparse-NULL selection work filtering input and
scattering results. A direct column or literal right operand does not make the
kernel safe: NULLIF can perform fallible comparison conversions internally.
The proposed change needs an explicit typed kernel contract as well as a safe
operand expression. It is limited to the measured Float64/Float64 domain.

This is single-partition execution over 262144 synthetic rows, with two runtime
workers. [Protocol](nullif-620-protocol.md) and [manifest](../nullif_620/manifest.json)
were committed before timing. The complete preflight checked 216 configurations,
144 native/Spark full-result comparisons and 16 operand-effect controls without
exclusions. The additional checked-arithmetic preflight checked 108 configurations
and 72 native pairs. These controls precede any shared-fork implementation.

Execution medians in ms, input batches of 8192 rows:

| Shape | Native | Spark off | Spark on | Native A/B noise |
| --- | ---: | ---: | ---: | ---: |
| Column/nonzero literal, no NULLs | 0.142745 | 0.160678 | 0.160568 | 0.001143 |
| Column/nonzero literal, sparse NULLs | 0.146362 | 0.976093 | 0.974751 | 0.000261 |
| Column/nonzero literal, half NULLs | 0.148365 | 0.608951 | 0.607187 | 0.001883 |
| Column/nonzero literal, all NULLs | 0.149708 | 0.084026 | 0.084798 | 0.003166 |
| Two columns, sparse NULLs | 0.154377 | 1.591987 | 1.589041 | 0.003507 |
| Computed left, sparse NULLs | 0.288005 | 1.815322 | 1.794022 | 0.017352 |

There are 58 positive-cost flags out of 72 cells in each ANSI mode. The strict
flag is Spark execution minus native execution greater than same-binary native
A/B noise. A small gap is not accepted merely because it is small.

For sparse two-column input, off-mode replay medians are 0.825368 ms filtering,
0.204960 ms invoking the kernel and 0.379926 ms scattering; on-mode filtering
and scattering are 0.814916/0.383032 ms. Sparse computed-left input additionally
filters the already evaluated first operand for 0.406127/0.399278 ms off/on.
These are diagnostic function-boundary samples, not an additive wall-time
breakdown. Preparing the actual planned input is outside these stage timers;
canonical execution includes that work.

The attributed product location is the partial-NULL branch of
`vendor/dependencies/datafusion-physical-expr/src/scalar_function.rs`, baseline
lines 320-348. Its NULL-first contract is in the existing `datafusion-expr` fork;
SparkNullIf owns the comparison conversions and original-left-bit preservation
in `vendor/sail/crates/sail-plan/src/function/scalar/conditional/nullif.rs`.
A typed opt-in belongs at these boundaries. Native kernels, float normalization,
comparison coercion, shared scatter and planner rules are outside this change.
The empty, scalar-NULL, all-NULL and no-NULL routes remain intact, as do every
unhandled type and computed-right expression.

[All raw samples, metadata, summaries and checksums](../nullif_620/before/assets.json)
retain the exact source and binary identities. Main timing has 2592 retained
samples across six fresh processes; stage replay has 432 retained samples.
Each canonical result is checked outside both timers. Both campaigns ran once;
no profile or repeated valid timing was used. The complete before/after decision
will follow the candidate verification; this attribution does not establish that
remaining kernel or fallback costs are acceptable.

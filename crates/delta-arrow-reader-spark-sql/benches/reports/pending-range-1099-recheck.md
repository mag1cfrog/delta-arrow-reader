# Pending RANGE candidate recheck

The candidate keeps the Spark speed-up. At 4,096 rows with one row per input
batch, Spark ANSI-off fell from 489.482 ms to 13.347 ms and ANSI-on from
489.228 ms to 13.347 ms. Eight of nine native cells exceeded the paired
regression rule. [#1099](https://github.com/mag1cfrog/delta-arrow-reader/issues/1099)
stays open and the candidate stays on its branch.

## Measurement

The [frozen protocol](https://github.com/mag1cfrog/delta-arrow-reader/blob/91e7aef2f36803454d2e2ad0b1ffe4cd10d52c35/crates/delta-arrow-reader-spark-sql/benches/reports/pending-range-1099-recheck-protocol.md) was committed
as `91e7aef2`. Staging `6a520463` and candidate `063b52e2`
ran in the same local Linux session from 2026-11-20T17:02:50Z to
2026-11-20T17:12:51Z. All 48 processes completed, yielding 9,072 samples.
Both builds matched across 63 trace contexts, including results, executed
fields, plans, delivered batches and progressive output. Both Spark ANSI
modes matched the frozen Spark 4.2.0 capture. The 18 wrapped-offset comparisons
with different native outcomes were checked outside the timed matrix.
The [compressed sample archive](pending-range-1099-recheck-samples.jsonl.gz)
contains the original timing records in process order and identifies
the two binaries, protocol revision and Spark reference.

The elapsed columns are medians of process medians. The paired shift is
the median of block differences, so it can differ from subtracting the two
elapsed columns. Repeat noise is the larger within-build replicate median
defined in the protocol. Times in both tables are milliseconds.

## Native controls

| Rows | Batch | Staging | Candidate | Paired shift | Repeat noise | Sign p | Regression |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: | :--- |
| 1,024 | 1 | 31.108587 | 31.280767 | +0.226627 | 0.098102 | 0.000244 | Yes |
| 1,024 | 32 | 1.196803 | 1.209081 | +0.011995 | 0.004734 | 0.000244 | Yes |
| 1,024 | 1,024 | 0.258580 | 0.262618 | +0.003103 | 0.002234 | 0.019287 | Yes |
| 2,048 | 1 | 119.620879 | 120.513415 | +1.106438 | 0.556129 | 0.000244 | Yes |
| 2,048 | 32 | 4.167879 | 4.206155 | +0.046138 | 0.018153 | 0.003174 | Yes |
| 2,048 | 1,024 | 0.556038 | 0.565195 | +0.009158 | 0.005846 | 0.003174 | Yes |
| 4,096 | 1 | 468.238776 | 471.824521 | +4.727227 | 3.695237 | 0.000244 | Yes |
| 4,096 | 32 | 15.470818 | 15.598819 | +0.135343 | 0.063397 | 0.000244 | Yes |
| 4,096 | 1,024 | 1.318790 | 1.339074 | +0.021838 | 0.024791 | 0.072998 | Inconclusive |

The native row loop has no pending cursor access in the release build.
Disassembly of the plain aggregate shows that its native loop is inlined
and that the new frame predicate is a call before the loop. The candidate
also extends the range-state layout to hold the cursor and changes code
generation in the shared aggregate routine. The measurements establish the
combined native regression. They leave the cost attributable to the new
dispatch, state layout and code generation unresolved. The native gate stopped
the candidate before merge and before post-merge attribution of buffer copies
and argument evaluation.

## Spark controls

| Rows | Batch | ANSI-off staging | ANSI-off candidate | ANSI-on staging | ANSI-on candidate |
| ---: | ---: | ---: | ---: | ---: | ---: |
| 1,024 | 1 | 32.817758 | 2.756222 | 32.840815 | 2.761090 |
| 1,024 | 32 | 1.169999 | 0.203503 | 1.170726 | 0.204385 |
| 1,024 | 1,024 | 0.175276 | 0.117323 | 0.176432 | 0.117439 |
| 2,048 | 1 | 125.305559 | 5.904020 | 125.325441 | 5.917777 |
| 2,048 | 32 | 4.186458 | 0.399657 | 4.184370 | 0.399682 |
| 2,048 | 1,024 | 0.393091 | 0.216392 | 0.392409 | 0.216848 |
| 4,096 | 1 | 489.481823 | 13.346661 | 489.227546 | 13.347491 |
| 4,096 | 32 | 15.828950 | 0.826832 | 15.825490 | 0.826952 |
| 4,096 | 1,024 | 0.998977 | 0.417561 | 0.999437 | 0.415733 |

## Evidence identifiers

- Staging executable SHA-256 `83e80c6d411c624b24e4a115fb86fb721c673fd75deb23de0783f8fdf3ee34d0`
- Candidate executable SHA-256 `252b6b1cc9e2f1b5eb7b05f04116c7c8937f7342b0d98e895b714eef4454ab89`
- Paired summary SHA-256 `9b1dd0d212ef0ad36738230bec173090db36a5aead2dbda74ebba5037f9b5f93`

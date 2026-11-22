# Timestamp difference correction (#1515)

Inlining the shared arithmetic restored the two-column TIMESTAMPDIFF
cost to its pre-#1414 range and kept the literal-end TIMEDIFF speedup.
The release binary retains the timestamp difference function and
has no separate shared arithmetic function.

The [frozen protocol](functions-1515-inline-protocol.md) compares
`e1580ef0` with `fcc25cbd` in one Linux release session. Both statements
passed full-input preflight in both ANSI modes with identical rows and
executed types. All 24 processes completed and recorded
[72 retained samples](functions-1515-inline-samples.jsonl.gz).

| Statement | ANSI | Before | Correction | Paired change | Floor | Result |
| --- | --- | ---: | ---: | ---: | ---: | --- |
| TIMESTAMPDIFF | off | 71.3383 | 71.1490 | -0.1193 | 0.7063 | Within floor |
| TIMESTAMPDIFF | on | 71.4925 | 71.1745 | -0.4024 | 0.0911 | Faster |
| TIMEDIFF | off | 41.6757 | 26.6719 | -15.0795 | 0.2446 | Faster |
| TIMEDIFF | on | 41.8706 | 26.6345 | -15.2360 | 0.2818 | Faster |

Times are nanoseconds per input row above projection. Each change
is the median of paired differences. All four comparisons pass
the protocol gate. The 20 repeated whole-build shifts remain
recorded in the [attribution report](functions-1515-attribution.md).

The Linux Spark SQL workspace passed 3,292 tests with 3 ignored.
Formatting and root-crate Clippy also passed.

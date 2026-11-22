# STRING to INTERVAL baseline with quoted input (#1389)

The quoted year-month cast took 58.4 to 59.0 ms for 8,184 rows.
Its unquoted Spark counterpart took 49.0 to 49.4 ms. The quoted
statement includes the work to form the quoted string.

The same Linux release executable ran in both arms of the
[protocol](interval-1389-round3-protocol.md). Values in the table
are medians of three process medians. A and B are the two arms.
The day-time reference projects the same input column. All other
references use a native interval cast on the unquoted input. Each
arm also runs two repeat series. Noise is their absolute difference
for native, or the difference of their cast-minus-projection costs
for day-time.

| Cast | ANSI | A / B query ms | A / B reference ms | A / B gap ns per row | A / B noise ns per row |
| --- | --- | ---: | ---: | ---: | ---: |
| Year-month | off | 49.403 / 49.299 | 39.309 / 39.254 | 1,233 / 1,227 | 127 / 33 |
| Year-month | on | 49.037 / 49.250 | 39.309 / 39.254 | 1,189 / 1,221 | 127 / 33 |
| Day-time | off | 56.744 / 57.086 | 39.413 / 39.264 | 2,118 / 2,178 | 42 / 52 |
| Day-time | on | 57.263 / 58.329 | 39.998 / 39.075 | 2,110 / 2,353 | 148 / 168 |
| Calendar | off | 66.997 / 66.950 | 38.399 / 38.705 | 3,494 / 3,451 | 26 / 27 |
| Calendar | on | 67.826 / 67.895 | 38.399 / 38.705 | 3,596 / 3,567 | 26 / 27 |
| Quoted year-month | off | 59.031 / 58.426 | 38.410 / 38.722 | 2,520 / 2,408 | 28 / 112 |
| Quoted year-month | on | 58.805 / 58.607 | 38.410 / 38.722 | 2,492 / 2,430 | 28 / 112 |

All 26 preflight outputs contained the declared 8,184 rows. All
29 exact comparisons passed, including the quoted Spark and native
results in both modes, and day-time retained the same source I/O
as the projection. The quoted result's executed type is
`Interval(YearMonth)`. No shape was excluded.

The [486 samples](../interval_1389_round3/baseline-samples.jsonl.gz)
cover all 162 processes and include both release receipts. The
release source was `310ffad9`. The runner at `cedfe678` records
the build identity next to the workload identity.

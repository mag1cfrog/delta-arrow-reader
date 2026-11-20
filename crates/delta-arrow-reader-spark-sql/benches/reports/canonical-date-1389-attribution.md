# Ten-byte DATE attribution (#1389)

The existing padded syntax check accounts for 1.360 ns per row in the
controlled Spark row loop. The array probe uses Arrow's safe cast API as
its native control. It measures 5.001 ns per row of Spark cost above that
control. The 3.255 ns per row left without the guard includes differences
in row handling and array assembly. The #1391 SQL CAST uses strict
options and a different Arrow loop, so these two native comparisons do
not decompose the strict SQL gap.

| Array operation | Median ns/row | MAD ns/row |
| --- | ---: | ---: |
| Safe Arrow cast | 13.251 | 0.027 |
| Full SparkDate call | 18.252 | 0.013 |
| Spark row loop, original check | 17.878 | 0.070 |
| Same loop, check removed for known valid test data | 16.518 | 0.045 |

Direct paired differences are 5.001 ns per row for Spark minus the safe Arrow cast,
1.360 for the old guard (MAD 0.051), and 3.255 for the unguarded loop
minus the safe Arrow cast (MAD 0.060). All seven differences in each comparison have
the same direction. The smaller [standalone guard control](../canonical_date_1389/guard-control.csv)
had two negative differences and is inconclusive. The [array samples](../canonical_date_1389/array-phases.csv)
come from the checked fixture in the [phase protocol](../canonical_date_1389/attribution.md).
All four array paths produced identical Date32 values and NULLs.

The candidate targets the measured `bytes.iter().enumerate()` check in
`SparkDate::string_to_date32`. A fixed slice pattern checks the two
separators and then checks the eight digits. The existing Arrow parser
still validates the calendar, and other strings use the original parser.
The [paired SQL report](canonical-date-1389-round2.md) records its result.

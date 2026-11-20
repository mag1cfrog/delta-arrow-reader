# Ten-byte DATE attribution (#1389)

The complete array experiment reproduces about 5 ns per input row of Spark
cost above the Arrow cast. The existing padded syntax check accounts for
1.360 ns per row in the controlled row loop. The 3.255 ns per row left
without the guard includes differences in row handling and Arrow array
assembly, which this experiment does not split further.

| Array operation | Median ns/row | MAD ns/row |
| --- | ---: | ---: |
| Native Arrow cast | 13.251 | 0.027 |
| Full SparkDate call | 18.252 | 0.013 |
| Spark row loop, original check | 17.878 | 0.070 |
| Same loop, check removed for known valid test data | 16.518 | 0.045 |

Direct paired differences are 5.001 ns per row for Spark minus native,
1.360 for the old guard (MAD 0.051), and 3.255 for the unguarded loop
minus native (MAD 0.060). All seven differences in each comparison have
the same direction. The smaller [standalone guard control](../canonical_date_1389/guard-control.csv)
had two negative differences and is inconclusive. The [array samples](../canonical_date_1389/array-phases.csv)
come from the checked fixture in the [phase protocol](../canonical_date_1389/attribution.md).
All four array paths produced identical Date32 values and NULLs.

The change targets the measured `bytes.iter().enumerate()` check in
`SparkDate::string_to_date32`. A fixed slice pattern checks the two
separators and then checks the eight digits. The existing Arrow parser
still validates the calendar, and other strings use the original parser.
The guarded Arrow path for unpadded dates is unchanged.

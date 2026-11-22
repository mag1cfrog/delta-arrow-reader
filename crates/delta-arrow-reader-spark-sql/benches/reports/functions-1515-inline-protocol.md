# Timestamp difference correction protocol (#1515)

The [attribution report](functions-1515-attribution.md) isolates the
literal-end TIMEDIFF edit. Its final build calls a shared arithmetic
function for each non-NULL row of the two-column TIMESTAMPDIFF query.
The correction at `fcc25cbd` inlines that function. The inputs,
arithmetic, null handling and errors use the same code.

Compare the exact pre-#1414 release at `e1580ef0` and the correction
release from `fcc25cbd` in one Linux session. The two SQL forms and
projection controls come from the
[isolation manifest](../final_round_slowdowns_1515/timediff-manifest.json).
Build the correction with `python3 benches/time_zone_range_1513/build.py
CORRECTION_CRATE OUTPUT TARGET` from the Spark SQL crate directory in
this checkout.

Use the existing [preflight and pairing rules](functions-1515-protocol.md).
Verify all rows and executed types across builds and both ANSI modes
before collecting any samples for a statement. Run three fresh pairs
per mode and three retained samples per process. Stop after one session
with a 15-minute cap including preflight.

Both TIMESTAMPDIFF modes must be within the paired floor or faster.
Both literal-end TIMEDIFF modes must be faster. These decisions use
the same sign and floor rules as isolation. An excluded statement,
missing sample, failed process or other outcome fails the gate. Commit
the compressed raw samples with the report regardless of the outcome.

```sh
python3 benches/final_round_slowdowns_1515/run.py correct BEFORE_BUILD CORRECTION_BUILD OUTPUT
```

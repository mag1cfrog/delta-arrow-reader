# Scalar approximate-percentile arguments and results (#1516)

The [attribution](../reports/classified-functions-1516-attribution.md)
found the largest measured difference in the two approximate-percentile
windows, 320-326 ns per input row. The scalar aggregate forms share
their evaluator. On each accumulator creation a physical percentile
literal is evaluated through an empty record batch, although its
value is already available. Returning one quantile also constructs
and clones a vector of scalar values.

Try one change to read a valid numeric literal directly and return
one quantile without the result vector. Keep the original extractor
for computed or invalid arguments, the existing list result path,
all casts and errors and the quantile algorithm. Leave frame replay
and the remaining accumulator state intact. Cover integer, Float64
and decimal literals, folded and invalid arguments, value conversion,
empty input and NULL. Keep existing fixtures and outcomes.

Verify full #1391 input rows and executed types against staging in
both ANSI modes and native before timing. The native unrestricted
results may differ from Spark. Each configuration must preserve its
own result across builds. Then use the restricted input and original
SQL from the [first comparison](manifest.json) for both aggregate
and both window names. All rows and executed types must match native
in both modes on this input. Exclude any failed comparison. Require
formatting, root Clippy, full workspace and independent review.

Use three paired blocks with alternating build order and six native
A/A processes per case. Use the frozen Linux release settings, the
same in-session schedule and a 3,600-second overall and 130-second
per-process time bound. Keep all 288 samples from 96 processes when
all four cases qualify. Retain the change only if every eligible
window has three faster pairs in each Spark mode and its median
paired drop exceeds the case's native floor. A missing window or
aggregate comparison holds the candidate. Also hold it if any
aggregate or native control has all three positive differences,
a median increase above its floor and all candidate process
medians above staging medians. For the unchanged native control,
apply the layout exception only with matching executed instructions
and state layouts. The floor is the largest of the absolute
native A/A median difference and both half-ranges of process medians.
Report all numbers and commit every sample even if the candidate
is held. Stop this cause after the single trial.

# NULLIF computed-right planning recheck (#620)

The six earlier ANSI-on planning flags meet the build-layout criterion.
The computed-right change leaves their planning path intact. The execution
gain from the original paired session supports applying the candidate.

## Planning path

The six statements all project a signed `Int32` addition, subtraction
or multiplication of two registered input columns. The retained
preflight plans match byte for byte across the trial's staging and
candidate binaries. They call `spark_checked_add`,
`spark_checked_subtract` or `spark_checked_multiply` on `a@0, b@1`.

The planning phase calls `plan_query` for parsing and Spark resolution,
then `SparkSqlQuery::create_physical_plan` for DataFusion analysis,
optimization and lowering through `SparkQueryPlanner` and
`DefaultPhysicalPlanner`. Checked arithmetic resolves input types and
fields through `SparkCheckedArithmetic`, then constructs the
`ScalarFunctionExpr` without evaluating the table's columns. Both
operands refer to columns, so constant folding cannot execute the
arithmetic at planning time. Spark's preparation-error check can evaluate
local `VALUES`. The controls use a registered table scan, which yields no
local batch, and the constant check skips column-dependent expressions.
The benchmark stops the planning timer before `collect`.

The only product functions changed by the candidate are the
`ScalarFunctionExpr::evaluate_legacy` execution method and its new
`is_float64_column_plus_literal` test. Neither participates in planning
the six controls. Execution sees a direct right column and an `Int32`
left input, so it never takes the addition shortcut. The UDF, expression
constructor and state fields are unchanged.

## Binary evidence

The exact original release binaries were built from staging
`bf7811b7` and candidate `50f83cbe`. Relocation-normalized
disassembly matches for 49 compared parser, resolver, analyzer,
optimizer, planner, constructor and checked arithmetic bodies.
`evaluate_legacy` is the sole difference among those 50 bodies
(8,527 to 8,554 bytes). Independent review checked another 37 bodies,
including the Spark preparation-error checks, DataFusion constant
evaluator and checked arithmetic simplifier, with no differences. The
arithmetic UDF and expression state layouts and field access offsets
match. Each is 80 bytes with 8-byte alignment in both binaries. The
product and test patches replay unchanged on current staging.

In an unclocked replay of the checked family, a breakpoint on
`evaluate_legacy` recorded 3,468 calls in each binary. Every call was
under projection execution. There were no calls from planning or any
other path. The replay includes all six flagged statements.

## Performance disposition

The [original result report](nullif-620-computed-right-results.md)
contains the six measurements (14.650 to 23.770 microseconds above
their paired baseline) and the committed compressed raw samples.
Under the current control rule, those flags do not reject the candidate.
This recheck took no new timings.

At 8,192 rows per batch, the sparse computed-right sample moved
from 1.686/1.683 to 0.420/0.415 ms, ANSI off/on, against 0.288 ms
for native. The half-NULL sample moved from 1.038/1.029 to
0.419/0.414 ms against 0.286 native. At 262,144 rows in one batch,
the sparse sample moved from 4.472/4.517 to 0.348/0.334 ms
against 0.189 native. The original comparison passed all 26
native phase controls.

The remaining difference in these cases and the previously recorded
Float64 normalization and kernel costs keep #620 open. The separate
Float64 fusion trial remains rejected.

# Wide scan rename comparison (#1390)

[Wide resolution attribution](wide-resolution-observation.md) finds
0.954 / 0.951 ms in the table scan's rename projection, ANSI off / on.
It builds the output schema using DataFusion alias field resolution
even though each field and qualifier is already bound in scan order.
The final SELECT projection builds another schema, 0.786 / 0.785 ms,
and remains outside this change.

For a wide scan with a single table qualifier, unique input names
and no functional dependencies, use the bound fields to construct
the same output schema. The aliases, metadata, column types and
nullability stay the same. DataFusion validates output names. Keep
the regular projection path for schemas outside those conditions.

Use [this source comparison](wide-resolution-comparison.json) and
the [same matched planning schedule](wide-merge-protocol.md#plan-and-timing-checks).
The retained `8bdf07b7` release executable matches the production
and driver source of staging `790da65b`, which added only benchmark
protocols and reports. Record the source diff and match the generated
driver and fixture hashes before clocks.

Require all 678 plan traces and exact Arrow schemas to match.
Validate invalid output names, constraints and real registered Delta
providers outside the timing run. The driver checks zero Delta data
reads and executes no queries. Keep the trial if the 400-column
logical phase meets the improvement gate in one ANSI mode and
no row triggers the regression gate. Report all 360 rows and any
remaining gap in both phases.

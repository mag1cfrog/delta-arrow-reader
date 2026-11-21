# Wide projection attribution (#1390)

Trace the 400-column query on the [pinned staging product](wide-inventory.json)
with the existing planning-only driver and #1379 Delta fixture.
Use four untimed processes, one for each native and ANSI configuration.
The observable resolved plan and resulting physical plan are captured
with their full schemas. The driver checks that planning reads no Delta
data.

Use the [inventory overlay](inventory.py) in a disposable source tree.
For every analyzer rule and optimizer rule application, capture how many
projection nodes and top-level expressions enter the rule. Count the
expression callbacks actually made through DataFusion's shared plan
expression and expression-child visitor APIs. Repeated callbacks count
again. Rule-specific loops outside those visitors are described by source
when needed. These counts characterize work and are not wall timings.

Compare the Spark and native plans for relabel-only layers. A candidate
must preserve the public resolved logical plan and the final physical
plan, including schemas, names and properties. Measure any candidate and
staging in turn in a separate, committed protocol over all 30 queries
and four configurations. The first-round assets remain unchanged.

From the repository root, use Python 3 to run `inventory.py build
SOURCE_DIRECTORY CARGO_TARGET BUILD_DIRECTORY` and then
`inventory.py trace BUILD_RECEIPT DELTA_ROOT NEW_TRACE_DIRECTORY`.

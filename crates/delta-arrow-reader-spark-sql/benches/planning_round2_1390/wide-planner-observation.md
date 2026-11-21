# Wide Spark query planner attribution (#1390)

Pin the [retained staging product](wide-planner-observation.json)
after the scan-rename change. In a disposable assembly, observe the
actual SparkQueryPlanner body, metadata alignment and the call to
DefaultPhysicalPlanner on the 400-column statement. The observer
uses the frozen planning driver, #1379 fixture, Linux release
build and CPU 2, with two warmups and nine retained samples.

Observe only native-configured and both ANSI modes. All three
invoke SparkQueryPlanner. Native-plain bypasses it and remains in
the four-path paired comparison after the candidate. Rotate the
three paths across five processes apiece. Report per-process
medians and within-process spans. Spans are inclusive and include
observation overhead. Planning must read zero Delta data.

Capture one Spark ANSI-off CPU stack profile on the same statement.
The profile's 240 plan repetitions populate stack samples and do
not contribute to the timing table. Use a 120-second profiler
timeout and a 128 MiB data cap. Identify the work inside the default
planner before choosing the single change.

Commit this protocol before observation. Use
[wide_planner_observer.py](wide_planner_observer.py) to patch the
disposable generated planning driver and referenced Sail sources.
Then pass its sealed build receipt, the existing Delta fixture and
a new evidence directory to the same controller on Linux.

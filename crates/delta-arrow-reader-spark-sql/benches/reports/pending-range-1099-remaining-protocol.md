# Remaining pending RANGE work protocol (#1099)

Measure row-equivalent work in the merged Spark plan. Use the frozen
[pending-frame manifest](../pending_range_1099/manifest.json) and
Spark 4.2.0 capture as the isolation report. Collect one pair of
canonical and instrumented traces on Linux. The pair has a combined
five-minute limit on CPU 2. These observations count work without
reading clocks.

The observer adds counters inside a disposable archive of the merged
source. It counts rows passed to each input and partition batch
concatenation, argument calls and rows returned by their arrays, and
comparisons of upper RANGE bounds. A copied row equivalent counts one
row in the combined input to a concatenation. It measures work at the
call site and makes no claim about memory bandwidth.

Compare the instrumented trace against the canonical release on all 63
contexts. Require complete agreement on results, Fields, error phases,
physical plans, and progressive delivery. Check both ANSI modes against
the independent Spark capture. The nine constant-key size and batch
combinations establish scaling in each of the three configurations.
Use counts before EOF, with no rows emitted yet.

For N rows in K equal batches of b rows, the accumulated prefix length
is b * K * (K + 1) / 2. Each buffer starts from the first batch and
concatenates each later prefix. Require K - 1 calls and prefix length
minus b row equivalents per buffer. One argument array spans each
prefix, so require K calls and the full prefix length in array rows.
Require native upper comparisons to span the same prefixes and Spark
upper comparisons to visit N rows. Save raw traces even when any check
fails. The entire paired observation runs once.

Run from the Spark SQL crate directory with an uninstrumented release
of the identical product tree and the existing Spark capture.

```sh
python3 benches/pending_range_1099_remaining/observe.py build SOURCE TARGET OBSERVER
python3 benches/pending_range_1099_remaining/observe.py observe CANONICAL OBSERVER SPARK_CAPTURE OUTPUT
```

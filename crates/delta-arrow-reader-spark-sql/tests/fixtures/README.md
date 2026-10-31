# Accepted-runtime replay

`accepted-runtime.jsonl.gz` preserves the frozen implementation's observations,
including its unresolved Spark differences. `import_accepted_runtime.py`
reconstructs it from the source archive named in `accepted-runtime.json`.
Importing the fixture does not run Spark or certify compatibility.

## Parallel errors

A single capture cannot specify which unordered partition finishes first.
`parallel-error-outcomes.json` lists one additional complete error outcome for
each of 20 exact observation IDs. The original captures remain unchanged.

- Eight ANSI `only_*_union_exists` / `only_*_union_count` queries race a failing
  grouped branch against a valid row under `LIMIT 1`. The frozen physical plans
  already contain parallel `UnionExec` children under a coalescer with `fetch=1`.
  Either the captured successful result or that branch's exact execution error
  can arrive first. The four expressions are integer division and casts to INT,
  DOUBLE and DECIMAL. Successful values and all recorded schema fields must match.
- Twelve ANSI `*_column_*` ROUND queries repartition size-two batches before
  rounding. Partition zero first overflows on the type's minimum value;
  partition one first overflows on its maximum. The table permits only those
  two complete errors, retaining the exact operand, scale, stage and output name.
  It does not permit success or an arbitrary overflow message.

These alternatives were checked against the frozen plans and by executing each
ROUND partition and each failing UNION branch separately. Some repeated complete
queries also reproduced both outcomes. The matcher tests reject wrong values,
types, nullability, names, errors, stages and alternatives applied to another ID.
All other replay observations retain their original comparisons, including the
nearby forced-evaluation, ORDER BY and OFFSET error cases.

Independent Spark 4.2.0 and the frozen Spark reference both fail the eight UNION
queries. The retained runtime's possible success is an unresolved compatibility
gap tracked in the [restart plan](../../../../plans/spark-sql-restart.md) under
S07, not a new supported Spark behavior. Fixing that gap must replace these
alternatives with strict Spark expectations. Neither scheduler retries nor
disabling parallelism is a fix.

# Seeded RAND lifecycle reference

`spark-reference.jsonl` is the complete, unchanged capture from
`901-spark-probes/spark-reference-r1/observations.jsonl` in the external
`c06-2026-11-09-standing-order` evidence directory. SHA256:
`6b2e52166b8c114eed7e262ff86807f7ea65465b86ea867195cbfcd4955896e6`.

`inputs.json` preserves the input file recorded by the capture's `run_start`.
Its contents also equal that record's `inputs` value. SHA256:
`39bf7e693e490b63723454a096ff0ab7624bdd7adfb575e39f2e58253d7ac1e5`.

Spark 4.2.0 completed all 18 collects for eight SQL definitions in both ANSI
modes. The capture retains runtime identity, source partition readbacks, complete
rows, schemas, plans, settings and its successful closure record. The input
tables have one and two explicit partitions; native tests preserve those rows
and their partition order while changing batch boundaries.

The native test compares complete row multisets and executed SQL type vectors.
Repeated and concurrent executions use one physical plan; pair and interleaved
empty-batch layouts use the same captured direct-query values. Field names,
nullability and metadata remain in receipts. No expected values come from the
native random-number implementation. The baseline collector remains available
at commit `92ed59af5cb124f4c14cc03060cc53f7468370fa`.

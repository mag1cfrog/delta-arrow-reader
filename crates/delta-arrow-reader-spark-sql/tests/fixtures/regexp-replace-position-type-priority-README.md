# REGEXP_REPLACE position type admission

`regexp-replace-position-type-priority-spark.jsonl` preserves 32 independent
Spark 4.2.0 references byte-for-byte, with SHA-256
`29255c32f2495890ae0df80b031ff11832de4cee378ed3db737d2bcd8e95b673`.
Its header retains the input schemas, rows, settings, runtime revision and loaded
artifact hashes. The references cover both ANSI modes, BOOLEAN/BINARY literals
and typed NULLs, provider expressions, empty input, LIMIT 0, dead CASE branches,
NULL sources, a nonfoldable INT control and a valid STRING control.

`regexp_replace_position_type_priority.rs` requires all 192 comparisons across
Utf8/LargeUtf8/Utf8View, matching binary representations and batch sizes 1/32.
Unsupported position types must report `DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE`
before foldability is checked. The test ties the cause to Spark's recorded
condition, fourth argument, required INT type and typed input. SQL-analysis
errors must fail at native resolution with all four result-field captures
explicitly unavailable. The INT control retains `NON_FOLDABLE_INPUT`.

Successful STRING controls compare complete normalized analyzed and physical
fields, including names, types, nullability and metadata, plus ordered rows.
The shared observer retains actual raw Arrow fields. No expected field overlay
or accepted negative profile is used. The matcher also rejects changed native
stages, source types, target types, unrelated causes and missing field markers.
Native structured error parameters are unavailable; complete raw diagnostic
parity with Spark remains unverified.

The type-priority repair is tracked in
[#925](https://github.com/mag1cfrog/delta-arrow-reader/issues/925). Existing
position coercion and callback fixtures remain separate and unchanged, including
the folding-disabled INT callback boundary under #893.

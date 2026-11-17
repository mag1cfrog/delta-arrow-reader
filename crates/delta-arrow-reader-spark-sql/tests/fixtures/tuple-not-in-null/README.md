# Tuple NOT IN references

The source-only requests cover tuple membership with nullable, duplicate and
empty inputs, partial NULL matches, computed operands and direct Boolean
consumers in both ANSI modes. `capture_spark.py` uses the shared capture protocol
to record Spark 4.2.0 results and executed schemas.

The Rust test compares tuple results with these raw references. Its two scalar
projection controls retain the approved SQL three-valued policy from #1305,
with explicit expected rows kept separately from Spark's existence results.

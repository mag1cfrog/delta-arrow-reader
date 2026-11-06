# Floating integer CAST Fields

`spark.jsonl`, `manifest.json` and `capture_numeric_cast_nullability.py` preserve
the independent 264-record #796 capture. The 528 batch comparisons use
`floating_integer_cast_fields_match_recorded_stages` in the existing floating
test suite. All 18 original consumer records remain unchanged.

`retained-type-function-names.json` preserves four complete #1019 observations.
The test changes only the expected public nullable leaves to the independent
Spark declaration. Other Fields, names, values and stages stay exact.

`null-inspection-spark.jsonl` preserves the existing 12-record regression capture,
replayed as 24 batch contexts. All Legacy and successful ANSI results require
strict stage and row equality. `null-inspection-retained-facets.json` binds only
eight ANSI error causes and phases from the prior root proof; its Fields and
stage availability already match Spark. Missing #922 condition identifiers
receive no Spark compatibility credit.

The exact original NULL-inspection capture, manifest and observation-only Rust
probe are retained in `sources/` as provenance. Their historical machine paths
are unchanged. The probe is not installed as a product assertion; the existing
shared observers perform the permanent regression checks.

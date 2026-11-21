# CONCAT view length capacity check (#1388)

The [SF1 attribution](../reports/strings-1388-round2-attribution.md)
profiles the per-view checked subtraction that protects Utf8
capacity. The candidate sums encoded u32 lengths with a u32
view-count bound, then checks capacity once. The sum includes
encoded bytes at NULL slots. Larger arrays keep the existing fold.

Measure the [pinned comparison](concat-comparison.json) for isolated
CONCAT and the combined string query. Follow the
[paired schedule and retention rule](substring-protocol.md), using
isolated CONCAT as the target. The baseline already includes the
merged SUBSTRING improvement. Exact rows, SQL and Arrow types and
source I/O must agree in both builds, both ANSI modes and native.

Run `python3 compare.py concat-comparison.json STAGING_BINARY CANDIDATE_BINARY SNAPSHOT NEW_EVIDENCE_DIR`
from this directory after committing the protocol.

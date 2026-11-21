# Short ASCII SUBSTRING prefixes (#1388)

The [SF1 attribution](../reports/strings-1388-round2-attribution.md)
profiles character boundary detection in the shared native prefix
kernel. The candidate builds short inline prefixes directly when
the prefix bytes are all ASCII. The same NULL, position, length,
type and evaluation contracts apply. Other prefixes use the current
kernel.

Compare the [pinned staging and candidate builds](substring-comparison.json)
on the frozen isolated SUBSTRING statement and the combined
SUBSTRING, CONCAT and UPPER statement. Use the same Linux release
and source I/O settings as the [first session](protocol.md).
Verify exact decoded rows, SQL and Arrow types and source I/O
against native and across both builds before clocks.

Run six process pairs per native or ANSI path and six native
A/B pairs per shape. Alternate the build order and rotate the
mode and shape order. Each process has one warmup and three
retained samples. The schedule bounds are 96 processes, 288
retained samples and 30 minutes. Preflight has the original
per-export and per-comparison timeouts.

Report median completion time in ms and ns per input row.
The candidate meets the native target when each changed shape
and mode is at or below same-session native plus its A/B floor.
Keep the candidate only if at least one isolated ANSI mode
improves by more than the floor in median paired time, with
at least four of six process pairs improving. Apply the same
rule in the opposite direction to detect a regression on any
Spark mode of either shape.

Run `python3 compare.py substring-comparison.json STAGING_BINARY CANDIDATE_BINARY SNAPSHOT NEW_EVIDENCE_DIR`
from this directory after committing the protocol. Each executable
has the retained build source, hash, toolchain and Cargo receipts
beside it.

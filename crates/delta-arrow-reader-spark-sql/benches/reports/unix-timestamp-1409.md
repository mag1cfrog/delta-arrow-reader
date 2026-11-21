# Default UTC unix timestamp results (#1409)

Both aliases fell from 156-158 to about 16.5 ns per row above projection.
Both ANSI modes meet the native noise target. The checked whole-second
parser and UTC conversion replace the row format interpreter and
repeated session zone lookup for this input shape.

Times are median [min, max] ns per input row above projection from nine
retained samples per arm. Each floor uses three process medians per
native control arm.

| Function | Path | Staging | Candidate | Native A/A floor |
| --- | --- | ---: | ---: | ---: |
| TO_UNIX_TIMESTAMP | Native | 50.957 [46.538, 52.324] | 50.636 [46.635, 50.940] | 0.172 |
| TO_UNIX_TIMESTAMP | ANSI off | 156.083 [155.392, 160.634] | 16.459 [16.330, 16.762] | 0.172 |
| TO_UNIX_TIMESTAMP | ANSI on | 157.567 [154.611, 167.673] | 16.486 [16.293, 16.542] | 0.172 |
| UNIX_TIMESTAMP | Native | 50.647 [50.093, 51.008] | 50.805 [50.281, 51.288] | 0.397 |
| UNIX_TIMESTAMP | ANSI off | 156.436 [154.319, 158.406] | 16.427 [16.290, 16.633] | 0.397 |
| UNIX_TIMESTAMP | ANSI on | 157.954 [154.075, 163.677] | 16.422 [16.309, 16.597] | 0.397 |

The new route applies to validated ASCII whole seconds with year 1
through 9999, the omitted pattern and the UTC session. The original
format parser handles other strings, explicit patterns and other
zones. DATE and TIMESTAMP arguments keep their existing dispatch.

Both preflights matched exact values and executed BIGINT types in all
million rows across builds, ANSI modes and native. The committed
differential checks compare the omitted and explicit default patterns
for both aliases and three string representations in UTC and Los
Angeles. They keep NULLs, empty inputs, date and DST boundaries, malformed
values and ANSI failures. All 645 library and integration tests passed,
including the existing formatted timestamp suite, along with formatting
and root Clippy.

The [protocol](../time_conversions_1409/unix-timestamp-protocol.md) was
committed at `8762a2e432ceae5e25f7ba5935dfd6cd0417231d` before clocks.
The local Linux session ran 2026-11-21 04:05:53 to 04:07:04 UTC.
All 36 comparison processes and 12 native controls completed,
retaining 144 query/projection pairs. Local evidence is under
`costs-168-2026-11-20/1409/unix-timestamp`.

| Build | Source | Executable SHA256 |
| --- | --- | --- |
| staging | `aaf1376fdc8bb47c24cf0c6b77738eb9463d88b3` | `ea9a2f64bb7b59082aac4b0eb97b0f6e1edc86d09c7c24404adf06986b83b408` |
| candidate | `0597a9fa61a5cf1b15224d11c67fe604bb1ea296` | `58f27cb22bc066b6be279cfa61dde751bf7072db1738822f8e13c43c9353a040` |

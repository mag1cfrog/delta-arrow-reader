# Interval token input comparison (#1389)

The [profile](interval-1389-residual-profile.md) attributes 143 and
198 ns per call to owned token streaming in the day-time and calendar
parsers. Test one change that feeds the numeric grammar from the
already lexed slice. Keep the existing owned stream and full grammar
for fallback.

Use the release executable built from
`bfde6ba025947172acae0c54f8ba1960d0f2fe12` as the staging arm. Its
release source matches staging `97a5fdc3`. The intervening changes are
tests, reports, measurements and their merge. Build the correction
from the protocol commit using the same harness and release settings.

Run the [third-round protocol](interval-1389-round3-protocol.md) once
after the full workspace suite, formatting, root Clippy and independent
review pass. Require all eight Spark cast cells to improve beyond
their staging spread. Check controls and any flags as prescribed.
Keep the same-session timings and compressed samples with the result
report even if the correction does not clear the gate. This is the
last product trial of this interval slice.

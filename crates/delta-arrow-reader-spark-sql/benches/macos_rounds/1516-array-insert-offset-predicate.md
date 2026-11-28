# Offset predicate for literal second-position ARRAY_INSERT (#1516)

MacBook Pro Mac14,6, Apple M2 Max, 32 GiB, macOS 26.7.3 (25G342).

The [profile](../reports/array-insert-1516-macos-profile.md) measured
about 43.9 ns per input row in ARRAY_LENGTH inside the Spark interior
predicate. The existing literal position 2 path on BIGINT List and
BIGINT columns now reads the offsets to test whether length exceeds
one. The CASE continues to select NULL, empty, interior and append
branches and preserves the evaluation of the value.

The [manifest](1516-array-insert-offset-predicate.json) pins both
native Rust 1.96.0 releases and the original and restricted case sets.
Staging uses the sealed #1599 candidate. Its product inputs match the
current staging head. Both binaries use the same measurement overlay.

Before timing, compare complete rows, executed types and source guards
across the two releases on both committed inputs in native and both
ANSI modes. Require Spark and native agreement on the restricted input.
Check that only the Spark target plans use the new predicate. Also run
the existing literal-versus-dynamic position and derived-value failure
tests unchanged in both ANSI modes. A failed preflight excludes the case.

Follow the [paired protocol](1516-exact-percentile-frame.md) for power
and thread controls, nine reversed blocks, nine retained samples per
process and the two staging A/A arms. Use the same metric, paired
median and floor. This session has 108 processes, each bounded to
130 seconds, with a one-hour total bound. Commit all emitted raw
samples. A change within its floor is not resolvable on Apple Silicon.

Merge if the Spark target is faster beyond its floor in at least one
ANSI mode and slower in neither. If the untouched native control is
slower beyond the floor, compare its normalized instructions and
state layouts in the two releases. Matching code and state classify
it as a build-layout shift. Hold if a slowdown remains, the run is
incomplete, any result differs or any workspace gate fails.

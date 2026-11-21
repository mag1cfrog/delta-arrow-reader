# Spark time conversion attribution (#1409)

The UTC-to-UTC cases cost 102-165 ns per row above projection. The real
Los Angeles conversions cost 111-179 ns per row and the +05:45 conversions
cost 85-158 ns. Literal zone lookup explains part of the CONVERT_TIMEZONE
cost. The two UTC timestamp functions already resolve their literal
zones once per batch.

Times are median ns per input row above projection from nine retained
samples per path. The native A/A floor uses the three process medians
in each control arm. Native UTC values are near zero because the
reference selects the existing values.

| Function | Zone or shape | Spark ANSI off | Spark ANSI on | Native | Native A/A floor |
| --- | --- | ---: | ---: | ---: | ---: |
| TIME | Frozen #1391 | 90.005 | 89.986 | 10.249 | 0.043 |
| CONVERT_TIMEZONE | UTC to UTC | 164.740 | 164.665 | 0.084 | 0.002 |
| CONVERT_TIMEZONE | America/Los_Angeles | 178.920 | 175.830 | n/a | n/a |
| CONVERT_TIMEZONE | +05:45 | 156.375 | 157.771 | n/a | n/a |
| FROM_UNIXTIME | Frozen #1391 | 310.379 | 316.296 | 210.570 | 2.730 |
| FROM_UTC_TIMESTAMP | UTC to UTC | 102.268 | 102.451 | 0.051 | 0.008 |
| FROM_UTC_TIMESTAMP | America/Los_Angeles | 110.841 | 111.216 | n/a | n/a |
| FROM_UTC_TIMESTAMP | +05:45 | 87.464 | 87.850 | n/a | n/a |
| MAKE_TIME | Frozen #1391 | 5.878 | 5.875 | 5.115 | 0.202 |
| TO_TIME | Frozen #1391 | 90.638 | 89.931 | 87.840 | 0.825 |
| TO_UNIX_TIMESTAMP | Frozen #1391 | 147.771 | 147.430 | 50.534 | 0.306 |
| TO_UTC_TIMESTAMP | UTC to UTC | 102.375 | 102.533 | 0.055 | 0.010 |
| TO_UTC_TIMESTAMP | America/Los_Angeles | 113.519 | 113.660 | n/a | n/a |
| TO_UTC_TIMESTAMP | +05:45 | 85.162 | 84.984 | n/a | n/a |
| TRY_TO_TIME | Frozen #1391 | 90.588 | 89.907 | 87.577 | 1.229 |
| UNIX_TIMESTAMP | Frozen #1391 | 147.600 | 147.580 | 50.675 | 0.280 |

## Where the per-row work comes from

### Time zone conversions

The UTC references are projections, so the difference is an absolute
cost per row. A real conversion has different work.

- CONVERT_TIMEZONE lowers through `convert_timezone` and `convert_tz` in
  sail-plan's datetime registry. The measured plan uses `ConvertTzLazy`
  to evaluate its arguments left to right. Its active rows carry expanded
  zone arrays into `convert_tz_inner`. The NULL-short-circuit branch parses
  both zone strings for each row whose arguments are present, including
  when both strings are literals. It then calls `convert_tz_classic`.
  A constant nonnull zone can be resolved once without changing the order
  or skipping rules of other arguments. The dynamic and NULL paths need
  their existing checks.
- FROM_UTC_TIMESTAMP and TO_UTC_TIMESTAMP use `ConvertTz` directly and
  call `convert_tz_non_classic`. Its eager singleton branch already
  resolves literal zones once per batch. Every row still passes through
  a UTC-to-local and local-to-UTC conversion. The input coercions and
  output time-zone labels are part of the executed type.
- UTC to UTC still goes through calendar conversion. An exact identity
  route for the equivalent microsecond case can avoid it. Fixed offsets
  still construct time-zone-aware date-times per row. Named regions
  require time-dependent offset and DST resolution. Both existing
  routines have range-dependent behavior, including their equal-zone
  fallback outside Chrono's range. SQL zone names and offset aliases
  cannot be treated as interchangeable without checking those boundaries.

### Unix time

- UNIX_TIMESTAMP and TO_UNIX_TIMESTAMP share `SparkUnixTimestamp`.
  Its invocation builds the default format once per batch.
  `formatted_string_to_seconds` invokes the general format parser
  and resolves the zone on every successfully parsed row. The frozen
  strings have the same whole-second layout and use the same session
  zone. The native cast uses Arrow's timestamp parser before
  `date_part` reads epoch seconds. The equivalent ordinary shape can
  reuse the checked whole-second parser and avoid repeated session
  zone parsing. Custom formats and embedded offsets or regions need
  the existing semantics.
- FROM_UNIXTIME casts numeric epoch seconds to a timestamp in the
  session zone and uses `SparkDateFormat`. The `TimestampSecond` scalar
  format branch builds the format once per batch. Each row goes through
  `format_timestamp_value`, which parses the same Arrow and Chrono
  zone labels, localizes the date-time, prepares an abbreviation and
  formats the value. The default pattern does not print a zone. The
  native reference uses `from_unixtime` and `to_char` with the equivalent
  default format. Reusing the zone preparation in the seconds path is
  independent of parsing a string input.

### TIME values

- TIME, TO_TIME and TRY_TO_TIME share `SparkTime::string_to_time_us_default`
  on the frozen strings. Each non-NULL value is a canonical `HH:mm:ss`
  string. The Spark path invokes Chrono's format parser for every value,
  including loading the default pattern. TIME's native reference uses
  Arrow's direct time parser. The native TO_TIME reference also uses
  generic format parsing, then casts its nanosecond result to
  microseconds. A checked default parser for the common layout can
  reuse Arrow's time conversion while other layouts and error behavior
  stay with the existing parser.
- MAKE_TIME uses `SparkMakeTime`. It expands the constant minute and
  Decimal(16,6) seconds into arrays and checks their validity for every
  hour. Their minute/second contribution and range checks depend only
  on the constant arguments. The native reference uses integer
  components and a final microsecond cast. A constant tail can be
  prepared once, subject to preserving NULL propagation and error
  precedence for invalid argument combinations.

## Validation and reproduction

All 16 cases passed full million-row preflight, with exact values and
executed types in both ANSI modes and against the ten supplied native
statements. The session completed 126 ordinary processes and 60 native
A/A controls. None of the cases was excluded. It retained 558
query/projection pairs.

The [protocol](../time_conversions_1409/protocol.md) and
[manifest](../time_conversions_1409/manifest.json) were committed at
`4f77bd971460d6efa290924a16f9326f7fc88907` before clocks. The Linux release
session ran 2026-11-21 02:45:59 to 02:50:38 UTC. The staging tip was
`ff4ed624c8a55730ff7578a76abf5b0d2ec0c101`. The executable source was
`ab35133d30ef6cd3170a724335770d75fd52cc74`, with only benchmark and report
changes up to the staging tip. Its SHA256 is
`72e9f124c417e048ae0762bc72d794818285d1cd8b60312a95991bc9c1c2aca0`.
Local evidence is under `costs-168-2026-11-20/1409/attribution`.

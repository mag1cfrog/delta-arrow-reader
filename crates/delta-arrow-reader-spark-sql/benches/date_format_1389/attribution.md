# Split STRING date_format into parsing and formatting (#1389)

Use staging `2135e3044fdc5825246be043427e0b7f77811c36`. In a detached
scratch checkout, append `attribute.rs` to
`sail-function/src/scalar/datetime/spark_date_format.rs` as a test module.
Build the locked Spark workspace library tests in the #1385 Linux release
profile, opt level 3, debug level 1 and 16 codegen units. Run just the
attribution test on CPUs 2-5 with a 60-second limit and 4 GiB address space.
Keep the production branch free of hooks.

The fixture is a column of 8,192 four-digit dates with times 12:34:56.
Its years span the SF1 workload's 1992-1998 range and every 97th input is
NULL. It uses the original SQL test's UTC and pattern `yyyy/MM/dd HH:mm`
(`%Y/%m/%d %H:%M` in native). The four paths are the original Spark array
parser, Arrow cast to UTC TIMESTAMP, the existing Spark formatter on the
preparsed array, and DataFusion TO_CHAR on the same array. Also measure
the existing Spark Chumsky grammar plus conversion to a naive datetime
without building output or applying timezone, to locate parsing work.
Before timing, require identical parsed values upon adding the UTC type
annotation to the Spark array, and exact formatted values, NULLs and UTF8
type. Warm each case with one batch. Run seven rotations of 32 batches
per case and summarize medians and median absolute deviations per input row.

Compare the Spark/native deltas for parsing and formatting inside this
single run. Identify the slower path with the larger excess as the first
optimization target if its magnitude exceeds both phase MADs. The grammar
control identifies input parsing cost but does not enter the deltas. This
probe omits file reads and query planning. Its phase times do not combine
with the prior SF1 medians. The separate candidate comparison uses full SQL.

# Regression accumulator comparison (#1413)

Use the four frozen regression statements and their native controls on
one million rows. Both Spark builds must use Sail's regression reducer
and the same physical plan. Compare all executed rows and types before
clocks. Local differential tests compare bit patterns for direct,
merged and retracted results against the full accumulation path.

Run the [matched schedule](protocol.md) with the
[pinned comparison](regr-moments-comparison.json). Retain the shared
update only if all four functions improve in at least two of three
paired processes in each ANSI mode, and each paired median reduction
exceeds the corresponding candidate native A/A floor. The target for
each function and mode is its candidate native median plus its floor.
No retries or replacement samples.

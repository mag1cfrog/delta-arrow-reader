# STRING DATE_FORMAT phase attribution (#1389)

The difference is mainly in parsing the timestamp text. In this isolated
release probe, Spark parsing took about 190 ns per row more than native.
Spark formatting added about 32 ns per row over native formatting.

| Phase | Spark ns/row | Native ns/row | Difference ns/row | Spark / native MAD ns/row |
| --- | ---: | ---: | ---: | ---: |
| Parse strings to microseconds | 267.605 | 77.767 | 189.838 | 0.918 / 0.331 |
| Format the same parsed array | 262.981 | 231.010 | 31.971 | 0.299 / 0.428 |

The Spark grammar and naive datetime conversion alone cost 215.113 ns per
row (MAD 0.248). The existing `parse_string_array_to_timestamp` calls
`timestamp_parser` and validates a Chumsky result for every non-null
string. That is the first target. The candidate reuses the already tested
whole-second ASCII timestamp parser for ordinary inputs and sends the
datetime through the same timezone resolution and formatting.

The [protocol](../date_format_1389/attribution.md) and
[35 samples](../date_format_1389/phases.csv) pin the release and fixed
STRING input. Parsed UTC values and final formatted results, NULLs and
type matched before timing. Both phases include their own array output,
except the grammar-only control. These are in-memory phase measurements
on the same fixture. The full paired SF1 run measures query completion.

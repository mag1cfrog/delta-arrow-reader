"""Compare numeric results independently, preserving floating-point bit patterns."""

import pyarrow as pa


def check(actual, expected):
    actual = actual.sort_by("row_id").combine_chunks()
    expected = expected.sort_by("row_id").combine_chunks()
    assert actual.column_names == expected.column_names and actual.num_rows == expected.num_rows
    for left, right in zip(actual.columns, expected.columns, strict=True):
        left, right = left.chunk(0), right.chunk(0)
        assert left.type == right.type and left.is_null().equals(right.is_null())
        left, right = left.filter(left.is_valid()), right.filter(right.is_valid())
        if pa.types.is_floating(left.type):
            bits = pa.int32() if left.type == pa.float32() else pa.int64()
            left, right = left.view(bits), right.view(bits)
        assert left.equals(right), "values differ (floats compared by bits)"

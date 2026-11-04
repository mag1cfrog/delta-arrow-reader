    let scenarios = [
        (
            "literal_empty_lambda",
            "SELECT TRANSFORM(CAST(ARRAY() AS ARRAY<INT>), x -> SLICE(ARRAY(1, 2), 0, 1)) AS r",
        ),
        (
            "provider_empty_lambda",
            "SELECT TRANSFORM(a_nn, x -> SLICE(ARRAY(1, 2), 0, 1)) AS r FROM slice_fields WHERE k = 1",
        ),
        (
            "provider_null_lambda",
            "SELECT TRANSFORM(a_pn, x -> SLICE(ARRAY(1, 2), 0, 1)) AS r FROM slice_fields WHERE k = 1",
        ),
        (
            "provider_valid_lambda",
            "SELECT TRANSFORM(a_nn, x -> SLICE(ARRAY(1, 2), 1, 1)) AS r FROM slice_fields WHERE k = 0",
        ),
        (
            "required_count_start",
            "SELECT COUNT(SLICE(ARRAY(1, 2), 0, 1)) AS r FROM slice_fields",
        ),
        (
            "required_count_length",
            "SELECT COUNT(SLICE(ARRAY(1, 2), 1, -1)) AS r FROM slice_fields",
        ),
        (
            "unused_projection",
            "SELECT 1 AS r FROM (SELECT SLICE(ARRAY(1, 2), 0, 1) AS unused FROM slice_fields) q",
        ),
        (
            "limit_zero",
            "SELECT SLICE(ARRAY(1, 2), 0, 1) AS r FROM slice_fields LIMIT 0",
        ),
        (
            "discarded_scalar_one_row",
            "SELECT CASE WHEN false THEN (SELECT SLICE(ARRAY(1, 2), 0, 1)) ELSE ARRAY(9) END AS r",
        ),
        (
            "discarded_scalar_provider",
            "SELECT CASE WHEN false THEN (SELECT SLICE(ARRAY(1, 2), 0, 1) FROM slice_fields WHERE k = 0) ELSE ARRAY(9) END AS r",
        ),
        (
            "valid_scalar_provider",
            "SELECT (SELECT SLICE(ARRAY(1, 2), 1, 1) FROM slice_fields WHERE k = 0) AS r",
        ),
        (
            "valid_scalar_empty_provider",
            "SELECT (SELECT SLICE(ARRAY(1, 2), 1, 1) FROM slice_empty) AS r",
        ),
    ];

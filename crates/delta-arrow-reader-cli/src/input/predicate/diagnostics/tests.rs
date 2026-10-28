use super::*;

#[test]
fn type_corrections_identify_parameters_without_disclosing_schema_text() {
    for (expected, supplied, field, expected_text, supplied_text) in [
        (
            DataType::Decimal128(10, 2),
            DataType::Decimal128(11, 2),
            "precision",
            "decimal128 (precision 10, scale 2)",
            "decimal128 (precision 11, scale 2)",
        ),
        (
            DataType::Decimal128(10, 2),
            DataType::Decimal128(10, 3),
            "scale",
            "decimal128 (precision 10, scale 2)",
            "decimal128 (precision 10, scale 3)",
        ),
        (
            DataType::FixedSizeBinary(3),
            DataType::FixedSizeBinary(2),
            "size",
            "fixed_size_binary (size 3)",
            "fixed_size_binary (size 2)",
        ),
        (
            DataType::Timestamp(TimeUnit::Microsecond, Some("secret-column-timezone".into())),
            DataType::Timestamp(TimeUnit::Microsecond, Some("secret-scalar-timezone".into())),
            "timezone",
            "timestamp_us (timezone set)",
            "timestamp_us (timezone set)",
        ),
        (
            DataType::Timestamp(TimeUnit::Microsecond, None),
            DataType::Timestamp(TimeUnit::Microsecond, Some("secret-scalar-timezone".into())),
            "timezone",
            "timestamp_us (timezone null)",
            "timestamp_us (timezone set)",
        ),
        (
            DataType::Struct(
                vec![arrow_schema::Field::new(
                    "secret-name",
                    DataType::Int32,
                    false,
                )]
                .into(),
            ),
            DataType::Utf8,
            "type",
            "a column type supported by predicate comparisons",
            "utf8",
        ),
    ] {
        let correction = predicate_type_mismatch_correction(&expected, &supplied, &[]);
        assert_eq!(
            correction.field_path.as_deref(),
            Some(format!("/value/{field}").as_str())
        );
        assert_eq!(correction.expected, expected_text);
        assert_eq!(correction.supplied.as_deref(), Some(supplied_text));
        let record = Error::InputValidation {
            reason: "test_type_mismatch",
            correction,
        }
        .to_json_diagnostic();
        assert!(!record.to_string().contains("secret"));
        if field == "timezone" {
            assert!(
                record["hint"]
                    .as_str()
                    .unwrap()
                    .contains("timezone text is omitted")
            );
        }
    }
}

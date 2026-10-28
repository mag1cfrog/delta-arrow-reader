use std::{collections::HashMap, sync::Arc};

use arrow_schema::{DataType, Field, TimeUnit};

use super::*;
use crate::capabilities;

#[test]
fn inspection_format_preserves_nested_schema_and_all_metadata() {
    let schema = Schema::new_with_metadata(
        vec![
            Field::new(
                "profile",
                DataType::Struct(
                    vec![
                        Field::new("amount", DataType::Decimal128(20, 4), false).with_metadata(
                            HashMap::from([("comment".into(), "secret user metadata".into())]),
                        ),
                    ]
                    .into(),
                ),
                true,
            ),
            Field::new(
                "times",
                DataType::List(Arc::new(Field::new(
                    "element",
                    DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
                    false,
                ))),
                false,
            ),
        ],
        HashMap::from([("owner".into(), "table author".into())]),
    );
    let encoded = encode_inspection_json(u64::MAX, &schema);
    assert_eq!(encoded.last(), Some(&b'\n'));
    assert_eq!(encoded.iter().filter(|&&byte| byte == b'\n').count(), 1);
    let actual: Value = serde_json::from_slice(&encoded).unwrap();
    let schema_document = capabilities::DiscoverySchema::Inspection.schema_document();
    jsonschema::validate(&schema_document["schema"], &actual).unwrap();
    // This literal freezes the upstream Arrow encoding within format version 1.
    assert_eq!(
        actual,
        json!({
            "format_version": 1, "table_version": "18446744073709551615",
            "schema": {"fields": [
                {"name": "profile", "nullable": true, "dict_id": 0, "dict_is_ordered": false, "metadata": {},
                 "data_type": {"Struct": [
                     {"name": "amount", "data_type": {"Decimal128": [20, 4]}, "nullable": false,
                      "dict_id": 0, "dict_is_ordered": false, "metadata": {"comment": "secret user metadata"}}
                 ]}},
                {"name": "times", "nullable": false, "dict_id": 0, "dict_is_ordered": false, "metadata": {},
                 "data_type": {"List": {
                     "name": "element", "data_type": {"Timestamp": ["Microsecond", "UTC"]}, "nullable": false,
                     "dict_id": 0, "dict_is_ordered": false, "metadata": {}
                 }}}
            ], "metadata": {"owner": "table author"}}
        })
    );
    let decoded: Schema = serde_json::from_value(actual["schema"].clone()).unwrap();
    assert_eq!(decoded, schema);
}

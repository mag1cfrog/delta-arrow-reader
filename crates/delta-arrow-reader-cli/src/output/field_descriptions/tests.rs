use std::{collections::HashMap, sync::Arc};

use arrow_schema::{DataType, Field, TimeUnit, UnionFields, UnionMode};

use super::*;

#[test]
fn nested_types_preserve_arrow_semantics_and_metadata_is_opt_in() {
    let metadata = HashMap::from([
        ("comment".to_owned(), "private nested metadata".to_owned()),
        (
            "dict_id".to_owned(),
            "a metadata key, not a field attribute".to_owned(),
        ),
    ]);
    let child =
        Arc::new(Field::new("子列", DataType::Decimal128(18, 4), false).with_metadata(metadata));
    let types = [
        DataType::Timestamp(TimeUnit::Microsecond, Some("private/timezone".into())),
        DataType::Struct(vec![child.clone()].into()),
        DataType::List(child.clone()),
        DataType::LargeList(child.clone()),
        DataType::ListView(child.clone()),
        DataType::LargeListView(child.clone()),
        DataType::FixedSizeList(child.clone(), 3),
        DataType::Map(child.clone(), true),
        DataType::Union(
            UnionFields::try_new([2], [child.clone()]).unwrap(),
            UnionMode::Dense,
        ),
        DataType::Dictionary(
            Box::new(DataType::Int32),
            Box::new(DataType::Struct(vec![child.clone()].into())),
        ),
        DataType::RunEndEncoded(Arc::new(Field::new("ends", DataType::Int32, false)), child),
    ];
    for data_type in types {
        let field = Field::new("selected", data_type, true);
        for include_metadata in [false, true] {
            let detail = field_details(&field, include_metadata);
            let text = detail.to_string();
            if !include_metadata {
                assert!(
                    !text.contains("metadata") && !text.contains("dict_id"),
                    "{text}"
                );
            }
            let full = json!(field);
            assert_same_selected_values(&detail, &full, include_metadata);
        }
    }
}

fn assert_same_selected_values(selected: &Value, full: &Value, include_metadata: bool) {
    match full {
        Value::Object(properties) => {
            for (key, value) in properties {
                if key == "dict_id"
                    || (key == "dict_is_ordered" && full["data_type"].get("Dictionary").is_none())
                {
                    continue;
                }
                if key == "metadata" {
                    if include_metadata && !value.as_object().unwrap().is_empty() {
                        assert_eq!(&selected[key], value);
                    } else {
                        assert!(selected.get(key).is_none());
                    }
                } else {
                    assert_same_selected_values(&selected[key], value, include_metadata);
                }
            }
        }
        Value::Array(values) => {
            assert_eq!(selected.as_array().unwrap().len(), values.len());
            for (index, value) in values.iter().enumerate() {
                assert_same_selected_values(&selected[index], value, include_metadata);
            }
        }
        _ => assert_eq!(selected, full),
    }
}

#[test]
fn dictionary_ordering_is_preserved_at_the_root_and_in_nested_fields() {
    let dictionary = Field::new_dictionary("labels", DataType::Int32, DataType::Utf8, true)
        .with_dict_is_ordered(true);
    let nested = Field::new(
        "row",
        DataType::Struct(vec![dictionary.clone()].into()),
        false,
    );
    for include_metadata in [false, true] {
        let detail = field_details(&dictionary, include_metadata);
        assert_eq!(detail["dict_is_ordered"], true);
        assert_same_selected_values(&detail, &json!(dictionary), include_metadata);
        let detail = field_details(&nested, include_metadata);
        assert_eq!(detail["data_type"]["Struct"][0]["dict_is_ordered"], true);
        assert_same_selected_values(&detail, &json!(nested), include_metadata);
    }
}

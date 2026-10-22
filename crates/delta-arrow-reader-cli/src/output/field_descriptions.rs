//! Column descriptions shared by focused inspection and typed JSON rows.
//!
//! Retain names, nullability, and Arrow type parameters, including nested fields.
//! Omit optional metadata unless requested; physical dictionary IDs do not
//! describe the decoded values in JSON.

use arrow_schema::{DataType, Field};
use serde_json::{Value, json};

pub(crate) fn field_details(field: &Field, include_metadata: bool) -> Value {
    let mut result = json!({
        "name": field.name(),
        "data_type": type_details(field.data_type(), include_metadata),
        "nullable": field.is_nullable(),
    });
    if let Some(ordered) = field.dict_is_ordered() {
        result["dict_is_ordered"] = json!(ordered);
    }
    if include_metadata && !field.metadata().is_empty() {
        result["metadata"] = json!(field.metadata());
    }
    result
}

fn type_details(data_type: &DataType, include_metadata: bool) -> Value {
    // Keep Arrow's type encoding while applying the metadata policy to nested fields.
    match data_type {
        DataType::List(field) => json!({"List": field_details(field, include_metadata)}),
        DataType::LargeList(field) => json!({"LargeList": field_details(field, include_metadata)}),
        DataType::ListView(field) => json!({"ListView": field_details(field, include_metadata)}),
        DataType::LargeListView(field) => {
            json!({"LargeListView": field_details(field, include_metadata)})
        }
        DataType::FixedSizeList(field, size) => {
            json!({"FixedSizeList": [field_details(field, include_metadata), size]})
        }
        DataType::Struct(fields) => {
            json!({"Struct": fields.iter().map(|field| field_details(field, include_metadata)).collect::<Vec<_>>()})
        }
        DataType::Map(field, sorted) => {
            json!({"Map": [field_details(field, include_metadata), sorted]})
        }
        DataType::Union(fields, mode) => {
            json!({"Union": [fields.iter().map(|(id, field)| json!([id, field_details(field, include_metadata)])).collect::<Vec<_>>(), mode]})
        }
        DataType::Dictionary(keys, values) => {
            json!({"Dictionary": [type_details(keys, include_metadata), type_details(values, include_metadata)]})
        }
        DataType::RunEndEncoded(ends, values) => {
            json!({"RunEndEncoded": [field_details(ends, include_metadata), field_details(values, include_metadata)]})
        }
        _ => json!(data_type),
    }
}

#[cfg(test)]
mod tests {
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
        let child = Arc::new(
            Field::new("子列", DataType::Decimal128(18, 4), false).with_metadata(metadata),
        );
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
                    if key == "dict_id" || (key == "dict_is_ordered" && selected.get(key).is_none())
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
}

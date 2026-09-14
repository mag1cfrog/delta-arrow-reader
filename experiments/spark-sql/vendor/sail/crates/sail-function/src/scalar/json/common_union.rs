// Modified from Sail v0.7.1 for the Delta reader experiment. See experiments/spark-sql/UPSTREAM.md in the host repository.
// https://github.com/datafusion-contrib/datafusion-functions-json/blob/cb1ba7a80a84e10a4d658f3100eae8f6bca2ced9/LICENSE
//
// [Credit]: https://github.com/datafusion-contrib/datafusion-functions-json/blob/78c5abbf7222510ff221517f5d2e3c344969da98/src/common_union.rs

use std::collections::HashMap;
use std::sync::{Arc, LazyLock, OnceLock};

use datafusion::arrow::array::{ArrayRef, AsArray, StringArray, UnionArray};
use datafusion::arrow::datatypes::{DataType, Field, UnionFields, UnionMode};
use datafusion::common::ScalarValue;

pub fn is_json_union(data_type: &DataType) -> bool {
    match data_type {
        DataType::Union(fields, UnionMode::Sparse) => fields == &union_fields(),
        _ => false,
    }
}

/// Extract nested JSON from a JSON `UnionArray`
///
/// # Arguments
/// * `array` - The `UnionArray` to extract the nested JSON from
/// * `object_lookup` - If `true`, extract from the "object" member of the union,
///   otherwise extract from the "array" member
pub(crate) fn nested_json_array(array: &ArrayRef, object_lookup: bool) -> Option<&StringArray> {
    nested_json_array_ref(array, object_lookup).map(AsArray::as_string)
}

pub(crate) fn nested_json_array_ref(array: &ArrayRef, object_lookup: bool) -> Option<&ArrayRef> {
    let union_array: &UnionArray = array.as_any().downcast_ref::<UnionArray>()?;
    let type_id = if object_lookup {
        TYPE_ID_OBJECT
    } else {
        TYPE_ID_ARRAY
    };
    Some(union_array.child(type_id))
}

/// Extract a JSON string from a JSON union scalar
pub(crate) fn json_from_union_scalar<'a>(
    type_id_value: Option<&'a (i8, Box<ScalarValue>)>,
    fields: &UnionFields,
) -> Option<&'a str> {
    if let Some((type_id, value)) = type_id_value {
        // we only want to take the ScalarValue string if the type_id indicates the value represents nested JSON
        if fields == &union_fields()
            && (*type_id == TYPE_ID_ARRAY || *type_id == TYPE_ID_OBJECT)
            && let ScalarValue::Utf8(s) | ScalarValue::Utf8View(s) | ScalarValue::LargeUtf8(s) =
                value.as_ref()
        {
            return s.as_deref();
        }
    }
    None
}

pub static JSON_UNION_DATA_TYPE: LazyLock<DataType> =
    LazyLock::new(|| DataType::Union(union_fields(), UnionMode::Sparse));

pub(crate) const TYPE_ID_NULL: i8 = 0;
const TYPE_ID_BOOL: i8 = 1;
const TYPE_ID_INT: i8 = 2;
const TYPE_ID_FLOAT: i8 = 3;
const TYPE_ID_STR: i8 = 4;
const TYPE_ID_ARRAY: i8 = 5;
const TYPE_ID_OBJECT: i8 = 6;

fn union_fields() -> UnionFields {
    static FIELDS: OnceLock<UnionFields> = OnceLock::new();
    FIELDS
        .get_or_init(|| {
            let json_metadata: HashMap<String, String> =
                HashMap::from_iter(vec![("is_json".to_string(), "true".to_string())]);
            UnionFields::from_iter([
                (
                    TYPE_ID_NULL,
                    Arc::new(Field::new("null", DataType::Null, true)),
                ),
                (
                    TYPE_ID_BOOL,
                    Arc::new(Field::new("bool", DataType::Boolean, false)),
                ),
                (
                    TYPE_ID_INT,
                    Arc::new(Field::new("int", DataType::Int64, false)),
                ),
                (
                    TYPE_ID_FLOAT,
                    Arc::new(Field::new("float", DataType::Float64, false)),
                ),
                (
                    TYPE_ID_STR,
                    Arc::new(Field::new("str", DataType::Utf8, false)),
                ),
                (
                    TYPE_ID_ARRAY,
                    Arc::new(
                        Field::new("array", DataType::Utf8, false)
                            .with_metadata(json_metadata.clone()),
                    ),
                ),
                (
                    TYPE_ID_OBJECT,
                    Arc::new(
                        Field::new("object", DataType::Utf8, false)
                            .with_metadata(json_metadata.clone()),
                    ),
                ),
            ])
        })
        .clone()
}

#[cfg(test)]
#[expect(clippy::unwrap_used)]
mod test {
    use datafusion::arrow::array::Array;

    use super::*;

    #[test]
    fn test_json_union() {
        let expected = vec![
            ScalarValue::Null,
            ScalarValue::Boolean(Some(true)),
            ScalarValue::Boolean(Some(false)),
            ScalarValue::Int64(Some(42)),
            ScalarValue::Float64(Some(42.0)),
            ScalarValue::Utf8(Some("foo".into())),
            ScalarValue::Utf8(Some("[42]".into())),
            ScalarValue::Utf8(Some(r#"{"foo": 42}"#.into())),
            ScalarValue::Null,
        ];
        let type_ids = vec![0, 1, 1, 2, 3, 4, 5, 6, 0];
        let children = union_fields()
            .iter()
            .map(|(id, field)| {
                ScalarValue::iter_to_array(type_ids.iter().zip(&expected).map(|(row_id, value)| {
                    if *row_id == id {
                        value.clone()
                    } else {
                        ScalarValue::try_from(field.data_type()).unwrap()
                    }
                }))
                .unwrap()
            })
            .collect();
        let union_array =
            UnionArray::try_new(union_fields(), type_ids.into(), None, children).unwrap();
        assert!(is_json_union(union_array.data_type()));
        assert_eq!(union_array.data_type(), &*JSON_UNION_DATA_TYPE);
        assert_eq!(
            union_array.type_ids().as_ref(),
            &[0, 1, 1, 2, 3, 4, 5, 6, 0]
        );
        let values_after: Vec<_> = (0..union_array.len())
            .map(|idx| {
                ScalarValue::try_from_array(union_array.child(union_array.type_id(idx)), idx)
                    .unwrap()
            })
            .collect();
        assert_eq!(values_after, expected);
        let array: ArrayRef = Arc::new(union_array.clone());
        assert_eq!(nested_json_array(&array, false).unwrap().value(6), "[42]");
        assert_eq!(
            nested_json_array(&array, true).unwrap().value(7),
            r#"{"foo": 42}"#
        );
        for (idx, value) in expected.into_iter().enumerate() {
            let scalar = Some((union_array.type_id(idx), Box::new(value)));
            let json = match idx {
                6 => Some("[42]"),
                7 => Some(r#"{"foo": 42}"#),
                _ => None,
            };
            assert_eq!(
                json_from_union_scalar(scalar.as_ref(), &union_fields()),
                json
            );
        }
    }
}

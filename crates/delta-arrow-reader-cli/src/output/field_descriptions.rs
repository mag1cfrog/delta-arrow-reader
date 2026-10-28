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
mod tests;

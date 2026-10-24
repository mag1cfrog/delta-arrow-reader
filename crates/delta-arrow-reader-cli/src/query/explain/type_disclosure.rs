//! Return exact selected Arrow types without repeating text derived from SQL literals.
//!
//! A primitive expression can have a name containing its literal value. Struct
//! child names and timezone parameters can also come from SQL, so these require
//! a fixed vocabulary before the existing field serializer can be used.

use arrow_schema::{DataType, Field};
use serde_json::{Value, json};

use crate::{Error, output::field_descriptions::field_details};

pub(super) fn describe_plan_field(index: usize, field: &Field) -> Result<Value, Error> {
    if !type_has_only_public_text(field.data_type()) {
        return Err(super::invalid_selection(
            "--field",
            "a type without arbitrary nested field names or timezone text",
            "This exact type could disclose SQL literals. Select a different field or request the summary; increasing the byte budget cannot make it safe.",
        ));
    }
    let mut description = field_details(field, false);
    if let Some(fields) = description.as_object_mut() {
        fields.remove("name");
    }
    description["index"] = json!(index);
    Ok(description)
}

fn type_has_only_public_text(data_type: &DataType) -> bool {
    match data_type {
        DataType::Timestamp(_, timezone) => timezone.as_deref().is_none_or(|zone| zone == "UTC"),
        DataType::List(field)
        | DataType::LargeList(field)
        | DataType::ListView(field)
        | DataType::LargeListView(field)
        | DataType::FixedSizeList(field, _)
        | DataType::Map(field, _) => nested_field_has_only_public_text(field),
        DataType::Struct(fields) => fields
            .iter()
            .all(|field| nested_field_has_only_public_text(field)),
        DataType::Union(fields, _) => fields
            .iter()
            .all(|(_, field)| nested_field_has_only_public_text(field)),
        DataType::Dictionary(keys, values) => {
            type_has_only_public_text(keys) && type_has_only_public_text(values)
        }
        DataType::RunEndEncoded(ends, values) => {
            nested_field_has_only_public_text(ends) && nested_field_has_only_public_text(values)
        }
        _ => true,
    }
}

fn nested_field_has_only_public_text(field: &Field) -> bool {
    matches!(
        field.name().as_str(),
        "item" | "element" | "entries" | "key" | "value" | "keys" | "values" | "run_ends"
    ) && type_has_only_public_text(field.data_type())
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_schema::{TimeUnit, UnionFields, UnionMode};
    use std::sync::Arc;

    #[test]
    fn nested_type_checks_cover_every_container_and_preserve_safe_types() {
        let private = Arc::new(Field::new("secret-child", DataType::Int64, true));
        for data_type in [
            DataType::List(private.clone()),
            DataType::LargeList(private.clone()),
            DataType::ListView(private.clone()),
            DataType::LargeListView(private.clone()),
            DataType::FixedSizeList(private.clone(), 2),
            DataType::Map(private.clone(), false),
            DataType::Struct(vec![private.clone()].into()),
            DataType::Union(
                UnionFields::try_new(vec![0], vec![private.clone()]).unwrap(),
                UnionMode::Dense,
            ),
            DataType::Dictionary(
                Box::new(DataType::Int8),
                Box::new(DataType::List(private.clone())),
            ),
            DataType::RunEndEncoded(
                Arc::new(Field::new("run_ends", DataType::Int32, false)),
                private,
            ),
            DataType::Timestamp(TimeUnit::Microsecond, Some("secret-zone".into())),
        ] {
            let field = Field::new("secret-parent", data_type, true);
            assert!(describe_plan_field(0, &field).is_err());
        }
        let child = Field::new(
            "item",
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            true,
        )
        .with_metadata([("secret-child-key".into(), "secret-child-value".into())].into());
        let field = Field::new("secret-name", DataType::List(Arc::new(child)), false)
            .with_metadata([("secret-key".into(), "secret-value".into())].into());
        let described = describe_plan_field(7, &field).unwrap();
        assert!(!described.to_string().contains("secret"));
        assert_eq!(described["index"], 7);
        assert_eq!(described["data_type"]["List"]["name"], "item");
        assert_eq!(
            described["data_type"]["List"]["data_type"],
            json!({"Timestamp": ["Microsecond", "UTC"]})
        );
        assert_eq!(described["nullable"], false);
    }
}

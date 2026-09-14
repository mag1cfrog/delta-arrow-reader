// Modified from Sail v0.7.1 for the Delta reader experiment. See experiments/spark-sql/UPSTREAM.md in the host repository.
use std::collections::HashMap;

use arrow_schema::extension::ExtensionType;
use arrow_schema::{DataType, Field};
use parquet_variant_compute::VariantType;

pub const VARIANT_METADATA_FIELD_NAME: &str = "metadata";
pub const VARIANT_VALUE_FIELD_NAME: &str = "value";
pub const VARIANT_TYPED_VALUE_FIELD_NAME: &str = "typed_value";
pub const VARIANT_METADATA_MARKER_KEY: &str = "variant";
pub const VARIANT_METADATA_MARKER_VALUE: &str = "true";

pub fn is_variant_storage_field(field: &Field) -> bool {
    let has_variant_extension = field.extension_type_name() == Some(VariantType::NAME)
        && field.try_extension_type::<VariantType>().is_ok();
    has_variant_extension || is_marked_variant_storage_type(field.data_type())
}

pub fn variant_metadata_field(data_type: DataType, nullable: bool) -> Field {
    Field::new(VARIANT_METADATA_FIELD_NAME, data_type, nullable).with_metadata(HashMap::from([(
        VARIANT_METADATA_MARKER_KEY.to_string(),
        VARIANT_METADATA_MARKER_VALUE.to_string(),
    )]))
}

pub fn is_variant_metadata_field(field: &Field) -> bool {
    field.name() == VARIANT_METADATA_FIELD_NAME
        && field
            .metadata()
            .get(VARIANT_METADATA_MARKER_KEY)
            .is_some_and(|value| value == VARIANT_METADATA_MARKER_VALUE)
        && is_binary_variant_field(field)
}

pub fn is_marked_variant_storage_type(data_type: &DataType) -> bool {
    let DataType::Struct(fields) = data_type else {
        return false;
    };
    let has_metadata = fields.iter().any(|field| is_variant_metadata_field(field));
    let has_value = fields
        .iter()
        .any(|field| field.name() == VARIANT_VALUE_FIELD_NAME && is_binary_variant_field(field));
    let has_typed_value = fields
        .iter()
        .any(|field| field.name() == VARIANT_TYPED_VALUE_FIELD_NAME);
    has_metadata && (has_value || has_typed_value)
}

pub fn is_binary_variant_field(field: &Field) -> bool {
    matches!(
        field.data_type(),
        DataType::Binary | DataType::LargeBinary | DataType::BinaryView
    )
}

// Modified from Sail v0.7.1 for the Delta reader experiment. See experiments/spark-sql/UPSTREAM.md in the host repository.
/// [Credit]: <https://github.com/datafusion-contrib/datafusion-variant/blob/51e0d4be62d7675e9b7b56ed1c0b0a10ae4a28d7/src/shared.rs>
use arrow_schema::extension::ExtensionType;
use arrow_schema::{DataType, Field};
use datafusion_common::{ScalarValue, exec_err};
use parquet_variant_compute::{VariantArray, VariantType};
use sail_common_datafusion::variant::is_variant_storage_field;

pub fn try_field_as_variant_array(field: &Field) -> datafusion_common::Result<()> {
    // Accept Null type (for parse_json(null) case)
    if matches!(field.data_type(), DataType::Null) {
        return Ok(());
    }

    if !is_variant_storage_field(field) {
        return exec_err!("field does not have extension type VariantType");
    }

    VariantType.supports_data_type(field.data_type())?;

    Ok(())
}
pub fn try_parse_variant_scalar(scalar: &ScalarValue) -> datafusion_common::Result<VariantArray> {
    let v = match scalar {
        ScalarValue::Struct(v) => v,
        unsupported => {
            return exec_err!(
                "expected variant scalar value, got data type: {}",
                unsupported.data_type()
            );
        }
    };

    VariantArray::try_new(v.as_ref()).map_err(Into::into)
}

pub fn try_parse_string_scalar(scalar: &ScalarValue) -> datafusion_common::Result<Option<&String>> {
    let b = match scalar {
        ScalarValue::Null => return Ok(None),
        ScalarValue::Utf8(s) | ScalarValue::Utf8View(s) | ScalarValue::LargeUtf8(s) => s,
        unsupported => {
            return exec_err!(
                "expected string scalar value, got data type: {}",
                unsupported.data_type()
            );
        }
    };

    Ok(b.as_ref())
}

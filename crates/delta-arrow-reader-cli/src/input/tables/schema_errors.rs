//! Field diagnostics for table declarations and their authorization references.

use serde_json::value::RawValue;

use super::{ManifestAuthorization, ManifestTable, TablesManifest};
use crate::{
    diagnostics::InputCorrection,
    input::schema_errors::{InputFieldLocation, ObjectFields, diagnose_object_schema},
};

pub(in crate::input) fn diagnose_schema_error(
    raw: &RawValue,
    location: &InputFieldLocation,
) -> Option<InputCorrection> {
    if let Some(correction) = diagnose_object_schema::<TablesManifest>(raw, location) {
        return Some(correction);
    }
    // Nested Value maps discard duplicate keys; recheck each object from the raw input.
    let fields: ObjectFields<&RawValue> = serde_json::from_str(raw.get()).ok()?;
    let tables: Vec<&RawValue> = serde_json::from_str(fields.field_value("tables")?.get()).ok()?;
    for (index, table) in tables.into_iter().enumerate() {
        let table_location = location.child_field("tables").array_element(index);
        if let Some(correction) = diagnose_object_schema::<ManifestTable>(table, &table_location) {
            return Some(correction);
        }
        let fields: ObjectFields<&RawValue> = serde_json::from_str(table.get()).ok()?;
        if let Some(authorization) = fields.field_value("authorization")
            && let Some(correction) = diagnose_object_schema::<ManifestAuthorization>(
                authorization,
                &table_location.child_field("authorization"),
            )
        {
            return Some(correction);
        }
    }
    None
}

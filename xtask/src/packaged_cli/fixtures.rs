//! One wide snapshot with exact integers, nullable decimals, nested fields, and a large cell.

use std::{fs, path::Path, sync::Arc};

use arrow_array::{
    ArrayRef, Date32Array, Decimal128Array, Int32Array, Int64Array, RecordBatch, StringArray,
    StructArray,
};
use arrow_schema::{DataType, Field, Schema};
use parquet::arrow::ArrowWriter;
use serde_json::{Value, json};

use crate::{Result, scan::ipc::IpcTable};

pub(super) fn write_table(directory: &Path) -> Result<IpcTable> {
    let mut nested_type = DataType::Int64;
    let mut nested_array: ArrayRef = Arc::new(Int64Array::from(vec![7, 8]));
    let mut nested_delta_type = json!("long");
    for _ in 0..12 {
        let field = Arc::new(Field::new("child", nested_type, true));
        nested_array = Arc::new(StructArray::new(
            vec![field.clone()].into(),
            vec![nested_array],
            None,
        ));
        nested_type = DataType::Struct(vec![field].into());
        nested_delta_type =
            json!({"type": "struct", "fields": [delta_field("child", nested_delta_type)]});
    }
    let mut fields = vec![
        Field::new("id", DataType::Int64, true),
        Field::new("amount", DataType::Decimal128(18, 4), true),
        Field::new("day", DataType::Date32, true),
        Field::new("nested", nested_type, true),
        Field::new("large", DataType::Utf8, true),
    ];
    let mut arrays: Vec<ArrayRef> = vec![
        Arc::new(Int64Array::from(vec![
            9_007_199_254_740_993,
            9_007_199_254_740_994,
        ])),
        Arc::new(Decimal128Array::from(vec![Some(12345), None]).with_precision_and_scale(18, 4)?),
        Arc::new(Date32Array::from(vec![0, 1])),
        nested_array,
        Arc::new(StringArray::from(vec!["large-cell-".repeat(4096); 2])),
    ];
    let mut delta_fields = vec![
        delta_field("id", json!("long")),
        delta_field("amount", json!("decimal(18,4)")),
        delta_field("day", json!("date")),
        delta_field("nested", nested_delta_type),
        delta_field("large", json!("string")),
    ];
    delta_fields[0]["metadata"] = json!({"comment": "private-metadata-".repeat(4096)});
    for index in 0..995 {
        let name = format!("column_{index:04}");
        fields.push(Field::new(&name, DataType::Int32, true));
        arrays.push(Arc::new(Int32Array::from(vec![index, index + 1])));
        delta_fields.push(delta_field(&name, json!("integer")));
    }
    fs::create_dir(directory)?;
    fs::create_dir(directory.join("_delta_log"))?;
    let batch = RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays)?;
    let data = directory.join("part.parquet");
    let mut writer = ArrowWriter::try_new(fs::File::create(&data)?, batch.schema(), None)?;
    writer.write(&batch)?;
    writer.close()?;
    let actions = [
        json!({"protocol": {"minReaderVersion": 1, "minWriterVersion": 2}}),
        json!({"metaData": {"id": "packaged-agent-fixture", "format": {"provider": "parquet", "options": {}},
            "schemaString": json!({"type": "struct", "fields": delta_fields}).to_string(),
            "partitionColumns": [], "configuration": {}}}),
        json!({"add": {"path": "part.parquet", "partitionValues": {}, "size": fs::metadata(data)?.len(),
            "modificationTime": 0, "dataChange": true}}),
    ];
    fs::write(
        directory.join("_delta_log/00000000000000000000.json"),
        actions
            .iter()
            .map(|action| format!("{action}\n"))
            .collect::<String>(),
    )?;
    Ok(IpcTable {
        schema: batch.schema(),
        batches: vec![batch],
    })
}

fn delta_field(name: &str, data_type: Value) -> Value {
    json!({"name": name, "type": data_type, "nullable": true, "metadata": {}})
}

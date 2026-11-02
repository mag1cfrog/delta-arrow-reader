//! Public declarations and physical refinement are separate Spark observations.

use std::sync::Arc;

use arrow::array::{Array, ArrayData, Int32Array, ListArray, NullArray, StringArray, StructArray};
use arrow::buffer::Buffer;
use arrow::datatypes::{DataType, Field, Fields, Schema};
use arrow::record_batch::RecordBatch;
use datafusion::execution::SessionStateBuilder;
use datafusion::prelude::{SessionConfig, SessionContext};
use delta_arrow_reader_spark_sql::configure_spark_sql;
use serde_json::{Value, json};

#[path = "support/spark_inputs.rs"]
mod spark_inputs;
#[path = "support/spark_results.rs"]
mod spark_results;
use spark_results::{
    TestResult, capture_spark_query_observation, capture_spark_query_result, encode_value,
    validate_batch, validate_physical_schema,
};

fn records() -> TestResult<Vec<Value>> {
    // Selected records are verbatim from the two frozen Spark captures named in
    // the header. The three declaration records are rejection controls below.
    let records = include_str!("fixtures/schema-observations-spark.jsonl")
        .lines()
        .map(serde_json::from_str::<Value>)
        .collect::<Result<Vec<_>, _>>()?;
    assert_eq!(records[0]["spark_version"], "4.2.0");
    assert_eq!(records[0]["timezone"], "UTC");
    assert_eq!(records.len() - 1, 25);
    Ok(records)
}

fn session(batch_size: usize) -> SessionContext {
    SessionContext::new_with_state(
        configure_spark_sql(
            SessionStateBuilder::new()
                .with_default_features()
                .with_config(SessionConfig::new().with_batch_size(batch_size)),
        )
        .build(),
    )
}

async fn check_refinement_cases(optimizer_control: bool) -> TestResult {
    let records = records()?;
    let mut differences = Vec::new();
    for batch_size in [1, 32] {
        let session = session(batch_size);
        spark_inputs::register_recorded_inputs(&session, &records[0], batch_size)?;
        let mut checked = 0;
        for case in &records[1..] {
            let id = case["id"].as_str().ok_or("missing ID")?;
            let selected = if optimizer_control {
                id == "optimizer_narrowing"
            } else {
                id.ends_with("_literal_valid")
            };
            if !selected {
                continue;
            }
            checked += 1;
            let observation = capture_spark_query_observation(&session, case).await;
            if let Ok(actual) = &observation {
                assert_eq!(actual["result"]["schema"]["fields"][0]["nullable"], true);
                assert_eq!(actual["physical_schema"]["fields"][0]["nullable"], false);
                if optimizer_control {
                    assert_eq!(
                        case["expected"]["schema"],
                        case["schema_stages"]["analyzed"]
                    );
                    assert_eq!(actual["physical_schema"], case["schema_stages"]["executed"]);
                }
                eprintln!(
                    "SPARK_SCHEMA_OBSERVATION {}",
                    json!({"id":id,"input":case["input"],
                    "ansi":case["ansi"],"batch":batch_size,"observation":actual})
                );
            }
            if observation.as_ref().ok().map(|actual| &actual["result"]) != Some(&case["expected"])
            {
                differences.push(format!(
                    "{id} input={} ANSI={} batch={batch_size}: expected {}, got {observation:?}",
                    case["input"], case["ansi"], case["expected"]
                ));
            }
        }
        assert_eq!(checked, if optimizer_control { 6 } else { 16 });
    }
    assert!(differences.is_empty(), "{}", differences.join("\n"));
    Ok(())
}

#[test]
fn physical_refinement_keeps_names_types_nested_declarations_and_metadata_exact() -> TestResult {
    let source = Field::new("#0", DataType::Int32, true)
        .with_metadata([("source".into(), "retained".into())].into());
    let physical = source.clone().with_name("r").with_nullable(false);
    let resolved: Fields = vec![source.clone()].into();
    let names = vec!["r".into()];
    validate_physical_schema(&resolved, &names, &vec![physical.clone()].into())?;
    for faulty in [
        physical.clone().with_name("wrong"),
        physical.clone().with_data_type(DataType::Int64),
        physical.clone().with_metadata(Default::default()),
    ] {
        assert!(validate_physical_schema(&resolved, &names, &vec![faulty].into()).is_err());
    }
    assert!(validate_physical_schema(&resolved, &[], &vec![physical.clone()].into()).is_err());
    assert!(validate_physical_schema(&resolved, &names, &Fields::empty()).is_err());
    assert!(
        validate_physical_schema(
            &vec![source.with_nullable(false)].into(),
            &names,
            &vec![physical.with_nullable(true)].into(),
        )
        .is_err()
    );

    let child = Field::new("id", DataType::Int32, false)
        .with_metadata([("source".into(), "child".into())].into());
    let nested = Field::new("#0", DataType::Struct(vec![child.clone()].into()), true);
    for faulty in [
        child.clone().with_nullable(true),
        child.with_metadata(Default::default()),
    ] {
        let physical = Field::new("r", DataType::Struct(vec![faulty].into()), false);
        assert!(
            validate_physical_schema(&vec![nested.clone()].into(), &names, &vec![physical].into())
                .is_err()
        );
    }
    let array = Field::new(
        "#0",
        DataType::List(Arc::new(Field::new("item", DataType::Int32, false))),
        true,
    );
    let physical = Field::new(
        "r",
        DataType::List(Arc::new(Field::new("item", DataType::Int32, true))),
        false,
    );
    assert!(validate_physical_schema(&vec![array].into(), &names, &vec![physical].into()).is_err());
    Ok(())
}

#[test]
fn batch_validation_rejects_forbidden_logical_nulls_and_schema_changes() -> TestResult {
    let required = Arc::new(Schema::new(vec![Field::new("r", DataType::Null, false)]));
    // RecordBatch checks physical null counts, which are zero for NullArray.
    let batch = RecordBatch::try_new(required.clone(), vec![Arc::new(NullArray::new(1))])?;
    assert!(validate_batch(required.as_ref(), &batch).is_err());
    let nullable = Schema::new(vec![Field::new("r", DataType::Null, true)]);
    assert!(validate_batch(&nullable, &batch).is_err());

    let nullable = Arc::new(Schema::new(vec![Field::new("r", DataType::Int32, true)]));
    let batch = RecordBatch::try_new(
        nullable.clone(),
        vec![Arc::new(Int32Array::from(vec![None]))],
    )?;
    validate_batch(nullable.as_ref(), &batch)?;
    let required = Schema::new(vec![Field::new("r", DataType::Int32, false)]);
    assert!(validate_batch(&required, &batch).is_err());
    assert!(validate_physical_schema(required.fields(), &["r".into()], nullable.fields()).is_err());
    Ok(())
}

#[test]
fn nested_null_validation_uses_logical_validity_and_masks_null_parents() -> TestResult {
    let nulls = NullArray::new(1);
    let children: Fields = vec![Field::new("leaf", DataType::Null, false)].into();
    // Arrow's safe ArrayData validation also uses physical child null counts.
    let data = ArrayData::builder(DataType::Struct(children.clone()))
        .len(1)
        .add_child_data(nulls.to_data())
        .build()?;
    let values = StructArray::from(data);
    assert!(encode_value(&values, 0).is_err());
    let masked = StructArray::new_null(children, 1);
    assert_eq!(encode_value(&masked, 0)?, Value::Null);

    let data = ArrayData::builder(DataType::List(Arc::new(Field::new(
        "item",
        DataType::Null,
        false,
    ))))
    .len(1)
    .add_buffer(Buffer::from_slice_ref([0_i32, 1]))
    .add_child_data(nulls.to_data())
    .build()?;
    let values = ListArray::from(data);
    assert!(encode_value(&values, 0).is_err());
    assert_eq!(encode_value(&nulls, 0)?, Value::Null);
    assert_eq!(encode_value(&StringArray::from(vec![""]), 0)?, json!(""));
    Ok(())
}

#[tokio::test]
async fn optimizer_refinement_preserves_public_declarations() -> TestResult {
    check_refinement_cases(true).await
}

#[tokio::test]
async fn literal_casts_preserve_public_declarations() -> TestResult {
    check_refinement_cases(false).await
}

#[tokio::test]
async fn wrong_constructor_declarations_and_missing_metadata_remain_visible() -> TestResult {
    let records = records()?;
    let session = session(32);
    let children: Fields = vec![Arc::new(Field::new("id", DataType::Int32, true))].into();
    let schema = Arc::new(Schema::new(vec![
        Field::new("selected_id", DataType::Int32, true),
        Field::new("selected_struct", DataType::Struct(children.clone()), false),
        Field::new("left_id", DataType::Int32, true),
        Field::new("right_id", DataType::Int32, true),
    ]));
    // Deliberately wrong declarations model the faults, without making today's
    // named_struct or USING implementation a required failing behavior.
    session.register_batch(
        "fault_declarations",
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int32Array::from(vec![99])),
                Arc::new(StructArray::try_new(
                    children,
                    vec![Arc::new(Int32Array::from(vec![99]))],
                    None,
                )?),
                Arc::new(Int32Array::from(vec![0])),
                Arc::new(Int32Array::from(vec![0])),
            ],
        )?,
    )?;
    for (id, projection, rows) in [
        ("nested_scalar", "selected_id", json!([["99"]])),
        ("struct_column", "selected_struct", json!([[["99"]]])),
        ("using_hidden", "left_id,right_id", json!([["0", "0"]])),
    ] {
        let reference = records
            .iter()
            .find(|r| r["id"] == id)
            .ok_or("missing control")?;
        let case = json!({"sql":format!("SELECT {projection} FROM fault_declarations"),
            "ansi":true,"expected":{"schema":reference["expected"]["schema"],"rows":rows}});
        let actual = capture_spark_query_result(&session, &case).await?;
        assert_eq!(actual["rows"], case["expected"]["rows"]);
        match id {
            "nested_scalar" => assert_eq!(actual["schema"]["fields"][0]["nullable"], true),
            "struct_column" => assert_eq!(
                actual["schema"]["fields"][0]["type"]["fields"][0]["nullable"],
                true
            ),
            "using_hidden" => assert_eq!(actual["schema"]["fields"][1]["metadata"], json!({})),
            _ => unreachable!(),
        }
        assert_ne!(actual["schema"], reference["expected"]["schema"], "{id}");
        assert_ne!(actual, case["expected"], "{id}");
    }
    Ok(())
}

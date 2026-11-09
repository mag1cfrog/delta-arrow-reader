//! Collect the fixed #901 lifecycle matrix before semantic assertions are enabled.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use arrow::array::{ArrayRef, Int32Array, StringArray};
use arrow::compute::cast;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use datafusion::datasource::MemTable;
use datafusion::execution::SessionStateBuilder;
use datafusion::physical_plan::{
    ExecutionPlan, ExecutionPlanProperties, collect, collect_partitioned, displayable,
};
use datafusion::prelude::{SessionConfig, SessionContext};
use delta_arrow_reader_spark_sql::{SparkSqlOptions, configure_spark_sql, plan_query};
use serde_json::{Value, json};

#[allow(dead_code)]
#[path = "support/spark_results.rs"]
mod spark_results;
#[allow(dead_code)]
#[path = "support/string_binary.rs"]
mod string_binary;
use string_binary::{TestResult, c06_receipts};

const CASES: [(&str, usize); 8] = [
    ("rand901_occurrences", 1),
    ("rand901_repeat_collect", 2),
    ("rand901_two_partitions", 1),
    ("rand901_group_floor", 1),
    ("rand901_window_order", 1),
    ("rand901_window_lag", 1),
    ("rand901_filter", 1),
    ("rand901_selected_case_concat", 1),
];

fn session(batch_size: usize, partitions: usize) -> SessionContext {
    SessionContext::new_with_state(
        configure_spark_sql(
            SessionStateBuilder::new()
                .with_default_features()
                .with_config(
                    SessionConfig::new()
                        .with_batch_size(batch_size)
                        .with_target_partitions(partitions),
                ),
        )
        .build(),
    )
}

async fn register_input(
    session: &SessionContext,
    input: &Value,
    representation: usize,
    batch_size: usize,
    empty_batches: bool,
) -> TestResult<Value> {
    let name = input["name"].as_str().ok_or("missing input name")?;
    let declared = input["schema"]["fields"]
        .as_array()
        .ok_or("missing input fields")?;
    if declared.len() != 2
        || declared[0]["type"] != "integer"
        || declared[1]["type"] != "string"
    {
        return Err("unexpected #901 input schema".into());
    }
    let text_type = [DataType::Utf8, DataType::LargeUtf8, DataType::Utf8View][representation].clone();
    let fields = declared
        .iter()
        .zip([DataType::Int32, text_type.clone()])
        .map(|(field, data_type)| {
            Ok(Field::new(
                field["name"].as_str().ok_or("missing input field name")?,
                data_type,
                field["nullable"]
                    .as_bool()
                    .ok_or("missing input nullability")?,
            )
            .with_metadata(serde_json::from_value::<HashMap<String, String>>(
                field["metadata"].clone(),
            )?))
        })
        .collect::<TestResult<Vec<_>>>()?;
    let schema = Arc::new(Schema::new(fields));
    let mut partitions = Vec::new();
    for rows in input["partitions"]
        .as_array()
        .ok_or("missing input partitions")?
    {
        let rows = rows.as_array().ok_or("missing partition rows")?;
        let row_ids = rows
            .iter()
            .map(|row| Ok(row[0].as_str().ok_or("missing row ID")?.parse::<i32>()?))
            .collect::<TestResult<Vec<_>>>()?;
        let strings: ArrayRef = Arc::new(StringArray::from(
            rows.iter().map(|row| row[1].as_str()).collect::<Vec<_>>(),
        ));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(row_ids)),
                cast(&strings, &text_type)?,
            ],
        )?;
        let mut batches = Vec::new();
        if empty_batches {
            batches.push(batch.slice(0, 0));
        }
        for offset in (0..batch.num_rows()).step_by(batch_size) {
            batches.push(batch.slice(offset, batch_size.min(batch.num_rows() - offset)));
            if empty_batches {
                batches.push(batch.slice(offset, 0));
            }
        }
        partitions.push(batches);
    }
    let registered = partitions
        .iter()
        .map(|batches| c06_receipts::batches(batches, string_binary::encode_value))
        .collect::<TestResult<Vec<_>>>()?;
    session.register_table(
        name,
        Arc::new(MemTable::try_new(schema, partitions)?),
    )?;
    let provider = session.table_provider(name).await?;
    let state = session.state();
    let scan = provider.scan(&state, None, &[], None).await?;
    let partition_count = scan.output_partitioning().partition_count();
    let readback = collect_partitioned(scan.clone(), session.task_ctx()).await?;
    let readback = readback
        .iter()
        .map(|batches| c06_receipts::batches(batches, string_binary::encode_value))
        .collect::<TestResult<Vec<_>>>()?;
    if readback.len() != registered.len() || partition_count != registered.len() {
        return Err("provider partition count differs from the fixed input".into());
    }
    for (actual, authored) in readback.iter().zip(
        input["partitions"]
            .as_array()
            .ok_or("missing input partitions")?,
    ) {
        if actual["rows"] != *authored {
            return Err("provider partition rows differ from the fixed input".into());
        }
    }
    Ok(json!({"input_definition":input,"registered_partitions":registered,
        "provider_schema":c06_receipts::schema(provider.schema().as_ref()),
        "scan_schema":c06_receipts::schema(scan.schema().as_ref()),
        "scan_plan":format!("{}",displayable(scan.as_ref()).indent(true)),
        "partition_count":partition_count,"readback_partitions":readback}))
}

async fn collect_once(plan: Arc<dyn ExecutionPlan>, session: &SessionContext) -> Value {
    let mut observed = json!({
        "before_collect":c06_receipts::schema(plan.schema().as_ref()),
        "executed_schema":null,"output":null,"error":null,
    });
    let result = collect(plan.clone(), session.task_ctx()).await;
    observed["executed_schema"] = c06_receipts::schema(plan.schema().as_ref());
    observed["executed_sql_schema"] = match string_binary::capture_schema(
        plan.schema().fields(),
        None,
    ) {
        Ok(schema) => schema,
        Err(error) => {
            observed["schema_encoding_error"] =
                c06_receipts::failure(error.as_ref(), "schema_encode");
            Value::Null
        }
    };
    let batches = match result {
        Ok(batches) => batches,
        Err(error) => {
            observed["error"] = c06_receipts::failure(&error, "collect");
            return observed;
        }
    };
    match c06_receipts::batches(&batches, string_binary::encode_value) {
        Ok(output) => observed["output"] = output,
        Err(error) => observed["error"] = c06_receipts::failure(error.as_ref(), "encode"),
    }
    // The second encoder preserves exact floating-point bits beside decimal rows.
    match c06_receipts::batches(&batches, spark_results::encode_value) {
        Ok(output) => observed["bit_encoded_output"] = output,
        Err(error) => {
            observed["bit_encoding_error"] = c06_receipts::failure(error.as_ref(), "bit_encode");
        }
    }
    observed
}

async fn observe(
    input: Value,
    case: Value,
    ansi: bool,
    representation: usize,
    batch_size: usize,
    empty_batches: bool,
) -> Value {
    let mut observed = json!({"provider":null,"public_names":null,"analyzed":null,
        "physical":null,"collections":[],"error":null});
    let mut stage = "provider_setup";
    let result: TestResult<()> = async {
        let partitions = input["partitions"]
            .as_array()
            .ok_or("missing partitions")?
            .len();
        let session = session(batch_size, partitions);
        observed["provider"] = register_input(
            &session,
            &input,
            representation,
            batch_size,
            empty_batches,
        )
        .await?;
        stage = "resolve";
        let query = plan_query(
            &session,
            case["sql"].as_str().ok_or("missing SQL")?,
            &SparkSqlOptions {
                ansi_mode: ansi,
                session_timezone: "UTC".into(),
                ..Default::default()
            },
        )
        .await?;
        observed["public_names"] = json!(query.column_names);
        observed["analyzed"] = c06_receipts::schema(query.logical_plan.schema().as_arrow());
        observed["analyzed_plan"] = json!(format!("{:?}", query.logical_plan));
        stage = "create_physical_plan";
        let plan = query.create_physical_plan(&session).await?;
        observed["physical"] = c06_receipts::schema(plan.schema().as_ref());
        observed["physical_plan"] =
            json!(format!("{}", displayable(plan.as_ref()).indent(true)));
        observed["physical_plan_debug"] = json!(format!("{plan:?}"));
        let mut collections = Vec::new();
        for index in 0..case["collect_count"]
            .as_u64()
            .ok_or("missing collect count")?
        {
            collections.push(json!({"mode":"sequential","index":index,
                "actual":collect_once(plan.clone(), &session).await}));
        }
        if case["id"] == "rand901_repeat_collect" {
            let (left, right) = tokio::join!(
                collect_once(plan.clone(), &session),
                collect_once(plan.clone(), &session),
            );
            collections.push(json!({"mode":"concurrent","index":0,"actual":left}));
            collections.push(json!({"mode":"concurrent","index":1,"actual":right}));
        }
        observed["collections"] = json!(collections);
        Ok(())
    }
    .await;
    if let Err(error) = result {
        observed["error"] = c06_receipts::failure(error.as_ref(), stage);
    }
    observed
}

#[tokio::test]
#[ignore = "Set DAR_C06_RAND901_INPUTS to the fixed external input packet; baseline capture only."]
async fn collect_seeded_rand_lifecycle_baseline() -> TestResult {
    let path = std::env::var("DAR_C06_RAND901_INPUTS")?;
    let packet: Value = serde_json::from_slice(&std::fs::read(&path)?)?;
    assert_eq!(packet["format_version"], 1);
    assert_eq!(packet["ansi_modes"], json!([false, true]));
    let definitions = packet["definitions"].as_array().ok_or("missing definitions")?;
    let expected_ids = CASES.iter().map(|(id, _)| *id).collect::<HashSet<_>>();
    let actual_ids = definitions
        .iter()
        .map(|case| Ok(case["id"].as_str().ok_or("missing case ID")?))
        .collect::<TestResult<HashSet<_>>>()?;
    assert_eq!(definitions.len(), 8);
    assert_eq!(actual_ids, expected_ids);
    for (id, count) in CASES {
        assert_eq!(
            definitions
                .iter()
                .find(|case| case["id"] == id)
                .ok_or("missing fixed case")?["collect_count"],
            count,
        );
    }
    let inputs = packet["registered_inputs"]
        .as_array()
        .ok_or("missing inputs")?;
    assert_eq!(inputs.len(), 2);
    let mut expected_keys = HashSet::new();
    let mut expected_collection_slots = 0;
    for (id, count) in CASES {
        for ansi in [false, true] {
            for representation in 0..3 {
                for (batch_size, empty_batches) in [(1, false), (32, false), (2, false), (1, true)] {
                    if id != "rand901_repeat_collect" && (batch_size == 2 || empty_batches) {
                        continue;
                    }
                    expected_keys.insert(
                        json!([id, ansi, representation, batch_size, empty_batches]).to_string(),
                    );
                    expected_collection_slots +=
                        count + if id == "rand901_repeat_collect" { 2 } else { 0 };
                }
            }
        }
    }
    assert_eq!(expected_keys.len(), 108);
    assert_eq!(expected_collection_slots, 180);
    eprintln!("c06 rand901 baseline header: {}", json!({"input_path":path,"input_packet":packet,
        "expected_contexts":108,"expected_collection_slots":180,"compatibility_assessed":false}));
    let mut keys = HashSet::new();
    let mut collection_slots = 0;
    let mut completed_collections = 0;
    let mut context_errors = 0;
    for case in definitions {
        let input = inputs
            .iter()
            .find(|input| input["name"] == case["input"])
            .ok_or("missing selected input")?;
        for ansi in [false, true] {
            for representation in 0..3 {
                for (batch_size, empty_batches) in [(1, false), (32, false), (2, false), (1, true)] {
                    if case["id"] != "rand901_repeat_collect" && (batch_size == 2 || empty_batches) {
                        continue;
                    }
                    let key = json!([case["id"], ansi, representation, batch_size, empty_batches])
                        .to_string();
                    assert!(keys.insert(key));
                    let slots = usize::try_from(
                        case["collect_count"].as_u64().ok_or("missing collect count")?,
                    )?
                        + if case["id"] == "rand901_repeat_collect" { 2 } else { 0 };
                    collection_slots += slots;
                    let task = tokio::spawn(observe(
                        input.clone(),
                        case.clone(),
                        ansi,
                        representation,
                        batch_size,
                        empty_batches,
                    ));
                    let actual = match task.await {
                        Ok(actual) => actual,
                        Err(error) => json!({"error":c06_receipts::failure(&error,"capture_task")}),
                    };
                    if actual["error"].is_null() {
                        let collections = actual["collections"].as_array().ok_or("missing collections")?;
                        assert_eq!(collections.len(), slots);
                        completed_collections += collections.len();
                    } else {
                        context_errors += 1;
                    }
                    eprintln!("c06 rand901 baseline observation: {}", json!({
                        "id":case["id"],"ansi":ansi,"sql":case["sql"],"representation":representation,
                        "batch_size":batch_size,"empty_batches":empty_batches,"input":case["input"],
                        "collection_slots":slots,"actual":actual,"compatibility_assessed":false}));
                }
            }
        }
    }
    eprintln!("c06 rand901 baseline summary: {}",json!({"contexts":keys.len(),
        "expected_contexts":108,"collection_slots":collection_slots,"expected_collection_slots":180,
        "completed_collections":completed_collections,"context_errors":context_errors,
        "compatibility_assessed":false}));
    assert_eq!(keys, expected_keys);
    assert_eq!(collection_slots, expected_collection_slots);
    Ok(())
}

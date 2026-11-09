//! Compare seeded RAND values and executed SQL types with independent Spark output.

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

const CASES: [(&str, usize); 9] = [
    ("rand901_occurrences", 1),
    ("rand901_repeat_collect", 2),
    ("rand901_two_partitions", 1),
    ("rand901_group_floor", 1),
    ("rand901_window_order", 1),
    ("rand901_window_lag", 1),
    ("rand901_filter", 1),
    ("rand901_selected_case_concat", 1),
    ("rand901_sibling_window_lag", 1),
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
    match batches
        .iter()
        .map(|batch| string_binary::capture_schema(batch.schema().fields(), None))
        .collect::<TestResult<Vec<_>>>()
    {
        Ok(schemas) => observed["executed_batch_sql_schemas"] = json!(schemas),
        Err(error) => {
            observed["schema_encoding_error"] =
                c06_receipts::failure(error.as_ref(), "batch_schema_encode");
        }
    }
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
            let barrier = Arc::new(tokio::sync::Barrier::new(2));
            let tasks = (0..2).map(|_| {
                let (plan, session, barrier) = (plan.clone(), session.clone(), barrier.clone());
                tokio::spawn(async move {
                    barrier.wait().await;
                    collect_once(plan, &session).await
                })
            }).collect::<Vec<_>>();
            for (index, task) in tasks.into_iter().enumerate() {
                let actual = match task.await {
                    Ok(actual) => actual,
                    Err(error) => json!({"error":c06_receipts::failure(&error,"concurrent_collect_task")}),
                };
                collections.push(json!({"mode":"concurrent","index":index,"actual":actual}));
            }
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

fn sql_types(schema: &Value) -> TestResult<Vec<Value>> {
    schema["fields"]
        .as_array()
        .ok_or("missing executed fields")?
        .iter()
        .map(|field| field.get("type").cloned().ok_or_else(|| "missing executed type".into()))
        .collect()
}

fn value_multiset(rows: &Value, types: &[Value]) -> TestResult<Vec<String>> {
    let mut rows = rows
        .as_array()
        .ok_or("missing complete rows")?
        .iter()
        .map(|row| {
            let cells = row.as_array().ok_or("missing row cells")?;
            if cells.len() != types.len() {
                return Err("row width differs from executed SQL types".into());
            }
            let cells = cells
                .iter()
                .zip(types)
                .map(|(value, data_type)| {
                    Ok(if data_type == "double" && !value.is_null() {
                        // Both engines supply finite decimal doubles; compare their exact bits.
                        json!(format!("{:016x}",value.as_str().ok_or("missing double")?.parse::<f64>()?.to_bits()))
                    } else {
                        value.clone()
                    })
                })
                .collect::<TestResult<Vec<_>>>()?;
            Ok(serde_json::to_string(&cells)?)
        })
        .collect::<TestResult<Vec<_>>>()?;
    rows.sort();
    Ok(rows)
}

fn compare_collection(actual: &Value, reference: &Value) -> TestResult<Value> {
    let expected = &reference["observed"];
    let types = sql_types(&expected["physical_schema"])?;
    let plan_types = sql_types(&actual["executed_sql_schema"])?;
    let batch_types = actual["executed_batch_sql_schemas"]
        .as_array()
        .ok_or("missing executed batch schemas")?
        .iter()
        .map(sql_types)
        .collect::<TestResult<Vec<_>>>()?;
    let rows_match = value_multiset(&actual["output"]["rows"], &types)?
        == value_multiset(&expected["rows"], &types)?;
    let types_match = plan_types == types && batch_types.iter().all(|batch| batch == &types);
    let errors_absent = ["error", "schema_encoding_error", "bit_encoding_error"]
        .iter()
        .all(|key| actual.get(*key).is_none_or(Value::is_null));
    Ok(json!({"rows_match":rows_match,"types_match":types_match,
        "errors_absent":errors_absent,"matches":rows_match && types_match && errors_absent,
        "expected_types":types,"executed_plan_types":plan_types,"executed_batch_types":batch_types}))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn seeded_rand_values_and_types_match_spark_across_lifecycles() -> TestResult {
    let mut packet: Value = serde_json::from_str(include_str!("fixtures/seeded-rand/inputs.json"))?;
    let sibling_packet: Value = serde_json::from_str(include_str!("fixtures/seeded-rand/sibling-inputs.json"))?;
    let records = include_str!("fixtures/seeded-rand/spark-reference.jsonl")
        .lines()
        .map(serde_json::from_str::<Value>)
        .collect::<Result<Vec<_>, _>>()?;
    let sibling_records = include_str!("fixtures/seeded-rand/sibling-spark-reference.jsonl")
        .lines()
        .map(serde_json::from_str::<Value>)
        .collect::<Result<Vec<_>, _>>()?;
    for (records, inputs, count) in [(&records, &packet, 18), (&sibling_records, &sibling_packet, 2)] {
        assert_eq!(records[0]["kind"], "run_start");
        assert_eq!(records[0]["inputs"], *inputs);
        assert_eq!(records[1]["kind"], "runtime_identity");
        assert_eq!(records[1]["spark_version"], "4.2.0");
        assert_eq!(records.last().ok_or("missing capture closure")?, &json!({
            "kind":"capture_complete","observations":count,"semantic_errors":0,
            "capture_metadata_or_partition_errors":false,
        }));
    }
    assert_eq!(packet["registered_inputs"], sibling_packet["registered_inputs"]);
    assert_eq!(packet["ansi_modes"], sibling_packet["ansi_modes"]);
    packet["definitions"].as_array_mut().ok_or("missing definitions")?.extend(
        sibling_packet["definitions"].as_array().ok_or("missing sibling definitions")?.iter().cloned(),
    );
    let references = records.iter().chain(&sibling_records)
        .filter(|record| record["kind"] == "observation")
        .collect::<Vec<_>>();
    assert_eq!(references.len(), 20);
    let expected_references = CASES.iter().flat_map(|(id, count)| {
        [false, true].into_iter().flat_map(move |ansi| {
            (0..*count).map(move |index| json!([id,ansi,index]).to_string())
        })
    }).collect::<HashSet<_>>();
    let actual_references = references.iter().map(|reference| {
        json!([reference["id"],reference["ansi"],reference["collect_index"]]).to_string()
    }).collect::<HashSet<_>>();
    assert_eq!(actual_references, expected_references);
    for reference in &references {
        assert_eq!(reference["observed"]["status"], "ok");
        assert_eq!(reference["observed"]["rows_complete"], true);
        assert_eq!(reference["observed"]["metadata_errors"], json!([]));
        assert_eq!(reference["definition"], packet["definitions"]
            .as_array().ok_or("missing reference input definitions")?
            .iter().find(|case| case["id"] == reference["id"])
            .ok_or("missing reference input definition")?.clone());
        assert_eq!(reference["sql"], reference["definition"]["sql"]);
    }
    assert_eq!(packet["format_version"], 1);
    assert_eq!(packet["ansi_modes"], json!([false, true]));
    let definitions = packet["definitions"].as_array().ok_or("missing definitions")?;
    let expected_ids = CASES.iter().map(|(id, _)| *id).collect::<HashSet<_>>();
    let actual_ids = definitions
        .iter()
        .map(|case| Ok(case["id"].as_str().ok_or("missing case ID")?))
        .collect::<TestResult<HashSet<_>>>()?;
    assert_eq!(definitions.len(), 9);
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
    assert_eq!(expected_keys.len(), 120);
    assert_eq!(expected_collection_slots, 192);
    eprintln!("c06 rand901 strict header: {}", json!({"input_packet":packet,
        "reference_fixtures":["fixtures/seeded-rand/spark-reference.jsonl",
            "fixtures/seeded-rand/sibling-spark-reference.jsonl"],
        "expected_contexts":120,"expected_collection_slots":192,
        "comparison":"complete_values_and_executed_sql_types"}));
    let mut keys = HashSet::new();
    let mut collection_slots = 0;
    let mut completed_collections = 0;
    let mut context_errors = 0;
    let mut failures = Vec::new();
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
                    let mut comparisons = Vec::new();
                    if actual["error"].is_null() {
                        if let Some(collections) = actual["collections"].as_array() {
                            let mut collection_keys = HashSet::new();
                            let expected_collections = if case["id"] == "rand901_repeat_collect" {
                                vec![("sequential",0),("sequential",1),("concurrent",0),("concurrent",1)]
                            } else {
                                vec![("sequential",0)]
                            };
                            let expected_collection_keys = expected_collections.into_iter()
                                .map(|(mode,index)| json!([mode,index]).to_string())
                                .collect::<HashSet<_>>();
                            completed_collections += collections.len();
                            for collection in collections {
                                collection_keys.insert(json!([collection["mode"],collection["index"]]).to_string());
                                let reference = references.iter().find(|reference| {
                                    reference["id"] == case["id"] && reference["ansi"] == ansi
                                        && reference["collect_index"] == collection["index"]
                                });
                                let comparison = match reference {
                                    Some(reference) => match compare_collection(&collection["actual"], reference) {
                                        Ok(comparison) => comparison,
                                        Err(error) => json!({"matches":false,
                                            "error":c06_receipts::failure(error.as_ref(),"compare")}),
                                    },
                                    None => json!({"matches":false,"error":"missing fixed Spark reference"}),
                                };
                                comparisons.push(json!({"mode":collection["mode"],"index":collection["index"],
                                    "comparison":comparison}));
                            }
                            if collections.len() != slots || collection_keys != expected_collection_keys {
                                comparisons.push(json!({"comparison":{"matches":false,
                                    "error":"incomplete collection inventory"}}));
                            }
                        } else {
                            comparisons.push(json!({"comparison":{"matches":false,"error":"missing collections"}}));
                        }
                    } else {
                        context_errors += 1;
                        comparisons.push(json!({"comparison":{"matches":false,"error":actual["error"]}}));
                    }
                    let matches = comparisons.iter().all(|comparison| comparison["comparison"]["matches"] == true);
                    let context = json!({
                        "id":case["id"],"ansi":ansi,"sql":case["sql"],"representation":representation,
                        "batch_size":batch_size,"empty_batches":empty_batches,"input":case["input"],
                    });
                    eprintln!("c06 rand901 strict observation: {}", json!({"context":context,
                        "collection_slots":slots,"actual":actual,"comparisons":comparisons,"matches":matches}));
                    if !matches {
                        failures.push(json!({"context":context,"comparisons":comparisons}));
                    }
                }
            }
        }
    }
    eprintln!("c06 rand901 strict summary: {}",json!({"contexts":keys.len(),
        "expected_contexts":120,"collection_slots":collection_slots,"expected_collection_slots":192,
        "completed_collections":completed_collections,"context_errors":context_errors,
        "failed_contexts":failures.len(),"values_and_executed_types_match":failures.is_empty()}));
    assert_eq!(keys, expected_keys);
    assert_eq!(collection_slots, expected_collection_slots);
    assert_eq!(completed_collections, 192);
    assert_eq!(context_errors, 0);
    assert!(failures.is_empty(), "{failures:#?}");
    Ok(())
}

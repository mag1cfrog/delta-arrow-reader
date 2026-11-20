//! Planning-only entry point, compiled as a child of the frozen #1379 harness.

use super::*;
use std::io::Cursor;

use arrow::ipc::{reader::StreamReader, writer::StreamWriter};
use datafusion::common::tree_node::TreeNodeRecursion;
use datafusion::logical_expr::LogicalPlan;
use datafusion::physical_plan::displayable;

// @event_functions@

fn schema_snapshot(schema: &Schema) -> TestResult<Value> {
    let mut bytes = Vec::new();
    let mut writer = StreamWriter::try_new(&mut bytes, schema)?;
    writer.finish()?;
    drop(writer);
    Ok(json!({"schema_ipc": bytes}))
}

fn logical_snapshot(plan: &LogicalPlan, names: Option<&[String]>) -> TestResult<Value> {
    let mut schemas = Vec::new();
    plan.apply_with_subqueries(|node| {
        schemas.push(json!({
            "arrow": schema_snapshot(node.schema().as_arrow()).map_err(|error| {
                datafusion::error::DataFusionError::External(error)
            })?,
            "qualifiers": node.schema().iter().map(|(qualifier, _)| qualifier.map(ToString::to_string)).collect::<Vec<_>>(),
            "functional_dependencies": format!("{:?}", node.schema().functional_dependencies())
        }));
        Ok(TreeNodeRecursion::Continue)
    })?;
    Ok(
        json!({"plan": plan.display_indent_schema().to_string(), "schemas": schemas, "column_names": names}),
    )
}

fn physical_snapshot(plan: &dyn ExecutionPlan) -> TestResult<Value> {
    let children = plan
        .children()
        .into_iter()
        .map(|child| physical_snapshot(child.as_ref()))
        .collect::<TestResult<Vec<_>>>()?;
    Ok(json!({
        "name": plan.name(), "schema": schema_snapshot(plan.schema().as_ref())?,
        "partitioning": format!("{:?}", plan.properties().output_partitioning()),
        "ordering": format!("{:?}", plan.properties().output_ordering()),
        "required_input_distribution": format!("{:?}", plan.required_input_distribution()),
        "required_input_ordering": format!("{:?}", plan.required_input_ordering()),
        "maintains_input_order": plan.maintains_input_order(),
        "children": children
    }))
}

fn decode_schema(value: &Value) -> TestResult<Arc<Schema>> {
    let bytes: Vec<u8> = serde_json::from_value(value.clone())?;
    Ok(StreamReader::try_new(Cursor::new(bytes), None)?.schema())
}

fn compare(expected: &Value, actual: &Value, path: &str) -> TestResult {
    if let (Some(left), Some(right)) = (expected.as_object(), actual.as_object()) {
        if left.len() != right.len() || !left.keys().all(|key| right.contains_key(key)) {
            return Err(format!("plan keys differ at {path}").into());
        }
        for (key, value) in left {
            if key == "schema_ipc" {
                if decode_schema(value)? != decode_schema(&right[key])? {
                    return Err(format!("exact Arrow schema differs at {path}").into());
                }
            } else {
                compare(value, &right[key], &format!("{path}/{key}"))?;
            }
        }
    } else if let (Some(left), Some(right)) = (expected.as_array(), actual.as_array()) {
        if left.len() != right.len() {
            return Err(format!("plan array length differs at {path}").into());
        }
        for (index, (left, right)) in left.iter().zip(right).enumerate() {
            compare(left, right, &format!("{path}/{index}"))?;
        }
    } else if expected != actual {
        return Err(format!("plan differs at {path}: {expected} != {actual}").into());
    }
    Ok(())
}

fn schema_self_check() -> TestResult {
    let schema = Schema::new(vec![Field::new("x", DataType::Int32, false)]);
    let reference = schema_snapshot(&schema)?;
    compare(&reference, &schema_snapshot(&schema)?, "roundtrip")?;
    let nullable = Schema::new(vec![Field::new("x", DataType::Int32, true)]);
    if compare(&reference, &schema_snapshot(&nullable)?, "nullability").is_ok() {
        return Err("schema check ignored nullability".into());
    }
    let metadata = Schema::new(vec![Field::new("x", DataType::Int32, false).with_metadata(
        std::collections::HashMap::from([("note".into(), "changed".into())]),
    )]);
    if compare(&reference, &schema_snapshot(&metadata)?, "metadata").is_ok() {
        return Err("schema check ignored field metadata".into());
    }
    let view = Schema::new(vec![Field::new("x", DataType::Utf8View, true)]);
    let string = Schema::new(vec![Field::new("x", DataType::Utf8, true)]);
    if compare(
        &schema_snapshot(&view)?,
        &schema_snapshot(&string)?,
        "storage",
    )
    .is_ok()
    {
        return Err("schema check ignored Arrow storage representation".into());
    }
    Ok(())
}

async fn plan_once(
    session: &SessionContext,
    query: &Query,
    configuration: &str,
    options: &SparkSqlOptions,
    trace: bool,
) -> TestResult<(
    Arc<dyn ExecutionPlan>,
    u64,
    u64,
    Option<Value>,
    Vec<(String, u64)>,
)> {
    // Match #1379: options and registration precede both phase timers.
    reset_events();
    let started = Instant::now();
    let (physical, logical_ns, physical_ns, logical) = if configuration.starts_with("spark-") {
        let query = plan_query(session, &query.sql, options).await?;
        let logical_ns = started.elapsed().as_nanos() as u64;
        let logical = trace
            .then(|| logical_snapshot(&query.logical_plan, Some(&query.column_names)))
            .transpose()?;
        let second = Instant::now();
        let physical = query.create_physical_plan(session).await?;
        (
            physical,
            logical_ns,
            second.elapsed().as_nanos() as u64,
            logical,
        )
    } else {
        let frame = session.sql(&query.sql).await?;
        let logical_ns = started.elapsed().as_nanos() as u64;
        let logical = trace
            .then(|| logical_snapshot(frame.logical_plan(), None))
            .transpose()?;
        let second = Instant::now();
        let physical = frame.create_physical_plan().await?;
        (
            physical,
            logical_ns,
            second.elapsed().as_nanos() as u64,
            logical,
        )
    };
    let events = take_events()?;
    scan_evidence(physical.as_ref())?;
    Ok((physical, logical_ns, physical_ns, logical, events))
}

/// Run only planning, or validate the schema comparator without a session.
pub async fn run() -> TestResult {
    let args = std::env::args().skip(1).collect::<Vec<_>>();
    if args == ["schema-self-check"] {
        schema_self_check()?;
        println!("exact Arrow schema self-check passed");
        return Ok(());
    }
    if args.len() != 5
        || !["trace", "measure"].contains(&args[0].as_str())
        || !CONFIGURATIONS.contains(&args[2].as_str())
    {
        return Err("usage: planning_attribution trace/measure DELTA_ROOT CONFIGURATION PROCESS REFERENCE_OR_DASH".into());
    }
    let trace = args[0] == "trace";
    schema_self_check()?;
    if !trace && args[4] != "-" {
        return Err("measure does not accept a trace reference".into());
    }
    let configuration = &args[2];
    let manifest: Manifest = serde_json::from_str(include_str!("../planning/queries.json"))?;
    let eligibility = include_str!("../planning/eligibility.jsonl")
        .lines()
        .map(serde_json::from_str::<Value>)
        .collect::<Result<Vec<_>, _>>()?;
    let sessions = build_registered_sessions(&manifest, configuration, Path::new(&args[1])).await?;
    let mut reference = if trace && args[4] != "-" {
        fs::read_to_string(&args[4])?
            .lines()
            .map(|line| {
                let row: Value = serde_json::from_str(line)?;
                Ok((
                    row["id"].as_str().ok_or("trace ID missing")?.to_string(),
                    row,
                ))
            })
            .collect::<TestResult<BTreeMap<String, Value>>>()?
    } else {
        BTreeMap::new()
    };
    let checking = trace && args[4] != "-";
    let mut count = 0;
    for query in &manifest.queries {
        if query.group == "sweep" {
            if !trace || !applies(query, configuration, false) {
                continue;
            }
            let record = eligibility
                .iter()
                .find(|row| row["id"] == query.id && row["configuration"] == *configuration)
                .ok_or("missing frozen eligibility")?;
            if record["status"] == "excluded" {
                continue;
            }
            if record["status"] != "included" {
                return Err("invalid frozen eligibility".into());
            }
        } else if !["paired", "stress"].contains(&query.group.as_str()) {
            continue;
        }
        let session = &sessions[&query.corpus];
        let options = options(configuration);
        if trace {
            let (physical, _, _, logical, _) =
                plan_once(session, query, configuration, &options, true)
                    .await
                    .map_err(|error| format!("{} {configuration}: {error}", query.id))?;
            let row = json!({"kind":"trace", "id":query.id, "configuration":configuration,
                "logical":logical, "physical_plan":displayable(physical.as_ref()).indent(true).to_string(),
                "physical":physical_snapshot(physical.as_ref())?, "delta_io":scan_evidence(physical.as_ref())?});
            if checking {
                let expected = reference
                    .remove(&query.id)
                    .ok_or("missing canonical trace")?;
                compare(&expected, &row, &format!("{configuration}/{}", query.id))?;
            }
            println!("{row}");
        } else {
            let (mut logical, mut physical, mut total, mut events) =
                (Vec::new(), Vec::new(), Vec::new(), Vec::new());
            for sample in 0..WARMUPS + SAMPLES {
                let (plan, logical_ns, physical_ns, _, spans) =
                    plan_once(session, query, configuration, &options, false)
                        .await
                        .map_err(|error| format!("{} {configuration}: {error}", query.id))?;
                std::hint::black_box(plan);
                if sample >= WARMUPS {
                    logical.push(logical_ns);
                    physical.push(physical_ns);
                    total.push(logical_ns + physical_ns);
                    events.push(spans);
                }
            }
            println!(
                "{}",
                json!({"kind":"samples", "id":query.id, "group":query.group,
                "configuration":configuration, "process":args[3], "logical_ns":logical,
                "physical_ns":physical, "total_ns":total, "events":events})
            );
        }
        count += 1;
    }
    let expected = if trace && configuration.starts_with("spark-") {
        309
    } else {
        30
    };
    if count != expected || !reference.is_empty() {
        return Err(format!(
            "incomplete inventory: {count}/{expected}, {} unmatched references",
            reference.len()
        )
        .into());
    }
    Ok(())
}

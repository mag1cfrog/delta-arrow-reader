//! Constant grouping declarations and optimized Fields have independent Spark references.

use std::collections::BTreeSet;

use datafusion::execution::SessionStateBuilder;
use datafusion::prelude::{SessionConfig, SessionContext};
use delta_arrow_reader_spark_sql::configure_spark_sql;
use serde_json::{Value, json};

#[path = "support/spark_inputs.rs"]
mod spark_inputs;
#[path = "support/spark_results.rs"]
mod spark_results;
use spark_results::{
    TestResult, capture_native_query_observation, compare_exact_spark_query_stages,
    compare_exact_spark_query_stages_with_optimized,
};

fn session(batch_size: usize, spark: bool) -> SessionContext {
    let state = SessionStateBuilder::new()
        .with_default_features()
        .with_config(
            SessionConfig::new()
                .with_batch_size(batch_size)
                .with_target_partitions(2),
        );
    SessionContext::new_with_state(if spark {
        configure_spark_sql(state).build()
    } else {
        state.build()
    })
}

#[test]
fn literal_projection_aliases_preserve_types_metadata_and_native_plans() -> TestResult {
    use std::collections::HashMap;
    use std::sync::Arc;

    use arrow::array::{Float32Array, ListArray};
    use arrow::buffer::OffsetBuffer;
    use arrow::datatypes::{DataType, Field};
    use datafusion::common::{ScalarValue, config::ConfigOptions, metadata::FieldMetadata};
    use datafusion::logical_expr::{Expr, LogicalPlan, LogicalPlanBuilder, col, lit};
    use datafusion::optimizer::optimizer::{OptimizerContext, OptimizerRule};
    use datafusion::optimizer::simplify_expressions::SimplifyExpressions;
    use sail_plan::expression_errors::SparkSqlExpressionSemantics;

    let metadata = HashMap::from([("source".to_owned(), "literal-alias".to_owned())]);
    let nested = ScalarValue::List(Arc::new(ListArray::new(
        Arc::new(
            Field::new("kept_element", DataType::Float32, true).with_metadata(metadata.clone()),
        ),
        OffsetBuffer::from_lengths([1]),
        Arc::new(Float32Array::from(vec![-0.0])),
        None,
    )));
    for value in [
        ScalarValue::Float32(Some(-0.0)),
        ScalarValue::Float32(None),
        nested,
    ] {
        let producer = LogicalPlanBuilder::empty(false)
            .project(vec![lit(value.clone()).alias("value")])?
            .alias("first")?
            .project(vec![col("first.value").alias_with_metadata(
                "key",
                Some(FieldMetadata::from(metadata.clone())),
            )])?
            .alias("second")?
            .build()?;
        let plan = LogicalPlanBuilder::from(producer)
            .project(vec![col("second.key").alias("result")])?
            .build()?;
        for spark in [false, true] {
            let mut options = ConfigOptions::default();
            if spark {
                options
                    .extensions
                    .insert(SparkSqlExpressionSemantics::default());
            }
            let config = OptimizerContext::new_with_config_options(Arc::new(options));
            let actual = SimplifyExpressions::new()
                .rewrite(plan.clone(), &config)?
                .data;
            assert_eq!(actual.schema().fields(), plan.schema().fields());
            assert_eq!(actual.schema().field(0).metadata(), &metadata);
            if !spark {
                assert_eq!(actual, plan);
                continue;
            }
            let LogicalPlan::Projection(projection) = actual else {
                return Err("expected literal consumer projection".into());
            };
            let Expr::Literal(actual, _) = projection.expr[0].clone().unalias() else {
                return Err("qualified literal binding was not propagated".into());
            };
            assert_eq!(actual.data_type(), value.data_type());
            assert_eq!(actual, value);
            if let ScalarValue::Float32(Some(value)) = actual {
                assert_eq!(value.to_bits(), (-0.0_f32).to_bits());
            }
        }
    }
    Ok(())
}

#[test]
fn literal_lookup_leaves_computed_and_aggregate_producers_opaque() -> TestResult {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use arrow::datatypes::DataType;
    use datafusion::common::{ScalarValue, config::ConfigOptions};
    use datafusion::logical_expr::{
        ColumnarValue, LogicalPlanBuilder, Volatility, col, create_udf, lit,
    };
    use datafusion::optimizer::optimizer::{OptimizerContext, OptimizerRule};
    use datafusion::optimizer::simplify_expressions::SimplifyExpressions;
    use sail_plan::expression_errors::SparkSqlExpressionSemantics;

    let mut options = ConfigOptions::default();
    options
        .extensions
        .insert(SparkSqlExpressionSemantics::default());
    let config = OptimizerContext::new_with_config_options(Arc::new(options));
    let callbacks = Arc::new(AtomicUsize::new(0));
    let mut producers = Vec::new();
    for volatility in [Volatility::Immutable, Volatility::Volatile] {
        let observed = Arc::clone(&callbacks);
        let function = create_udf(
            "literal_source",
            vec![],
            DataType::Int32,
            volatility,
            Arc::new(move |_| {
                observed.fetch_add(1, Ordering::SeqCst);
                Ok(ColumnarValue::Scalar(ScalarValue::Int32(Some(7))))
            }),
        );
        producers.push(
            LogicalPlanBuilder::empty(false)
                .project(vec![function.call(vec![]).alias("key")])?
                .build()?,
        );
    }
    producers.push(
        LogicalPlanBuilder::empty(false)
            .project(vec![(lit(1_i32) + lit(2_i32)).alias("key")])?
            .build()?,
    );
    producers.push(
        LogicalPlanBuilder::empty(false)
            .project(vec![lit(7_i32).alias("key")])?
            .aggregate(vec![col("key")], Vec::new())?
            .build()?,
    );
    for producer in producers {
        let plan = LogicalPlanBuilder::from(producer)
            .alias("producer")?
            .project(vec![col("producer.key").alias("result")])?
            .build()?;
        let actual = SimplifyExpressions::new()
            .rewrite(plan.clone(), &config)?
            .data;
        assert_eq!(actual, plan);
    }
    let left = LogicalPlanBuilder::empty(false)
        .project(vec![lit(7_i32).alias("key")])?
        .alias("left")?
        .build()?;
    let right = LogicalPlanBuilder::empty(false)
        .project(vec![lit(9_i32).alias("key")])?
        .alias("right")?
        .build()?;
    let joined = LogicalPlanBuilder::from(left)
        .cross_join(right)?
        .project(vec![
            col("left.key").alias("left_key"),
            col("right.key").alias("right_key"),
        ])?
        .build()?;
    let actual = SimplifyExpressions::new()
        .rewrite(joined.clone(), &config)?
        .data;
    assert_eq!(actual, joined);
    assert_eq!(callbacks.load(Ordering::SeqCst), 0);
    Ok(())
}

#[tokio::test]
async fn original_constant_groups_keep_public_fields_and_signed_zero_representatives() -> TestResult
{
    let records = include_str!("fixtures/constant-group-fields-original-spark.jsonl")
        .lines()
        .map(serde_json::from_str::<Value>)
        .collect::<Result<Vec<_>, _>>()?;
    assert_eq!(records.len() - 1, 8);
    let mut differences = Vec::new();
    for batch_size in [1, 32] {
        let session = session(batch_size, true);
        spark_inputs::register_recorded_inputs(&session, &records[0], batch_size)?;
        for original in &records[1..] {
            // The original capture recorded public Fields only, without physical stages.
            let mut case = original.clone();
            case["analyzed_schema"] = original["expected"]["schema"].clone();
            let actual = compare_exact_spark_query_stages(&session, &case).await?;
            eprintln!(
                "CONSTANT_GROUP_ORIGINAL {}",
                json!({"id":case["id"],"ansi":case["ansi"],"batch":batch_size,
                    "observation":actual})
            );
            if actual["matches"] != true {
                differences.push(format!(
                    "{} ANSI={} batch={batch_size}: {actual}",
                    case["id"], case["ansi"]
                ));
            }
        }
    }
    assert!(differences.is_empty(), "{}", differences.join("\n"));
    Ok(())
}

#[tokio::test]
async fn constant_group_fields_match_each_independent_spark_stage() -> TestResult {
    let records = include_str!("fixtures/constant-group-fields-spark.jsonl")
        .lines()
        .map(serde_json::from_str::<Value>)
        .collect::<Result<Vec<_>, _>>()?;
    let manifest: Value =
        serde_json::from_str(include_str!("fixtures/constant-group-fields-cases.json"))?;
    assert_eq!(records[0]["spark_version"], "4.2.0");
    assert_eq!(
        records[0]["spark_revision"],
        "32f7299601108917fb01920a54e084595b7b3bf8"
    );
    assert_eq!(records[0]["timezone"], "UTC");
    assert_eq!(records[0]["inputs"], manifest["inputs"]);
    assert_eq!(records.len() - 1, 196);
    for line in include_str!("fixtures/constant-group-fields-original-spark.jsonl")
        .lines()
        .skip(1)
    {
        let original: Value = serde_json::from_str(line)?;
        let captured = records[1..]
            .iter()
            .find(|case| case["sql"] == original["sql"] && case["ansi"] == original["ansi"])
            .ok_or("missing original query in stage capture")?;
        assert_eq!(captured["expected"], original["expected"]);
    }
    let mut identities = BTreeSet::new();
    for case in &records[1..] {
        assert!(identities.insert((
            case["id"].as_str().ok_or("missing ID")?,
            case["ansi"].as_bool().ok_or("missing ANSI mode")?,
        )));
        let definition = manifest["cases"]
            .as_array()
            .ok_or("missing query definitions")?
            .iter()
            .find(|definition| definition["id"] == case["id"])
            .ok_or("unrecorded query identity")?;
        assert_eq!(case["sql"], definition["sql"]);
        assert_eq!(case["category"], definition["category"]);
        if case["expected"].get("rows").is_some() {
            for stage in [
                "analyzed",
                "optimized",
                "executed_before_collect",
                "executed",
            ] {
                assert!(case["schema_stages"].get(stage).is_some(), "{case}");
            }
            assert_eq!(case["public_schema"], case["expected"]["schema"]);
        } else {
            assert_eq!(
                case["expected"]["condition"], "CAST_INVALID_INPUT",
                "{case}"
            );
            assert!(
                case["spark_error"]
                    .as_str()
                    .is_some_and(|error| error.contains("bad"))
            );
            assert!(case["spark_error_phase"].is_string());
        }
    }
    let mut differences = Vec::new();
    for batch_size in [1, 32] {
        let session = session(batch_size, true);
        spark_inputs::register_recorded_inputs(&session, &records[0], batch_size)?;
        for case in &records[1..] {
            let actual = compare_exact_spark_query_stages_with_optimized(&session, case).await?;
            let public_matches = case["expected"]
                .get("schema")
                .is_none_or(|expected| actual["public_schema"] == *expected);
            let cause_matches = if case["expected"]["condition"] == "CAST_INVALID_INPUT" {
                let target = if case["id"]
                    .as_str()
                    .ok_or("missing ID")?
                    .starts_with("float_")
                {
                    "Float32"
                } else {
                    "Float64"
                };
                actual["error"].as_str().is_some_and(|error| {
                    error.contains(&format!(
                        "[CAST_INVALID_INPUT] Cannot cast string \"bad\" to {target}"
                    ))
                })
            } else {
                true
            };
            let raw_payload_matches = if case["category"] == "payload" {
                let mut rows = case["rows_raw"]
                    .as_array()
                    .ok_or("missing raw payload rows")?
                    .clone();
                rows.sort_by_key(Value::to_string);
                actual["rows"] == json!(rows)
            } else {
                true
            };
            eprintln!(
                "CONSTANT_GROUP_STAGE {}",
                json!({"id":case["id"],"ansi":case["ansi"],"batch":batch_size,
                    "observation":actual,"public_result_matches":public_matches,
                    "cast_cause_matches":cause_matches,"raw_payload_matches":raw_payload_matches})
            );
            if actual["matches"] != true
                || actual["optimized_matches"] != true
                || !public_matches
                || !cause_matches
                || !raw_payload_matches
            {
                differences.push(format!(
                    "{} ANSI={} batch={batch_size}: public_result={public_matches} \
                     cast_cause={cause_matches} raw_payload={raw_payload_matches} {actual}",
                    case["id"], case["ansi"]
                ));
            }
        }
    }
    assert!(differences.is_empty(), "{}", differences.join("\n"));
    Ok(())
}

#[tokio::test]
async fn literal_array_consumers_match_independent_boundary_stages() -> TestResult {
    let records = include_str!("fixtures/constant-group-literal-consumers-spark.jsonl")
        .lines()
        .map(serde_json::from_str::<Value>)
        .collect::<Result<Vec<_>, _>>()?;
    let manifest: Value = serde_json::from_str(include_str!(
        "fixtures/constant-group-literal-consumers-cases.json"
    ))?;
    assert_eq!(records.len() - 1, 24);
    assert_eq!(records[0]["inputs"], manifest["inputs"]);
    assert_eq!(
        records[0]["spark_revision"],
        "32f7299601108917fb01920a54e084595b7b3bf8"
    );
    let mut identities = BTreeSet::new();
    for case in &records[1..] {
        assert!(identities.insert((
            case["id"].as_str().ok_or("missing ID")?,
            case["ansi"].as_bool().ok_or("missing ANSI mode")?
        )));
        let definition = manifest["cases"]
            .as_array()
            .ok_or("missing definitions")?
            .iter()
            .find(|definition| definition["id"] == case["id"])
            .ok_or("missing boundary query")?;
        assert_eq!(case["sql"], definition["sql"]);
        assert_eq!(case["category"], definition["category"]);
        if case["expected"].get("rows").is_none() {
            assert_eq!(case["expected"]["condition"], "INVALID_ARRAY_INDEX");
        } else {
            assert_eq!(case["public_schema"], case["expected"]["schema"]);
            for stage in [
                "analyzed",
                "optimized",
                "executed_before_collect",
                "executed",
            ] {
                assert!(case["schema_stages"].get(stage).is_some(), "{case}");
            }
        }
    }
    let mut differences = Vec::new();
    for batch_size in [1, 32] {
        let session = session(batch_size, true);
        spark_inputs::register_recorded_inputs(&session, &records[0], batch_size)?;
        for case in &records[1..] {
            let actual = compare_exact_spark_query_stages_with_optimized(&session, case).await?;
            let cause_matches = if case["expected"]["condition"] == "INVALID_ARRAY_INDEX" {
                let parameters = &case["spark_error_details"]["message_parameters"];
                let index = parameters["indexValue"]
                    .as_str()
                    .ok_or("missing failing index")?;
                let length = parameters["arraySize"]
                    .as_str()
                    .ok_or("missing array size")?;
                actual["error"].as_str().is_some_and(|error| error.contains(&format!(
                    "[INVALID_ARRAY_INDEX] The index {index} is out of bounds. The array has {length} elements."
                )))
            } else {
                true
            };
            let public_matches = case["expected"]
                .get("schema")
                .is_none_or(|expected| actual["public_schema"] == *expected);
            eprintln!(
                "CONSTANT_LITERAL_BOUNDARY {}",
                json!({
                    "id":case["id"],"ansi":case["ansi"],"batch":batch_size,
                    "observation":actual,"cause_matches":cause_matches,
                    "public_result_matches":public_matches,
                })
            );
            if actual["matches"] != true
                || actual["optimized_matches"] != true
                || !cause_matches
                || !public_matches
            {
                differences.push(format!("{} ANSI={} batch={batch_size}: cause={cause_matches} public={public_matches} {actual}",
                    case["id"],case["ansi"]));
            }
        }
    }
    assert!(differences.is_empty(), "{}", differences.join("\n"));
    Ok(())
}

#[tokio::test]
async fn constant_group_planning_preserves_native_fields_values_and_cast_causes() -> TestResult {
    let metadata: Value =
        serde_json::from_str(include_str!("fixtures/constant-group-fields-cases.json"))?;
    let originals = include_str!("fixtures/constant-group-fields-original-spark.jsonl")
        .lines()
        .skip(1)
        .map(serde_json::from_str::<Value>)
        .collect::<Result<Vec<_>, _>>()?;
    for batch_size in [1, 32] {
        let native = session(batch_size, false);
        let configured = session(batch_size, true);
        for session in [&native, &configured] {
            spark_inputs::register_recorded_inputs(session, &metadata, batch_size)?;
        }
        let mut controls = Vec::new();
        for kind in ["float", "double"] {
            for table in [
                format!("{kind}_scalar_mixed"),
                format!("{kind}_scalar_empty"),
            ] {
                for expression in [
                    format!("CAST('-0.0' AS {kind})"),
                    format!("ARRAY[CAST('-0.0' AS {kind})]"),
                    "CAST('7' AS INT)".into(),
                ] {
                    let grouped = format!(
                        "SELECT {expression} AS key,COUNT(*) AS n FROM {table} GROUP BY {expression}"
                    );
                    controls.push(grouped.clone());
                    controls.push(format!(
                        "SELECT key,n FROM ({grouped}) grouped WHERE key IS NOT NULL"
                    ));
                }
            }
            for field in ["k", "record", "values"] {
                controls.push(format!(
                    "SELECT {field} AS key,COUNT(*) AS n FROM {kind}_metadata GROUP BY {field}"
                ));
            }
            controls.push(format!("SELECT CAST('bad' AS {kind}) AS key,COUNT(*) AS n FROM {kind}_scalar_mixed GROUP BY CAST('bad' AS {kind})"));
        }
        let mut expected = Vec::new();
        for sql in &controls {
            let observation = capture_native_query_observation(&native, sql).await?;
            if sql.contains("'bad'") {
                assert!(
                    observation["error"]
                        .as_str()
                        .is_some_and(|error| error.contains("bad")),
                    "{sql}: {observation}"
                );
            } else {
                assert_eq!(observation["error"], Value::Null, "{sql}: {observation}");
            }
            expected.push(observation);
        }
        for after_spark in [false, true] {
            if after_spark {
                for original in &originals {
                    let mut case = original.clone();
                    case["analyzed_schema"] = case["expected"]["schema"].clone();
                    let actual = compare_exact_spark_query_stages(&configured, &case).await?;
                    assert_eq!(actual["matches"], true, "{case}: {actual}");
                }
            }
            for (sql, expected) in controls.iter().zip(&expected) {
                let actual = capture_native_query_observation(&configured, sql).await?;
                assert_eq!(
                    &actual, expected,
                    "batch={batch_size} after_spark={after_spark} {sql}"
                );
            }
        }
    }
    Ok(())
}

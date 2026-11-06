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
fn grouped_literal_consumers_preserve_types_metadata_and_native_plans() -> TestResult {
    use std::collections::HashMap;
    use std::sync::Arc;

    use arrow::array::{Float32Array, ListArray};
    use arrow::buffer::OffsetBuffer;
    use arrow::datatypes::{DataType, Field};
    use datafusion::common::{ScalarValue, config::ConfigOptions, metadata::FieldMetadata};
    use datafusion::logical_expr::{Expr, LogicalPlan, LogicalPlanBuilder, col, lit, when};
    use datafusion::optimizer::AnalyzerRule;
    use datafusion::optimizer::optimizer::{OptimizerContext, OptimizerRule};
    use datafusion::optimizer::simplify_expressions::SimplifyExpressions;
    use sail_plan::expression_errors::{PrepareSparkExpressions, SparkSqlExpressionSemantics};

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
            .aggregate(vec![lit(value.clone()).alias("value")], Vec::<Expr>::new())?
            .alias("first")?
            .project(vec![col("first.value").alias_with_metadata(
                "key",
                Some(FieldMetadata::from(metadata.clone())),
            )])?
            .alias("second")?
            .build()?;
        let plan = LogicalPlanBuilder::from(producer)
            .project(vec![
                when(lit(true), col("second.key"))
                    .otherwise(col("second.key"))?
                    .alias_with_metadata("result", Some(FieldMetadata::from(metadata.clone()))),
            ])?
            .build()?;
        for spark in [false, true] {
            let mut options = ConfigOptions::default();
            if spark {
                options
                    .extensions
                    .insert(SparkSqlExpressionSemantics::default());
            }
            let prepared = PrepareSparkExpressions.analyze(plan.clone(), &options)?;
            let config = OptimizerContext::new_with_config_options(Arc::new(options));
            let actual = SimplifyExpressions::new().rewrite(prepared, &config)?.data;
            assert_eq!(actual.schema().fields(), plan.schema().fields());
            assert_eq!(actual.schema().field(0).metadata(), &metadata);
            let LogicalPlan::Projection(projection) = actual else {
                return Err("expected literal consumer projection".into());
            };
            if !spark {
                assert_eq!(projection.expr[0].clone().unalias(), col("second.key"));
                continue;
            }
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
    use datafusion::functions_aggregate::count::count;
    use datafusion::logical_expr::{
        ColumnarValue, Expr, LogicalPlan, LogicalPlanBuilder, Volatility, col, create_udf, lit,
        when,
    };
    use datafusion::optimizer::AnalyzerRule;
    use datafusion::optimizer::optimizer::{OptimizerContext, OptimizerRule};
    use datafusion::optimizer::simplify_expressions::SimplifyExpressions;
    use sail_plan::expression_errors::{PrepareSparkExpressions, SparkSqlExpressionSemantics};

    let mut options = ConfigOptions::default();
    options
        .extensions
        .insert(SparkSqlExpressionSemantics::default());
    let config = OptimizerContext::new_with_config_options(Arc::new(options.clone()));
    let callbacks = Arc::new(AtomicUsize::new(0));
    let mut producers = Vec::new();
    let mut opaque_group_outputs = Vec::new();
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
        opaque_group_outputs.push(
            LogicalPlanBuilder::empty(false)
                .aggregate(vec![function.call(vec![]).alias("key")], Vec::<Expr>::new())?
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
            .aggregate(
                vec![col("key")],
                Vec::<datafusion::logical_expr::Expr>::new(),
            )?
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
    for groups in [Vec::new(), vec![lit(1_i64).alias("dimension")]] {
        opaque_group_outputs.push(
            LogicalPlanBuilder::empty(false)
                .aggregate(groups, vec![count(lit(1_i32)).alias("key")])?
                .build()?,
        );
    }
    opaque_group_outputs.push(
        LogicalPlanBuilder::empty(false)
            .aggregate(
                vec![Expr::GroupingSet(
                    datafusion::logical_expr::expr::GroupingSet::GroupingSets(vec![
                        vec![lit(1_i64).alias("key")],
                        vec![],
                    ]),
                )],
                Vec::<Expr>::new(),
            )?
            .build()?,
    );
    for producer in opaque_group_outputs {
        let plan = LogicalPlanBuilder::from(producer)
            .alias("producer")?
            .project(vec![
                when(lit(true), col("producer.key"))
                    .otherwise(col("producer.key"))?
                    .alias("result"),
            ])?
            .build()?;
        let prepared = PrepareSparkExpressions.analyze(plan, &options)?;
        let actual = SimplifyExpressions::new().rewrite(prepared, &config)?.data;
        let LogicalPlan::Projection(projection) = actual else {
            return Err("expected opaque grouping consumer projection".into());
        };
        assert_eq!(projection.expr[0].clone().unalias(), col("producer.key"));
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
async fn literal_group_consumers_keep_opaque_projection_boundaries() -> TestResult {
    let records = include_str!("fixtures/constant-group-provenance-spark.jsonl")
        .lines()
        .map(serde_json::from_str::<Value>)
        .collect::<Result<Vec<_>, _>>()?;
    assert_eq!(records.len() - 1, 16);
    assert_eq!(records[0]["version"], "4.2.0");
    assert_eq!(
        records[0]["revision"],
        "32f7299601108917fb01920a54e084595b7b3bf8"
    );
    let mut checked = BTreeSet::new();
    for batch_size in [1, 32] {
        let session = session(batch_size, true);
        for reference in records[1..].iter().filter(|case| {
            matches!(
                case["id"].as_str(),
                Some(
                    "map_plain"
                        | "map_group_scalar"
                        | "map_group_equal_array"
                        | "map_group_key_after"
                        | "literal_group_positive"
                )
            )
        }) {
            let mut case = reference.clone();
            case["expected"] = json!({
                "schema":reference["public_schema"],"rows":reference["rows"],
            });
            let actual = compare_exact_spark_query_stages_with_optimized(&session, &case).await?;
            assert!(checked.insert((
                case["id"].as_str().ok_or("missing ID")?.to_owned(),
                case["ansi"].as_bool().ok_or("missing ANSI")?,
                batch_size,
            )));
            eprintln!(
                "CONSTANT_GROUP_PROVENANCE {}",
                json!({"id":case["id"],"ansi":case["ansi"],"batch":batch_size,
                    "observation":actual})
            );
            assert_eq!(actual["matches"], true, "{case}: {actual}");
            assert_eq!(actual["optimized_matches"], true, "{case}: {actual}");
        }
    }
    assert_eq!(checked.len(), 20);
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
async fn unmasked_constant_array_grouping_sets_match_each_spark_stage() -> TestResult {
    let records = include_str!("fixtures/constant-group-unmasked-sets-spark.jsonl")
        .lines()
        .map(serde_json::from_str::<Value>)
        .collect::<Result<Vec<_>, _>>()?;
    assert_eq!(records.len() - 1, 4);
    assert_eq!(records[0]["spark_version"], "4.2.0");
    assert_eq!(
        records[0]["spark_revision"],
        "32f7299601108917fb01920a54e084595b7b3bf8"
    );
    assert_eq!(records[0]["timezone"], "UTC");
    // The unchanged header describes the source capture containing these four records.
    assert_eq!(records[0]["case_count"], 23);
    assert_eq!(records[0]["observations"], 46);
    let mut identities = BTreeSet::new();
    for case in &records[1..] {
        assert!(identities.insert((
            case["id"].as_str().ok_or("missing ID")?,
            case["ansi"].as_bool().ok_or("missing ANSI mode")?,
        )));
        assert_eq!(case["category"], "unmasked_constant");
        assert_eq!(case["public_schema"], case["expected"]["schema"]);
        assert_eq!(case["public_schema"]["fields"][0]["nullable"], false);
        assert_eq!(
            case["public_schema"]["fields"][0]["type"]["containsNull"],
            true
        );
        for stage in [
            "analyzed",
            "optimized",
            "executed_before_collect",
            "executed",
        ] {
            assert!(case["schema_stages"].get(stage).is_some(), "{case}");
        }
        assert!(case["rows_raw"].is_array());
    }
    assert_eq!(
        identities,
        BTreeSet::from([
            ("float_unmasked_array_sets", true),
            ("float_unmasked_array_sets", false),
            ("double_unmasked_array_sets", true),
            ("double_unmasked_array_sets", false),
        ])
    );
    let mut differences = Vec::new();
    for batch_size in [1, 32] {
        let session = session(batch_size, true);
        spark_inputs::register_recorded_inputs(&session, &records[0], batch_size)?;
        for case in &records[1..] {
            let actual = compare_exact_spark_query_stages_with_optimized(&session, case).await?;
            let public_matches = actual["public_schema"] == case["public_schema"];
            let zero = if case["id"] == "float_unmasked_array_sets" {
                "00000000"
            } else {
                "0000000000000000"
            };
            let key_payload_matches = actual["rows_raw"]
                .as_array()
                .is_some_and(|rows| rows.iter().all(|row| row[0] == json!([zero])));
            eprintln!(
                "CONSTANT_GROUP_UNMASKED {}",
                json!({"id":case["id"],"ansi":case["ansi"],"batch":batch_size,
                    "observation":actual,"public_result_matches":public_matches,
                    "key_payload_matches":key_payload_matches})
            );
            if actual["matches"] != true
                || actual["optimized_matches"] != true
                || !public_matches
                || !key_payload_matches
            {
                differences.push(format!(
                    "{} ANSI={} batch={batch_size}: public_result={public_matches} \
                     key_payload={key_payload_matches} {actual}",
                    case["id"], case["ansi"]
                ));
            }
        }
    }
    assert!(differences.is_empty(), "{}", differences.join("\n"));
    Ok(())
}

#[tokio::test]
async fn materialized_grouping_keys_preserve_fields_through_having_and_hidden_sort() -> TestResult {
    use std::collections::HashMap;
    use std::sync::Arc;

    use arrow::array::{Float32Array, Int32Array, ListArray};
    use arrow::buffer::OffsetBuffer;
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use datafusion::datasource::MemTable;
    use datafusion::physical_plan::collect;
    use delta_arrow_reader_spark_sql::{SparkSqlOptions, plan_query};
    use sail_plan::expression_errors::SparkSqlExpressionSemantics;

    let element = Arc::new(
        Field::new("kept_element", DataType::Float32, true)
            .with_metadata(HashMap::from([("unit".into(), "signed-zero".into())])),
    );
    let key = Field::new("key", DataType::List(Arc::clone(&element)), false)
        .with_metadata(HashMap::from([("owner".into(), "materialized-key".into())]));
    let nullable_key = Field::new("k", DataType::Float32, true)
        .with_metadata(HashMap::from([("owner".into(), "nullable-key".into())]));
    let id = Field::new("id", DataType::Int32, false);
    let schema = Arc::new(Schema::new(vec![
        key.clone(),
        nullable_key.clone(),
        id.clone(),
    ]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(ListArray::new(
                element,
                OffsetBuffer::from_lengths([1, 1]),
                Arc::new(Float32Array::from(vec![-0.0, -0.0])),
                None,
            )),
            Arc::new(Float32Array::from(vec![Some(-0.0), None])),
            Arc::new(Int32Array::from(vec![0, 1])),
        ],
    )?;
    let session = session(1, true);
    for (table, input) in [("keys", batch.clone()), ("empty_keys", batch.slice(0, 0))] {
        session.register_table(
            table,
            Arc::new(MemTable::try_new(Arc::clone(&schema), vec![vec![input]])?),
        )?;
        for (select, suffix, visible_key) in [
            ("key,k,id", "", true),
            (
                "key,k,id",
                " HAVING key IS NOT NULL ORDER BY key,k,id",
                true,
            ),
            ("k,id", " HAVING key IS NOT NULL ORDER BY key,k,id", false),
        ] {
            let sql = format!(
                "SELECT {select},GROUPING(id) AS missing,COUNT(*) AS n FROM {table} \
                 GROUP BY GROUPING SETS ((key,k,id),(key,k)){suffix}"
            );
            let query = plan_query(&session, &sql, &SparkSqlOptions::default()).await?;
            let mut expected_fields = vec![
                nullable_key.clone(),
                id.clone().with_nullable(true),
                Field::new("missing", DataType::Int8, false),
                Field::new("n", DataType::Int64, false),
            ];
            if visible_key {
                expected_fields.insert(0, key.clone());
            }
            let mut state = session.state();
            state
                .config_mut()
                .options_mut()
                .extensions
                .insert(SparkSqlExpressionSemantics::default());
            let optimized = state.optimize(&query.logical_plan)?;
            for fields in [
                query.logical_plan.schema().fields(),
                optimized.schema().fields(),
            ] {
                let named = fields
                    .iter()
                    .zip(&query.column_names)
                    .map(|(field, name)| field.as_ref().clone().with_name(name))
                    .collect::<Vec<_>>();
                assert_eq!(named, expected_fields, "{sql}");
            }
            let physical = query.create_physical_plan(&session).await?;
            assert_eq!(
                physical.schema().as_ref(),
                &Schema::new(expected_fields.clone()),
                "{sql}"
            );
            let mut actual = Vec::new();
            for batch in collect(physical.clone(), session.task_ctx()).await? {
                spark_results::validate_batch(physical.schema().as_ref(), &batch)?;
                for row in 0..batch.num_rows() {
                    actual.push(
                        batch
                            .columns()
                            .iter()
                            .map(|column| spark_results::encode_value(column.as_ref(), row))
                            .collect::<TestResult<Vec<_>>>()?,
                    );
                }
            }
            let mut expected = if table == "empty_keys" {
                Vec::new()
            } else {
                vec![
                    vec![json!("00000000"), json!("0"), json!("0"), json!("1")],
                    vec![json!("00000000"), Value::Null, json!("1"), json!("1")],
                    vec![Value::Null, json!("1"), json!("0"), json!("1")],
                    vec![Value::Null, Value::Null, json!("1"), json!("1")],
                ]
            };
            if visible_key {
                for row in &mut expected {
                    row.insert(0, json!(["00000000"]));
                }
            }
            actual.sort_by_key(|row| json!(row).to_string());
            expected.sort_by_key(|row| json!(row).to_string());
            assert_eq!(actual, expected, "{sql}");
        }
    }
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

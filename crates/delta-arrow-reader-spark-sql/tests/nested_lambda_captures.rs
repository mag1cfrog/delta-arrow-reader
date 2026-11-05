//! Nested parameters must retain their lexical slots when captures are projected.

use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, Int32Array, Int32Builder, ListArray, ListBuilder, MapBuilder, StringBuilder,
};
use arrow::datatypes::{DataType, Field, Int32Type, Schema};
use arrow::record_batch::RecordBatch;
use datafusion::common::{DFSchema, TableReference};
use datafusion::datasource::MemTable;
use datafusion::execution::SessionStateBuilder;
use datafusion::functions_nested::array_transform::array_transform;
use datafusion::logical_expr::execution_props::ExecutionProps;
use datafusion::logical_expr::{Expr, JoinType, col, lambda, lambda_var, lit, when};
use datafusion::physical_expr::expressions::LambdaVariable;
use datafusion::physical_expr::{PhysicalExpr, create_physical_expr};
use datafusion::physical_plan::collect;
use datafusion::prelude::{SessionConfig, SessionContext};
use delta_arrow_reader_spark_sql::{SparkSqlOptions, configure_spark_sql, plan_query};
use serde_json::{Value, json};

#[path = "support/spark_inputs.rs"]
mod spark_inputs;
#[path = "support/spark_results.rs"]
#[expect(
    dead_code,
    reason = "This regression compares complete observations directly."
)]
mod spark_results;
#[path = "support/string_binary.rs"]
#[expect(
    dead_code,
    reason = "The connected consumer uses the existing raw schema observer."
)]
mod string_binary;
use spark_results::{
    TestResult, capture_schema, compare_exact_spark_query_stages_with_optimized, encode_value,
};

fn integer(value: &Value) -> TestResult<Option<i32>> {
    Ok(if value.is_null() {
        None
    } else {
        Some(value.as_str().ok_or("missing integer")?.parse()?)
    })
}

fn map_column(rows: &[Value], index: usize, nullable: bool) -> TestResult<ArrayRef> {
    let maps = MapBuilder::new(None, StringBuilder::new(), Int32Builder::new())
        .with_values_field(Field::new("values", DataType::Int32, nullable));
    let mut lists = ListBuilder::new(maps);
    for row in rows {
        let value = &row[index];
        if let Some(maps) = value.as_array() {
            for map in maps {
                if let Some(entries) = map.as_array() {
                    for entry in entries {
                        lists
                            .values()
                            .keys()
                            .append_value(entry[0].as_str().ok_or("missing map key")?);
                        lists.values().values().append_option(integer(&entry[1])?);
                    }
                }
                lists.values().append(!map.is_null())?;
            }
        }
        lists.append(!value.is_null());
    }
    let map_type = lists.values().finish_cloned().data_type().clone();
    Ok(Arc::new(
        lists
            .with_field(Field::new("item", map_type, nullable))
            .finish(),
    ))
}

fn register_inputs(session: &SessionContext, metadata: &Value, batch_size: usize) -> TestResult {
    for input in metadata["inputs"].as_array().ok_or("missing inputs")? {
        let rows = input["rows"].as_array().ok_or("missing rows")?;
        let numbers = rows
            .iter()
            .map(|row| {
                row[2]
                    .as_array()
                    .map(|values| values.iter().map(integer).collect::<TestResult<Vec<_>>>())
                    .transpose()
            })
            .collect::<TestResult<Vec<_>>>()?;
        let columns: Vec<ArrayRef> = vec![
            Arc::new(Int32Array::from(
                rows.iter()
                    .map(|row| integer(&row[0]))
                    .collect::<TestResult<Vec<_>>>()?,
            )),
            Arc::new(Int32Array::from(
                rows.iter()
                    .map(|row| integer(&row[1]))
                    .collect::<TestResult<Vec<_>>>()?,
            )),
            Arc::new(ListArray::from_iter_primitive::<Int32Type, _, _>(numbers)),
            map_column(rows, 3, true)?,
            map_column(rows, 4, false)?,
        ];
        let fields = input["schema"]["fields"]
            .as_array()
            .ok_or("missing fields")?;
        assert_eq!(fields.len(), columns.len());
        let schema = Arc::new(Schema::new(
            fields
                .iter()
                .zip(&columns)
                .map(|(field, column)| {
                    Ok(Field::new(
                        field["name"].as_str().ok_or("missing field name")?,
                        column.data_type().clone(),
                        field["nullable"].as_bool().ok_or("missing nullability")?,
                    )
                    .with_metadata(serde_json::from_value(field["metadata"].clone())?))
                })
                .collect::<TestResult<Vec<_>>>()?,
        ));
        assert_eq!(capture_schema(schema.fields(), None)?, input["schema"]);
        let batch = RecordBatch::try_new(schema.clone(), columns)?;
        let batches = (0..batch.num_rows())
            .step_by(batch_size)
            .map(|offset| batch.slice(offset, batch_size.min(batch.num_rows() - offset)))
            .collect();
        session.register_table(
            input["name"].as_str().ok_or("missing name")?,
            Arc::new(MemTable::try_new(schema, vec![batches])?),
        )?;
    }
    Ok(())
}

fn session(batch_size: usize) -> SessionContext {
    SessionContext::new_with_state(
        configure_spark_sql(
            SessionStateBuilder::new()
                .with_default_features()
                .with_config(
                    SessionConfig::new()
                        .with_batch_size(batch_size)
                        .with_target_partitions(2),
                ),
        )
        .build(),
    )
}

async fn verify_nested_lambda_query_contracts(
    reference_source: &str,
    expected_query_count: usize,
) -> TestResult {
    let reference_records = reference_source
        .lines()
        .map(serde_json::from_str::<Value>)
        .collect::<Result<Vec<_>, _>>()?;
    assert_eq!(reference_records.len() - 1, expected_query_count);
    let inherited_controls = include_str!("fixtures/nested-lambda-inherited-declarations.jsonl")
        .lines()
        .map(serde_json::from_str::<Value>)
        .collect::<Result<Vec<_>, _>>()?;
    assert_eq!(inherited_controls.len() - 1, 10);
    assert_eq!(
        inherited_controls[0]["product_parent"],
        "e1622a8999bf6bd3ef21aa5f0834f0551374f742"
    );
    let mut differences = Vec::new();
    for batch_size in [1, 32] {
        let query_session = session(batch_size);
        register_inputs(&query_session, &reference_records[0], batch_size)?;
        for case in &reference_records[1..] {
            let label = format!(
                "{} {} ANSI={} batch={batch_size}",
                case["id"], case["input"], case["ansi"]
            );
            if case["phase"] == "planning" {
                let result = plan_query(
                    &query_session,
                    case["sql"].as_str().ok_or("missing SQL")?,
                    &SparkSqlOptions {
                        ansi_mode: case["ansi"].as_bool().ok_or("missing ANSI")?,
                        session_timezone: "UTC".into(),
                        ..SparkSqlOptions::default()
                    },
                )
                .await;
                match result {
                    Ok(_) => differences.push(format!("{label}: expected planning rejection")),
                    Err(error) => {
                        let error = error.to_string();
                        let correct_cause = match case["id"].as_str() {
                            Some("null_lambda_rejection") => {
                                case["expected"]["condition"] == "INVALID_EXTRACT_BASE_FIELD_TYPE"
                                    && error.contains("INVALID_EXTRACT_BASE_FIELD_TYPE")
                                    && error.contains("missing")
                                    && error.contains("Null")
                            }
                            Some("invalid_inner_type") => {
                                case["expected"]["condition"] == "INVALID_EXTRACT_BASE_FIELD_TYPE"
                                    && error.contains("INVALID_EXTRACT_BASE_FIELD_TYPE")
                                    && error.contains("key")
                                    && error.contains("Int32")
                            }
                            Some("invalid_free_variable") => {
                                case["expected"]["condition"] == "UNRESOLVED_COLUMN.WITH_SUGGESTION"
                                    && error.contains("Identifier(\"missing\")")
                                    && error.contains("missing from the schema")
                            }
                            Some("invalid_qualified_variable") => {
                                case["expected"]["condition"] == "UNRESOLVED_COLUMN.WITH_SUGGESTION"
                                    && error.contains("Identifier(\"q\"), Identifier(\"x\")")
                                    && error.contains("missing from the schema")
                            }
                            _ => false,
                        };
                        if !correct_cause {
                            differences.push(format!("{label}: wrong planning cause: {error}"));
                        }
                    }
                }
            } else {
                assert_eq!(case["phase"], "success");
                match compare_exact_spark_query_stages_with_optimized(&query_session, case).await {
                    Ok(observed)
                        if observed["error"].is_null()
                            && observed["public_schema"] == case["schema_stages"]["analyzed"]
                            && observed["public_schema"] == case["expected"]["schema"]
                            && observed["optimized_schema"]
                                == case["schema_stages"]["optimized"]
                            && observed["before_collect"] == case["schema_stages"]["executed"]
                            && observed["physical_schema"] == case["schema_stages"]["executed"]
                            && observed["rows_match"] == true => {}
                    Ok(mut observed) => {
                        observed["batch"] = json!(batch_size);
                        let inherited = inherited_controls[1..].iter().find(|control| {
                            control["id"] == case["id"]
                                && control["input"] == case["input"]
                                && control["ansi"] == case["ansi"]
                                && control["batch_size"] == batch_size
                                && control["observation"]["batch"] == batch_size
                        });
                        match inherited {
                            Some(control)
                                if matches!(
                                    control["tracking_issue"].as_u64(),
                                    Some(822) | Some(823)
                                ) && spark_results::matches_inherited_query_observation(
                                    case, &observed, control,
                                ) =>
                            {
                                eprintln!(
                                    "Unresolved #{} declaration control {label}: {observed}",
                                    control["tracking_issue"]
                                );
                            }
                            _ => differences.push(format!("{label}: {observed}")),
                        }
                    }
                    Err(error) => differences.push(format!("{label}: {error}")),
                }
            }
        }
    }
    assert!(differences.is_empty(), "{}", differences.join("\n"));
    Ok(())
}

#[tokio::test]
async fn supplemental_nested_lambda_captures_match_spark() -> TestResult {
    verify_nested_lambda_query_contracts(
        include_str!("fixtures/nested-lambda-captures-spark.jsonl"),
        126,
    )
    .await
}

#[tokio::test]
async fn case_lambda_rebuilds_match_spark() -> TestResult {
    verify_nested_lambda_query_contracts(
        include_str!("fixtures/nested-lambda-captures-case-spark.jsonl"),
        54,
    )
    .await
}

#[tokio::test]
async fn original_nested_lambda_capture_preserves_query_contracts() -> TestResult {
    verify_nested_lambda_query_contracts(
        include_str!("fixtures/nested-lambda-captures-original-spark.jsonl"),
        92,
    )
    .await
}

#[tokio::test]
async fn nested_lambda_rewrite_consumers_match_spark() -> TestResult {
    verify_nested_lambda_query_contracts(
        include_str!("fixtures/nested-lambda-captures-interactions-spark.jsonl"),
        54,
    )
    .await
}

#[tokio::test]
async fn native_nested_lambda_capture_uses_inner_values() -> TestResult {
    for batch_size in [1, 32] {
        let session = session(batch_size);
        let frame = session
            .sql("SELECT 0 AS ignored,[1,2] AS items,3 AS k")
            .await?;
        let expression = array_transform(
            col("items"),
            lambda(
                ["x"],
                array_transform(col("items"), lambda(["x"], lambda_var("x") + col("k"))),
            ),
        )
        .resolve_lambda_variables(frame.schema())?
        .data
        .alias("result");
        let physical = frame
            .select(vec![expression])?
            .create_physical_plan()
            .await?;
        let mut rows = Vec::new();
        for batch in collect(physical.clone(), session.task_ctx()).await? {
            spark_results::validate_batch(physical.schema().as_ref(), &batch)?;
            for row in 0..batch.num_rows() {
                rows.push(encode_value(batch.column(0).as_ref(), row)?);
            }
        }
        assert_eq!(rows, vec![json!([["4", "5"], ["4", "5"]])]);
    }
    Ok(())
}

#[tokio::test]
async fn native_replaced_lambda_retains_its_parameter_boundary() -> TestResult {
    let session = session(32);
    let frame = session
        .sql("SELECT 0 AS ignored,[1,2] AS items,3 AS k")
        .await?;
    let expression = array_transform(col("items"), lambda(["x"], lambda_var("x") + col("k")))
        .resolve_lambda_variables(frame.schema())?
        .data;
    let physical = create_physical_expr(&expression, frame.schema(), &ExecutionProps::new())?;
    let DataType::List(item) = frame.schema().field(1).data_type() else {
        return Err("expected native list".into());
    };
    let replacement = datafusion::physical_expr::expressions::lambda(
        ["x"],
        Arc::new(LambdaVariable::new(
            frame.schema().fields().len(),
            Arc::new(item.as_ref().clone().with_name("x")),
        )),
    )?;
    let mut children = physical.children().into_iter().cloned().collect::<Vec<_>>();
    children[1] = replacement;
    let replaced = physical.with_new_children(children)?;
    let batches = frame.collect().await?;
    assert_eq!(batches.len(), 1);
    let values = replaced
        .evaluate(&batches[0])?
        .into_array(batches[0].num_rows())?;
    assert_eq!(encode_value(values.as_ref(), 0)?, json!(["1", "2"]));
    Ok(())
}

#[tokio::test]
async fn native_case_projection_and_filter_rewrites_preserve_scope() -> TestResult {
    use datafusion::physical_expr::expressions::Column;
    use datafusion::physical_expr::projection::{
        ProjectionExpr, update_expr_with_input_schema_size,
    };
    use datafusion::physical_expr::utils::reassign_expr_columns;
    let session = session(32);
    let frame = session.sql("SELECT 0 AS ignored,[1,2] AS items,3 AS k,99 AS trailing UNION ALL SELECT 1,[3,NULL],-2,99").await?;
    let body = |subtract| {
        array_transform(
            col("items"),
            lambda(
                ["x"],
                array_transform(
                    col("items"),
                    lambda(
                        ["x"],
                        if subtract {
                            lambda_var("x") - col("k")
                        } else {
                            lambda_var("x") + col("k")
                        },
                    ),
                ),
            ),
        )
    };
    let expression = when(col("k").gt(lit(0i64)), body(false))
        .otherwise(body(true))?
        .resolve_lambda_variables(frame.schema())?
        .data;
    let original = create_physical_expr(&expression, frame.schema(), &ExecutionProps::new())?;
    let unchanged = reassign_expr_columns(Arc::clone(&original), frame.schema().as_arrow())?;
    assert!(Arc::ptr_eq(&original, &unchanged));
    let mapping = vec![
        ProjectionExpr {
            expr: Arc::new(Column::new("k", 2)),
            alias: "k".into(),
        },
        ProjectionExpr {
            expr: Arc::new(Column::new("items", 1)),
            alias: "items".into(),
        },
    ];
    let projected = update_expr_with_input_schema_size(&original, &mapping, false, 2)?
        .ok_or("projection rejected")?;
    assert!(update_expr_with_input_schema_size(&original, &mapping[..1], false, 1)?.is_none());
    let restored = update_expr_with_input_schema_size(&projected, &mapping, true, 4)?
        .ok_or("unprojection rejected")?;
    let schema = frame.schema().as_arrow().project(&[2, 1])?;
    let reassigned = reassign_expr_columns(Arc::clone(&original), &schema)?;
    let mut rows = Vec::new();
    for batch in frame.collect().await? {
        let compact = batch.project(&[2, 1])?;
        for row in 0..batch.num_rows() {
            let expected = original.evaluate(&batch)?.into_array(batch.num_rows())?;
            for (expr, input) in [
                (&projected, &compact),
                (&reassigned, &compact),
                (&restored, &batch),
            ] {
                let actual = expr.evaluate(input)?.into_array(input.num_rows())?;
                assert_eq!(
                    encode_value(actual.as_ref(), row)?,
                    encode_value(expected.as_ref(), row)?
                );
            }
            rows.push(encode_value(expected.as_ref(), row)?);
        }
    }
    rows.sort_by_key(Value::to_string);
    let mut expected = vec![
        json!([["4", "5"], ["4", "5"]]),
        json!([["5", null], ["5", null]]),
    ];
    expected.sort_by_key(Value::to_string);
    assert_eq!(rows, expected);
    Ok(())
}

#[tokio::test]
async fn native_filter_and_join_consumers_preserve_lambda_scope() -> TestResult {
    use datafusion::functions_nested::expr_fn::array_has;
    let session = session(32);
    let frame = session
        .sql("SELECT 0 AS id,[1,2] AS items,3 AS k UNION ALL SELECT 1,[3,NULL],-2")
        .await?;
    let transformed = array_transform(col("items"), lambda(["x"], lambda_var("x") + col("k")))
        .resolve_lambda_variables(frame.schema())?
        .data;
    let filtered = frame
        .clone()
        .filter(array_has(transformed.clone(), lit(4i64)))?
        .select(vec![col("id"), transformed.clone().alias("result")])?;
    let mut filtered_rows = Vec::new();
    for batch in filtered.collect().await? {
        for row in 0..batch.num_rows() {
            filtered_rows.push(json!([
                encode_value(batch.column(0).as_ref(), row)?,
                encode_value(batch.column(1).as_ref(), row)?
            ]));
        }
    }
    assert_eq!(filtered_rows, vec![json!(["0", ["4", "5"]])]);
    for both_sides in [false, true] {
        let right = session
            .sql("SELECT 2 AS rid,4 AS wanted UNION ALL SELECT 3,99")
            .await?;
        let wanted = if both_sides { col("wanted") } else { lit(4i64) };
        let joined = frame
            .clone()
            .join_on(
                right,
                JoinType::Inner,
                [col("id")
                    .lt(col("rid"))
                    .and(array_has(transformed.clone(), wanted))],
            )?
            .select(vec![
                col("id"),
                col("rid"),
                transformed.clone().alias("result"),
            ])?;
        let mut rows = Vec::new();
        for batch in joined.collect().await? {
            for row in 0..batch.num_rows() {
                rows.push(json!([
                    encode_value(batch.column(0).as_ref(), row)?,
                    encode_value(batch.column(1).as_ref(), row)?,
                    encode_value(batch.column(2).as_ref(), row)?
                ]));
            }
        }
        rows.sort_by_key(Value::to_string);
        let mut expected = vec![json!(["0", "2", ["4", "5"]])];
        if !both_sides {
            expected.push(json!(["0", "3", ["4", "5"]]));
        }
        assert_eq!(rows, expected);
    }
    Ok(())
}

#[tokio::test]
async fn legacy_projection_apis_retain_full_source_width() -> TestResult {
    use datafusion::physical_expr::expressions::Column;
    use datafusion::physical_expr::projection::{
        ProjectionExpr, ProjectionExprs, update_expr, update_expr_with_input_schema_size,
    };
    let session = session(32);
    let frame = session
        .sql("SELECT 0 AS ignored,[1,2] AS items,3 AS k,99 AS trailing,7 AS partition_value")
        .await?;
    let expression = array_transform(
        col("items"),
        lambda(
            ["x"],
            array_transform(col("items"), lambda(["x"], lambda_var("x") + col("k"))),
        ),
    )
    .resolve_lambda_variables(frame.schema())?
    .data;
    let physical = create_physical_expr(&expression, frame.schema(), &ExecutionProps::new())?;
    let source = ProjectionExprs::from_indices(&[2, 1], frame.schema().as_arrow());
    let replacement_expression =
        array_transform(col("items"), lambda(["x"], lambda_var("x") + col("k")))
            .resolve_lambda_variables(frame.schema())?
            .data;
    let replacement_target = create_physical_expr(
        &replacement_expression,
        frame.schema(),
        &ExecutionProps::new(),
    )?;
    let reordered =
        ProjectionExprs::from_indices(&[1, 0], &source.project_schema(frame.schema().as_arrow())?);
    let merged = source.try_merge(&reordered)?;
    let unchanged = source.clone().try_map_exprs(Ok)?;
    let projector =
        ProjectionExprs::new(source.iter().cloned()).make_projector(frame.schema().as_arrow())?;
    let batches = frame.collect().await?;
    assert_eq!(batches.len(), 1);
    for selection in [&source, &merged, &unchanged, projector.projection()] {
        let projected =
            update_expr_with_input_schema_size(&physical, selection.as_ref(), false, 2)?
                .ok_or("projection rejected")?;
        let restored = selection.unproject_expr(&projected)?;
        let actual = restored.evaluate(&batches[0])?.into_array(1)?;
        assert_eq!(
            encode_value(actual.as_ref(), 0)?,
            json!([["4", "5"], ["4", "5"]])
        );
        let projected_target = selection.project_expr(&replacement_target)?;
        let restored_target = selection.unproject_expr(&projected_target)?;
        let DataType::List(item) = batches[0].schema().field(1).data_type().clone() else {
            return Err("expected native list".into());
        };
        let replacement = datafusion::physical_expr::expressions::lambda(
            ["x"],
            Arc::new(LambdaVariable::new(
                5,
                Arc::new(item.as_ref().clone().with_name("x")),
            )),
        )?;
        let mut children = restored_target
            .children()
            .into_iter()
            .cloned()
            .collect::<Vec<_>>();
        children[1] = replacement;
        let replaced = restored_target.with_new_children(children)?;
        let actual = replaced.evaluate(&batches[0])?.into_array(1)?;
        assert_eq!(encode_value(actual.as_ref(), 0)?, json!(["1", "2"]));
        assert!(update_expr(&projected, selection.as_ref(), true)?.is_none());
        let expressions = selection.iter().cloned().collect::<Vec<ProjectionExpr>>();
        let unknown = [
            ProjectionExprs::new(expressions.clone()),
            ProjectionExprs::from(expressions.clone()),
            ProjectionExprs::from(expressions.as_slice()),
            expressions.clone().into_iter().collect::<ProjectionExprs>(),
            ProjectionExprs::from_expressions(Arc::<[ProjectionExpr]>::from(expressions)),
        ];
        for projection in unknown {
            assert_eq!(projection, *selection);
            let error = projection
                .unproject_expr(&projected)
                .unwrap_err()
                .to_string();
            assert!(
                error.contains("unproject_expr_with_input_schema_size"),
                "{error}"
            );
            assert!(error.contains("complete source schema width"), "{error}");
            let explicit = projection.unproject_expr_with_input_schema_size(&projected, 5)?;
            let actual = explicit.evaluate(&batches[0])?.into_array(1)?;
            assert_eq!(
                encode_value(actual.as_ref(), 0)?,
                json!([["4", "5"], ["4", "5"]])
            );
        }
        let changed = selection.clone().try_map_exprs(|expr| {
            let column = expr.downcast_ref::<Column>().ok_or_else(|| {
                datafusion::common::DataFusionError::Internal("expected column".into())
            })?;
            Ok(Arc::new(Column::new(column.name(), column.index() + 1)) as Arc<dyn PhysicalExpr>)
        })?;
        assert!(changed.unproject_expr(&projected).is_err());
    }
    let unknown = ProjectionExprs::new(source.iter().cloned());
    let column: Arc<dyn PhysicalExpr> = Arc::new(Column::new("k", 0));
    let unprojected = unknown.unproject_expr(&column)?;
    assert_eq!(
        unprojected.downcast_ref::<Column>(),
        Some(&Column::new("k", 2))
    );
    assert!(update_expr(&column, source.as_ref(), true)?.is_some());
    Ok(())
}

#[tokio::test]
async fn datasource_legacy_unprojection_preserves_filter_fallback() -> TestResult {
    use datafusion::common::config::ConfigOptions;
    use datafusion::datasource::physical_plan::{ArrowSource, FileScanConfigBuilder};
    use datafusion::datasource::source::DataSource;
    use datafusion::datasource::table_schema::TableSchema;
    use datafusion::execution::object_store::ObjectStoreUrl;
    use datafusion::functions_nested::expr_fn::array_has;
    use datafusion::physical_expr::projection::{
        ProjectionExprs, update_expr_with_input_schema_size,
    };
    use datafusion::physical_plan::filter_pushdown::PushedDown;
    let session = session(32);
    let frame = session
        .sql("SELECT 0 AS ignored,[1,2] AS items,3 AS k,99 AS trailing,7 AS partition_value")
        .await?;
    let schema = frame.schema().as_arrow();
    let table_schema = TableSchema::new(
        Arc::new(schema.project(&[0, 1, 2, 3])?),
        vec![Arc::clone(&schema.fields()[4])],
    );
    let scan = FileScanConfigBuilder::new(
        ObjectStoreUrl::local_filesystem(),
        Arc::new(ArrowSource::new_file_source(table_schema)),
    )
    .with_projection_indices(Some(vec![2, 1]))?
    .build();
    let projection = ProjectionExprs::from_indices(&[2, 1], schema);
    let reorder = ProjectionExprs::from_indices(&[1, 0], scan.projected_schema()?.as_ref());
    let merged = scan
        .try_swapping_with_projection(&reorder)?
        .ok_or("projection declined")?;
    let merged_projection = projection.try_merge(&reorder)?;
    let expression = array_has(
        array_transform(col("items"), lambda(["x"], lambda_var("x") + col("k"))),
        lit(4i64),
    )
    .resolve_lambda_variables(frame.schema())?
    .data;
    let physical = create_physical_expr(&expression, frame.schema(), &ExecutionProps::new())?;
    let stages: [(&dyn DataSource, &ProjectionExprs); 2] =
        [(&scan, &projection), (merged.as_ref(), &merged_projection)];
    for (source, projection) in stages {
        let filter = update_expr_with_input_schema_size(&physical, projection.as_ref(), false, 2)?
            .ok_or("filter projection declined")?;
        let result = source.try_pushdown_filters(vec![filter], &ConfigOptions::new())?;
        assert_eq!(result.filters.len(), 1);
        assert!(matches!(result.filters[0], PushedDown::No));
    }
    Ok(())
}

#[tokio::test]
async fn custom_datasource_projection_preserves_filter_fallback() -> TestResult {
    use datafusion::common::config::ConfigOptions;
    use datafusion::datasource::physical_plan::{
        FileOpener, FileScanConfig, FileScanConfigBuilder, FileSource,
    };
    use datafusion::datasource::source::DataSource;
    use datafusion::datasource::table_schema::TableSchema;
    use datafusion::execution::object_store::ObjectStoreUrl;
    use datafusion::functions_nested::expr_fn::array_has;
    use datafusion::object_store::ObjectStore;
    use datafusion::physical_expr::projection::{ProjectionExpr, ProjectionExprs};
    use datafusion::physical_plan::filter_pushdown::PushedDown;
    use datafusion::physical_plan::metrics::ExecutionPlanMetricsSet;

    #[derive(Clone)]
    struct CustomSource {
        schema: TableSchema,
        projection: ProjectionExprs,
        metrics: ExecutionPlanMetricsSet,
    }
    impl FileSource for CustomSource {
        fn create_file_opener(
            &self,
            _: Arc<dyn ObjectStore>,
            _: &FileScanConfig,
            _: usize,
        ) -> datafusion::common::Result<Arc<dyn FileOpener>> {
            Err(datafusion::common::DataFusionError::Internal(
                "test source has no files".into(),
            ))
        }
        fn table_schema(&self) -> &TableSchema {
            &self.schema
        }
        fn with_batch_size(&self, _: usize) -> Arc<dyn FileSource> {
            Arc::new(self.clone())
        }
        fn projection(&self) -> Option<&ProjectionExprs> {
            Some(&self.projection)
        }
        fn metrics(&self) -> &ExecutionPlanMetricsSet {
            &self.metrics
        }
        fn file_type(&self) -> &str {
            "custom"
        }
    }

    let session = session(32);
    let frame = session
        .sql("SELECT 0 AS ignored,[1,2] AS items,3 AS k,99 AS trailing,7 AS partition_value")
        .await?;
    let schema = frame.schema().as_arrow();
    let table_schema = TableSchema::new(
        Arc::new(schema.project(&[0, 1, 2, 3])?),
        vec![Arc::clone(&schema.fields()[4])],
    );
    for indices in [vec![0, 1, 2, 3, 4], vec![2, 1], vec![1, 2]] {
        let selection = ProjectionExprs::from_indices(&indices, schema);
        for use_arc in [false, true] {
            let expressions = selection.iter().cloned().collect::<Vec<ProjectionExpr>>();
            let projection = if use_arc {
                ProjectionExprs::from_expressions(Arc::<[ProjectionExpr]>::from(expressions))
            } else {
                ProjectionExprs::new(expressions)
            };
            let scan = FileScanConfigBuilder::new(
                ObjectStoreUrl::local_filesystem(),
                Arc::new(CustomSource {
                    schema: table_schema.clone(),
                    projection,
                    metrics: ExecutionPlanMetricsSet::new(),
                }),
            )
            .build();
            for capture in [false, true] {
                let body = if capture {
                    lambda_var("x") + col("k")
                } else {
                    lambda_var("x") + lit(1i64)
                };
                let expression = array_has(
                    array_transform(col("items"), lambda(["x"], body)),
                    lit(2i64),
                )
                .resolve_lambda_variables(frame.schema())?
                .data;
                let physical =
                    create_physical_expr(&expression, frame.schema(), &ExecutionProps::new())?;
                let filter = selection.project_expr(&physical)?;
                let result = scan.try_pushdown_filters(vec![filter], &ConfigOptions::new())?;
                assert_eq!(result.filters.len(), 1);
                assert!(matches!(result.filters[0], PushedDown::No));
                assert!(result.updated_node.is_none());
            }
        }
    }
    Ok(())
}

#[tokio::test]
async fn projection_replacement_rebases_lambda_and_source_width() -> TestResult {
    use datafusion::datasource::memory::MemorySourceConfig;
    use datafusion::physical_expr::expressions::Column;
    use datafusion::physical_expr::projection::ProjectionExpr;
    use datafusion::physical_plan::ExecutionPlan;
    use datafusion::physical_plan::projection::ProjectionExec;
    let session = session(32);
    let frame = session
        .sql("SELECT 0 AS ignored,[1,2] AS items,3 AS k")
        .await?;
    let schema = Arc::new(frame.schema().as_arrow().clone());
    let expression = array_transform(col("items"), lambda(["x"], lambda_var("x") + col("k")))
        .resolve_lambda_variables(frame.schema())?
        .data;
    let physical = create_physical_expr(&expression, frame.schema(), &ExecutionProps::new())?;
    let batches = frame.collect().await?;
    let input = MemorySourceConfig::try_new_exec(
        std::slice::from_ref(&batches),
        Arc::clone(&schema),
        None,
    )?;
    let projection = Arc::new(ProjectionExec::try_new(
        [
            ProjectionExpr::new(physical, "result"),
            ProjectionExpr::new(Arc::new(Column::new("items", 1)), "items"),
        ],
        input,
    )?);
    let wider_schema = Arc::new(Schema::new(
        schema
            .fields()
            .iter()
            .cloned()
            .chain([
                Arc::new(Field::new("trailing", DataType::Int64, false)),
                Arc::new(Field::new("partition_value", DataType::Int64, false)),
            ])
            .collect::<Vec<_>>(),
    ));
    let mut columns = batches[0].columns().to_vec();
    columns.extend([
        Arc::new(arrow::array::Int64Array::from(vec![99])) as ArrayRef,
        Arc::new(arrow::array::Int64Array::from(vec![7])) as ArrayRef,
    ]);
    let wider = RecordBatch::try_new(Arc::clone(&wider_schema), columns)?;
    let replacement = MemorySourceConfig::try_new_exec(&[vec![wider.clone()]], wider_schema, None)?;
    let rebuilt = projection.with_new_children(vec![replacement])?;
    let rebuilt_projection = rebuilt
        .downcast_ref::<ProjectionExec>()
        .ok_or("expected projection")?;
    let projected_target = Arc::clone(&rebuilt_projection.expr()[0].expr);
    let DataType::List(item) = schema.field(1).data_type() else {
        return Err("expected list".into());
    };
    let replacement = datafusion::physical_expr::expressions::lambda(
        ["x"],
        Arc::new(LambdaVariable::new(
            5,
            Arc::new(item.as_ref().clone().with_name("x")),
        )),
    )?;
    let mut children = projected_target
        .children()
        .into_iter()
        .cloned()
        .collect::<Vec<_>>();
    children[1] = replacement;
    let replaced = projected_target.with_new_children(children)?;
    let actual = replaced.evaluate(&wider)?.into_array(1)?;
    assert_eq!(encode_value(actual.as_ref(), 0)?, json!(["1", "2"]));
    let output = collect(rebuilt, session.task_ctx()).await?;
    assert_eq!(
        encode_value(output[0].column(0).as_ref(), 0)?,
        json!(["4", "5"])
    );
    Ok(())
}

#[test]
fn malformed_lambda_bindings_stay_rejected() -> TestResult {
    let error = datafusion::physical_expr::expressions::lambda(
        ["x", "x"],
        Arc::new(datafusion::physical_expr::expressions::NoOp::new()),
    )
    .unwrap_err();
    assert!(
        error.to_string().contains("lambda params must be unique"),
        "{error}"
    );
    let qualifier = TableReference::bare("scope");
    let field = Arc::new(
        Field::new("x", DataType::Int32, true)
            .with_metadata([("source".into(), "binding".into())].into()),
    );
    let schema = DFSchema::new_with_metadata(
        vec![(Some(qualifier.clone()), field.clone())],
        Default::default(),
    )?;
    let props = ExecutionProps::new().with_qualified_lambda_variables(&qualifier, &["x".into()]);
    let variable = |name: &str, field| {
        Expr::LambdaVariable(datafusion::logical_expr::expr::LambdaVariable::new(
            name.into(),
            field,
        ))
    };
    assert!(create_physical_expr(&variable("x", Some(field.clone())), &schema, &props).is_ok());
    for wrong in [
        Field::new("x", DataType::Int64, true).with_metadata(field.metadata().clone()),
        Field::new("x", DataType::Int32, false).with_metadata(field.metadata().clone()),
        Field::new("x", DataType::Int32, true),
    ] {
        let error = create_physical_expr(&variable("x", Some(Arc::new(wrong))), &schema, &props)
            .unwrap_err();
        assert!(
            error
                .to_string()
                .contains("LambdaVariable field and schema field mismatch"),
            "{error}"
        );
    }
    let error = create_physical_expr(&variable("x", None), &schema, &props).unwrap_err();
    assert!(
        error.to_string().contains("unresolved LambdaVariable"),
        "{error}"
    );
    let error = create_physical_expr(
        &variable("x", Some(field.clone())),
        &schema,
        &ExecutionProps::new(),
    )
    .unwrap_err();
    assert!(
        error.to_string().contains("qualifier for lambda variable"),
        "{error}"
    );
    let wrong_props = ExecutionProps::new()
        .with_qualified_lambda_variables(&TableReference::bare("wrong"), &["x".into()]);
    let error = create_physical_expr(&variable("x", Some(field.clone())), &schema, &wrong_props)
        .unwrap_err();
    assert!(
        error.to_string().contains("not found in planning schema"),
        "{error}"
    );
    let error =
        create_physical_expr(&variable("free", Some(field.clone())), &schema, &props).unwrap_err();
    assert!(
        error.to_string().contains("qualifier for lambda variable"),
        "{error}"
    );

    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![field.clone()])),
        vec![Arc::new(Int32Array::from(vec![Some(7), None]))],
    )?;
    assert!(
        LambdaVariable::new(0, field.clone())
            .evaluate(&batch)
            .is_ok()
    );
    let error = LambdaVariable::new(1, field.clone())
        .evaluate(&batch)
        .unwrap_err();
    assert!(
        error.to_string().contains("batch only has 1 columns"),
        "{error}"
    );
    for wrong in [
        field.as_ref().clone().with_name("wrong"),
        Field::new("x", DataType::Int64, true).with_metadata(field.metadata().clone()),
        Field::new("x", DataType::Int32, false).with_metadata(field.metadata().clone()),
        Field::new("x", DataType::Int32, true),
    ] {
        let error = LambdaVariable::new(0, Arc::new(wrong))
            .evaluate(&batch)
            .unwrap_err();
        assert!(
            error.to_string().contains("doesn't match batch field"),
            "{error}"
        );
    }
    Ok(())
}

#[test]
fn map_observations_preserve_values_empty_maps_and_null_containers() -> TestResult {
    let mut maps = MapBuilder::new(None, StringBuilder::new(), Int32Builder::new());
    maps.keys().append_value("missing");
    maps.values().append_null();
    maps.keys().append_value("present");
    maps.values().append_value(7);
    maps.append(true)?;
    maps.append(true)?;
    maps.append(false)?;
    let maps = maps.finish();
    assert_eq!(
        encode_value(&maps, 0)?,
        json!([["missing", null], ["present", "7"]])
    );
    assert_eq!(encode_value(&maps, 1)?, json!([]));
    assert_eq!(encode_value(&maps, 2)?, Value::Null);
    let schema = Schema::new(vec![Field::new("m", maps.data_type().clone(), true)]);
    assert_eq!(
        capture_schema(schema.fields(), None)?,
        json!({"type":"struct","fields":[{
        "name":"m","type":{"type":"map","keyType":"string","valueType":"integer","valueContainsNull":true},
        "nullable":true,"metadata":{}}]})
    );
    Ok(())
}

#[tokio::test]
async fn runtime_concat_nested_lambda_preserves_selected_rows_without_captures() -> TestResult {
    use datafusion::logical_expr::{
        ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
    };
    use std::sync::atomic::{AtomicUsize, Ordering};

    static OBSERVED_ROWS: AtomicUsize = AtomicUsize::new(0);

    #[derive(Debug, PartialEq, Eq, Hash)]
    struct CountRows {
        signature: Signature,
    }
    impl ScalarUDFImpl for CountRows {
        fn name(&self) -> &str {
            "lambda_count_rows"
        }
        fn signature(&self) -> &Signature {
            &self.signature
        }
        fn return_type(&self, _: &[DataType]) -> datafusion::common::Result<DataType> {
            Ok(DataType::Int32)
        }
        fn invoke_with_args(
            &self,
            args: ScalarFunctionArgs,
        ) -> datafusion::common::Result<ColumnarValue> {
            OBSERVED_ROWS.fetch_add(args.number_rows, Ordering::SeqCst);
            Ok(ColumnarValue::Array(Arc::new(Int32Array::from(vec![
                10;
                args.number_rows
            ]))))
        }
    }

    for batch_size in [1, 32] {
        for masks in [vec![Some("a"), None, Some("b")], vec![None, None], vec![]] {
            let session = session(batch_size);
            session.register_udf(ScalarUDF::from(CountRows {
                signature: Signature::exact(vec![], Volatility::Volatile),
            }));
            let schema = Arc::new(Schema::new(vec![Field::new("mask", DataType::Utf8, true)]));
            let batch = RecordBatch::try_new(
                schema.clone(),
                vec![Arc::new(arrow::array::StringArray::from(masks.clone()))],
            )?;
            let batches = (0..batch.num_rows())
                .step_by(batch_size)
                .map(|offset| batch.slice(offset, batch_size.min(batch.num_rows() - offset)))
                .collect();
            session.register_table(
                "lambda_masks",
                Arc::new(MemTable::try_new(schema, vec![batches])?),
            )?;
            for ansi in [false, true] {
                OBSERVED_ROWS.store(0, Ordering::SeqCst);
                let query = plan_query(
                    &session,
                    "SELECT CONCAT(mask, CAST(ARRAY_MAX(TRANSFORM(ARRAY(1,2), \
                     y -> y + lambda_count_rows())) AS STRING)) AS r FROM lambda_masks",
                    &SparkSqlOptions {
                        ansi_mode: ansi,
                        ..Default::default()
                    },
                )
                .await?;
                let physical = query.create_physical_plan(&session).await?;
                let schema = physical.schema();
                let mut actual = Vec::new();
                for batch in collect(physical, session.task_ctx()).await? {
                    spark_results::validate_batch(schema.as_ref(), &batch)?;
                    for row in 0..batch.num_rows() {
                        actual.push(encode_value(batch.column(0).as_ref(), row)?);
                    }
                }
                let expected = masks
                    .iter()
                    .map(|mask| match mask {
                        Some(mask) => json!(format!("{mask}12")),
                        None => Value::Null,
                    })
                    .collect::<Vec<_>>();
                assert_eq!(actual, expected, "ANSI={ansi} batch={batch_size}");
                assert_eq!(
                    OBSERVED_ROWS.load(Ordering::SeqCst),
                    2 * masks.iter().filter(|mask| mask.is_some()).count(),
                    "ANSI={ansi} batch={batch_size}"
                );
            }
        }
    }
    Ok(())
}

#[tokio::test]
async fn original_concat_nested_lambda_consumer_keeps_both_parameter_scopes() -> TestResult {
    let records = include_str!("fixtures/nested-lambda-concat-consumer-spark.jsonl")
        .lines()
        .map(serde_json::from_str::<Value>)
        .collect::<Result<Vec<_>, _>>()?;
    assert_eq!(records.len() - 1, 6);
    let mut differences = Vec::new();
    for payload_type in [DataType::Utf8, DataType::LargeUtf8, DataType::Utf8View] {
        for batch_size in [1, 32] {
            let session = session(batch_size);
            spark_inputs::register_recorded_inputs(&session, &records[0], batch_size)?;
            let input = session.table("slice_projection_input").await?;
            let mut fields = input.schema().fields().iter().cloned().collect::<Vec<_>>();
            fields[1] = Arc::new(
                fields[1]
                    .as_ref()
                    .clone()
                    .with_data_type(payload_type.clone()),
            );
            let schema = Arc::new(Schema::new(fields));
            let mut encoded_batches = Vec::new();
            for batch in input.collect().await? {
                let mut columns = batch.columns().to_vec();
                columns[1] = arrow::compute::cast(&columns[1], &payload_type)?;
                encoded_batches.push(RecordBatch::try_new(Arc::clone(&schema), columns)?);
            }
            assert_eq!(schema.field(1).data_type(), &payload_type);
            session.deregister_table("slice_projection_input")?;
            session.register_table(
                "slice_projection_input",
                Arc::new(MemTable::try_new(schema, vec![encoded_batches])?),
            )?;
            for case in &records[1..] {
                let label = format!(
                    "{} {payload_type:?} batch={batch_size} ANSI={}",
                    case["id"], case["ansi"]
                );
                // Separate resolve-only probe; the primary observation is collected below.
                let resolved = plan_query(
                    &session,
                    case["sql"].as_str().ok_or("missing SQL")?,
                    &SparkSqlOptions {
                        ansi_mode: case["ansi"].as_bool().ok_or("missing ANSI")?,
                        session_timezone: "UTC".into(),
                        ..SparkSqlOptions::default()
                    },
                )
                .await;
                let analyzed_payload_type_matches = resolved.as_ref().is_ok_and(|query| {
                    query
                        .column_names
                        .get(1)
                        .is_some_and(|name| name == "payload")
                        && query
                            .logical_plan
                            .schema()
                            .fields()
                            .get(1)
                            .is_some_and(|field| field.data_type() == &payload_type)
                });
                let observed = string_binary::capture_observation(&session, case).await;
                match observed {
                    Ok(actual)
                        if actual["error"].is_null()
                            && actual["analyzed_schema"] == case["schema_stages"]["analyzed"]
                            && actual["physical_schema"] == case["schema_stages"]["executed"]
                            && actual["rows"] == case["expected"]["rows"]
                            && analyzed_payload_type_matches
                            && actual["physical_arrow_schema"].as_str().is_some_and(|raw| {
                                raw.contains(&format!(
                                    "name: \"payload\", data_type: {payload_type:?},"
                                ))
                            }) => {}
                    Ok(actual) => differences.push(format!(
                        "{label}: resolve-only payload probe={analyzed_payload_type_matches}, {actual}"
                    )),
                    Err(error) => differences.push(format!("{label}: {error}")),
                }
            }
        }
    }
    assert!(differences.is_empty(), "{}", differences.join("\n"));
    Ok(())
}

#[tokio::test]
async fn lambda_rewrites_preserve_callback_errors_and_reconstruction_validation() -> TestResult {
    use datafusion::common::DataFusionError;
    use datafusion::common::config::ConfigOptions;
    use datafusion::logical_expr::async_udf::{AsyncScalarUDF, AsyncScalarUDFImpl};
    use datafusion::logical_expr::{
        ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
    };
    use datafusion::physical_expr::ScalarFunctionExpr;
    use datafusion::physical_expr::expressions::Column;
    use datafusion::physical_expr::utils::rewrite_expr_columns;

    #[derive(Debug, PartialEq, Eq, Hash)]
    struct AsyncIdentity(Signature);
    impl ScalarUDFImpl for AsyncIdentity {
        fn name(&self) -> &str {
            "lambda_async_identity"
        }
        fn signature(&self) -> &Signature {
            &self.0
        }
        fn return_type(&self, _: &[DataType]) -> datafusion::common::Result<DataType> {
            Ok(DataType::Int64)
        }
        fn invoke_with_args(
            &self,
            _: ScalarFunctionArgs,
        ) -> datafusion::common::Result<ColumnarValue> {
            datafusion::common::exec_err!("lambda async identity requires asynchronous invocation")
        }
    }
    impl AsyncScalarUDFImpl for AsyncIdentity {
        fn invoke_async_with_args<'life0, 'async_trait>(
            &'life0 self,
            args: ScalarFunctionArgs,
        ) -> std::pin::Pin<
            Box<
                dyn std::future::Future<Output = datafusion::common::Result<ColumnarValue>>
                    + Send
                    + 'async_trait,
            >,
        >
        where
            'life0: 'async_trait,
            Self: 'async_trait,
        {
            Box::pin(async move { Ok(args.args[0].clone()) })
        }
    }

    #[derive(Debug, Eq)]
    struct CaptureReconstructionProbe {
        child: Arc<dyn PhysicalExpr>,
        udf: Option<Arc<ScalarUDF>>,
    }
    impl PartialEq for CaptureReconstructionProbe {
        fn eq(&self, other: &Self) -> bool {
            self.child.eq(&other.child) && self.udf.eq(&other.udf)
        }
    }
    impl std::hash::Hash for CaptureReconstructionProbe {
        fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
            std::hash::Hash::hash(&self.child, state);
            std::hash::Hash::hash(&self.udf, state);
        }
    }
    impl std::fmt::Display for CaptureReconstructionProbe {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.write_str("capture_reconstruction_probe")
        }
    }
    impl PhysicalExpr for CaptureReconstructionProbe {
        fn fmt_sql(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            std::fmt::Display::fmt(self, f)
        }
        fn data_type(&self, schema: &Schema) -> datafusion::common::Result<DataType> {
            self.child.data_type(schema)
        }
        fn nullable(&self, schema: &Schema) -> datafusion::common::Result<bool> {
            self.child.nullable(schema)
        }
        fn return_field(&self, schema: &Schema) -> datafusion::common::Result<Arc<Field>> {
            self.child.return_field(schema)
        }
        fn evaluate(&self, batch: &RecordBatch) -> datafusion::common::Result<ColumnarValue> {
            self.child.evaluate(batch)
        }
        fn children(&self) -> Vec<&Arc<dyn PhysicalExpr>> {
            vec![&self.child]
        }
        fn with_new_children(
            self: Arc<Self>,
            children: Vec<Arc<dyn PhysicalExpr>>,
        ) -> datafusion::common::Result<Arc<dyn PhysicalExpr>> {
            let [child] = children.as_slice() else {
                return datafusion::common::internal_err!(
                    "capture reconstruction probe expects one child"
                );
            };
            // Own normalization maps k@2 to k@0; the outer rebase maps it to k@1.
            let rebased = child
                .downcast_ref::<Column>()
                .is_some_and(|column| column.index() == 1);
            if rebased && let Some(udf) = &self.udf {
                return Ok(Arc::new(ScalarFunctionExpr::new(
                    "lambda_async_identity",
                    Arc::clone(udf),
                    vec![Arc::clone(child)],
                    Arc::new(Field::new("", DataType::Int64, true)),
                    Arc::new(ConfigOptions::new()),
                )));
            }
            if rebased {
                use datafusion::common::ScalarValue;
                use datafusion::logical_expr::Operator;
                use datafusion::physical_expr::expressions::{BinaryExpr, Literal};
                return Ok(Arc::new(BinaryExpr::new(
                    Arc::clone(child),
                    Operator::Plus,
                    Arc::new(Literal::new(ScalarValue::Int64(Some(100)))),
                )));
            }
            Ok(Arc::new(Self {
                child: Arc::clone(child),
                udf: self.udf.clone(),
            }))
        }
    }

    let session = session(32);
    let frame = session
        .sql("SELECT 0 AS ignored,[1,2] AS items,3 AS k")
        .await?;
    let expression = array_transform(col("items"), lambda(["x"], lambda_var("x") + col("k")))
        .resolve_lambda_variables(frame.schema())?
        .data;
    let physical = create_physical_expr(&expression, frame.schema(), &ExecutionProps::new())?;
    let bound_lambda = Arc::clone(physical.children()[1]);
    let width = frame.schema().fields().len();
    let mut mapped = Vec::new();
    let error = rewrite_expr_columns(Arc::clone(&bound_lambda), width, |column| {
        mapped.push((column.name().to_owned(), column.index()));
        Err(DataFusionError::Execution(
            "lambda identity callback sentinel".into(),
        ))
    })
    .unwrap_err();
    assert_eq!(mapped, vec![("k".to_owned(), 2)]);
    assert!(
        matches!(error, DataFusionError::Execution(ref cause) if cause == "lambda identity callback sentinel"),
        "{error}"
    );

    let udf = AsyncScalarUDF::new(Arc::new(AsyncIdentity(Signature::exact(
        vec![DataType::Int64],
        Volatility::Volatile,
    ))))
    .into_scalar_udf();
    let udf = Arc::new(udf);
    let asynchronous: Arc<dyn PhysicalExpr> = Arc::new(ScalarFunctionExpr::new(
        "lambda_async_identity",
        Arc::clone(&udf),
        vec![Arc::new(Column::new("k", 2))],
        Arc::new(Field::new("", DataType::Int64, true)),
        Arc::new(ConfigOptions::new()),
    ));
    for error in [
        rewrite_expr_columns(Arc::clone(&bound_lambda), width, |_| {
            Ok(Arc::clone(&asynchronous))
        })
        .unwrap_err(),
        bound_lambda
            .with_new_children(vec![Arc::clone(&asynchronous)])
            .unwrap_err(),
    ] {
        assert!(
            matches!(error, DataFusionError::Plan(ref cause) if cause.contains("Async functions in lambdas aren't supported")),
            "{error}"
        );
    }
    let nested = array_transform(
        col("items"),
        lambda(
            ["outer"],
            array_transform(
                col("items"),
                lambda(["inner"], lambda_var("inner") + col("k")),
            ),
        ),
    )
    .resolve_lambda_variables(frame.schema())?
    .data;
    let nested = create_physical_expr(&nested, frame.schema(), &ExecutionProps::new())?;
    let outer_lambda = Arc::clone(nested.children()[1]);
    let inner_function = Arc::clone(outer_lambda.children()[0]);
    let inner_lambda = Arc::clone(inner_function.children()[1]);
    let batches = frame.clone().collect().await?;
    for inject_async in [true, false] {
        let replacement = Arc::clone(&inner_lambda).with_new_children(vec![Arc::new(
            CaptureReconstructionProbe {
                child: Arc::new(Column::new("k", 2)),
                udf: inject_async.then(|| Arc::clone(&udf)),
            },
        )])?;
        let mut children = inner_function
            .children()
            .into_iter()
            .cloned()
            .collect::<Vec<_>>();
        children[1] = replacement;
        let replaced_inner = Arc::clone(&inner_function).with_new_children(children)?;
        let result = Arc::clone(&outer_lambda).with_new_children(vec![replaced_inner]);
        if inject_async {
            let error = result.unwrap_err();
            assert!(
                matches!(error, DataFusionError::Plan(ref cause) if cause.contains("Async functions in lambdas aren't supported")),
                "{error}"
            );
        } else {
            let mut children = nested.children().into_iter().cloned().collect::<Vec<_>>();
            children[1] = result?;
            let rebuilt = Arc::clone(&nested).with_new_children(children)?;
            assert_eq!(
                rebuilt.return_field(frame.schema().as_arrow())?,
                nested.return_field(frame.schema().as_arrow())?
            );
            let mut actual = Vec::new();
            for batch in &batches {
                let values = rebuilt.evaluate(batch)?.into_array(batch.num_rows())?;
                for row in 0..values.len() {
                    actual.push(encode_value(values.as_ref(), row)?);
                }
            }
            assert_eq!(actual, vec![json!([["103", "103"], ["103", "103"]])]);
        }
    }
    Ok(())
}

#[test]
fn nested_capture_compaction_and_equal_width_permutations_preserve_values_and_fields() -> TestResult
{
    use datafusion::physical_expr::expressions::Column;
    use datafusion::physical_expr::utils::rewrite_expr_columns;

    let items: ArrayRef = Arc::new(ListArray::from_iter_primitive::<Int32Type, _, _>([
        Some(vec![Some(1), Some(2)]),
        Some(vec![]),
        None,
    ]));
    let schema = Arc::new(Schema::new(vec![
        Field::new("items", items.data_type().clone(), true)
            .with_metadata([("source".into(), "items".into())].into()),
        Field::new("left", DataType::Int32, true)
            .with_metadata([("source".into(), "left".into())].into()),
        Field::new("right", DataType::Int32, true)
            .with_metadata([("source".into(), "right".into())].into()),
        Field::new("trailing", DataType::Int32, false),
    ]));
    let input = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            items,
            Arc::new(Int32Array::from(vec![3, 4, 5])),
            Arc::new(Int32Array::from(vec![7, 8, 9])),
            Arc::new(Int32Array::from(vec![99; 3])),
        ],
    )?;
    let input_schema = DFSchema::try_from(schema.as_ref().clone())?;
    let permutation = [0, 2, 1, 3];
    let permuted = input.project(&permutation)?;
    assert_eq!(
        permuted.schema().as_ref(),
        &Schema::new(vec![
            schema.field(0).clone(),
            schema.field(2).clone(),
            schema.field(1).clone(),
            schema.field(3).clone(),
        ])
    );
    let nested_type = DataType::List(Arc::new(Field::new(
        "item",
        DataType::List(Arc::new(Field::new("item", DataType::Int32, true))),
        true,
    )));
    let expected_field = Arc::new(Field::new("", nested_type, true));
    for (capture_outer, use_case) in [(false, false), (true, false), (false, true)] {
        let body = lambda_var("inner") + col("left") * lit(10i32) + col("right");
        let body = if capture_outer {
            body + lambda_var("outer")
        } else {
            body
        };
        let body = if use_case {
            when(col("left").gt(lit(0i32)), body.clone()).otherwise(body + lit(100i32))?
        } else {
            body
        };
        let expression = array_transform(
            col("items"),
            lambda(
                ["outer"],
                array_transform(col("items"), lambda(["inner"], body)),
            ),
        )
        .resolve_lambda_variables(&input_schema)?
        .data;
        let physical = create_physical_expr(&expression, &input_schema, &ExecutionProps::new())?;
        let mut mapped = Vec::new();
        let rewritten =
            rewrite_expr_columns(Arc::clone(&physical), schema.fields().len(), |column| {
                mapped.push((column.name().to_owned(), column.index()));
                let index = match column.index() {
                    1 => 2,
                    2 => 1,
                    index => index,
                };
                Ok(Arc::new(Column::new(column.name(), index)))
            })?;
        assert!(mapped.contains(&("left".to_owned(), 1)), "{mapped:?}");
        assert!(mapped.contains(&("right".to_owned(), 2)), "{mapped:?}");
        let expected = if capture_outer {
            vec![json!([["39", "40"], ["40", "41"]]), json!([]), Value::Null]
        } else {
            vec![json!([["38", "39"], ["38", "39"]]), json!([]), Value::Null]
        };
        for (expression, batch) in [(physical, &input), (rewritten, &permuted)] {
            assert_eq!(
                expression.return_field(batch.schema().as_ref())?,
                expected_field
            );
            for batch_size in [1, 32] {
                let mut actual = Vec::new();
                for offset in (0..batch.num_rows()).step_by(batch_size) {
                    let chunk = batch.slice(offset, batch_size.min(batch.num_rows() - offset));
                    let values = expression.evaluate(&chunk)?.into_array(chunk.num_rows())?;
                    assert_eq!(values.data_type(), expected_field.data_type());
                    for row in 0..values.len() {
                        actual.push(encode_value(values.as_ref(), row)?);
                    }
                }
                assert_eq!(
                    actual, expected,
                    "capture_outer={capture_outer} use_case={use_case} batch={batch_size}"
                );
            }
        }
    }
    Ok(())
}

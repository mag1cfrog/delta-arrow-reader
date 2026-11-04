//! Measured Rust callback contracts. These controls have no Apache Spark oracle credit.
use std::sync::{
    Arc, LazyLock,
    atomic::{AtomicUsize, Ordering},
};

use arrow::datatypes::{DataType, FieldRef};
use arrow::util::display::array_value_to_string;
use datafusion::execution::SessionStateBuilder;
use datafusion::logical_expr::{
    ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature,
    Volatility,
};
use datafusion::prelude::{SessionConfig, SessionContext};
use delta_arrow_reader_spark_sql::{SparkSqlOptions, configure_spark_sql, plan_query};
use serde_json::{Value, json};

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error + Send + Sync>>;
static CALLS: AtomicUsize = AtomicUsize::new(0);

#[derive(Debug, PartialEq, Eq, Hash)]
struct PositionCallback {
    folding: bool,
}

impl ScalarUDFImpl for PositionCallback {
    fn name(&self) -> &str {
        "position_callback"
    }
    fn signature(&self) -> &Signature {
        static SIGNATURE: LazyLock<Signature> =
            LazyLock::new(|| Signature::any(1, Volatility::Immutable));
        &SIGNATURE
    }
    fn supports_constant_folding(&self) -> bool {
        self.folding
    }
    fn return_type(&self, args: &[DataType]) -> datafusion::common::Result<DataType> {
        Ok(args[0].clone())
    }
    fn return_field_from_args(
        &self,
        args: ReturnFieldArgs,
    ) -> datafusion::common::Result<FieldRef> {
        Ok(Arc::clone(&args.arg_fields[0]))
    }
    fn invoke_with_args(
        &self,
        args: ScalarFunctionArgs,
    ) -> datafusion::common::Result<ColumnarValue> {
        CALLS.fetch_add(1, Ordering::SeqCst);
        Ok(args.args[0].clone())
    }
}

#[tokio::test]
async fn regexp_replace_position_preserves_folding_permissions() -> TestResult {
    let records = include_str!("fixtures/regexp-replace-position-callback-native.jsonl")
        .lines()
        .map(serde_json::from_str::<Value>)
        .collect::<Result<Vec<_>, _>>()?;
    let (metadata, profiles) = records.split_first().ok_or("missing native profiles")?;
    assert_eq!(profiles.len(), 36);
    assert_eq!(metadata["spark_oracle_credit"], false);
    let mut host_contract_differences = 0;
    let mut observed = 0;
    for ansi_mode in [true, false] {
        for folding in [true, false] {
            for (kind, literal) in [
                ("string", "'2'"),
                ("bigint", "CAST(2 AS BIGINT)"),
                ("int", "2"),
            ] {
                for surface in ["regexp_replace", "standalone_spark", "standalone_native"] {
                    let session = SessionContext::new_with_state(
                        configure_spark_sql(
                            SessionStateBuilder::new()
                                .with_default_features()
                                .with_config(
                                    SessionConfig::new()
                                        .with_batch_size(32)
                                        .with_target_partitions(2),
                                ),
                        )
                        .build(),
                    );
                    session.register_udf(ScalarUDF::from(PositionCallback { folding }));
                    let sql = if surface == "regexp_replace" {
                        format!(
                            "SELECT regexp_replace('a1a','a','X',position_callback({literal})) AS r"
                        )
                    } else {
                        format!("SELECT position_callback({literal}) AS r")
                    };
                    CALLS.store(0, Ordering::SeqCst);
                    let mut actual = json!({});
                    let physical = if surface == "standalone_native" {
                        match session.sql(&sql).await {
                            Ok(frame) => {
                                actual["after_resolve"] = json!(CALLS.load(Ordering::SeqCst));
                                actual["analyzed_arrow_schema"] =
                                    json!(format!("{:?}", frame.schema()));
                                frame
                                    .create_physical_plan()
                                    .await
                                    .map_err(|e| ("physical", e.to_string()))
                            }
                            Err(error) => Err(("resolve", error.to_string())),
                        }
                    } else {
                        match plan_query(
                            &session,
                            &sql,
                            &SparkSqlOptions {
                                ansi_mode,
                                ..Default::default()
                            },
                        )
                        .await
                        {
                            Ok(query) => {
                                actual["after_resolve"] = json!(CALLS.load(Ordering::SeqCst));
                                actual["analyzed_arrow_schema"] =
                                    json!(format!("{:?}", query.logical_plan.schema()));
                                actual["column_names"] = json!(query.column_names);
                                query
                                    .create_physical_plan(&session)
                                    .await
                                    .map_err(|e| ("physical", e.to_string()))
                            }
                            Err(error) => Err(("resolve", error.to_string())),
                        }
                    };
                    match physical {
                        Err((stage, error)) => {
                            actual["error_stage"] = json!(stage);
                            actual["error"] = json!(error);
                            let field = if stage == "resolve" {
                                "after_resolve"
                            } else {
                                "after_physical"
                            };
                            actual[field] = json!(CALLS.load(Ordering::SeqCst));
                        }
                        Ok(physical) => {
                            actual["after_physical"] = json!(CALLS.load(Ordering::SeqCst));
                            actual["physical_arrow_schema"] =
                                json!(format!("{:?}", physical.schema()));
                            match datafusion::physical_plan::collect(physical, session.task_ctx())
                                .await
                            {
                                Err(error) => {
                                    actual["error_stage"] = json!("execute");
                                    actual["error"] = json!(error.to_string());
                                }
                                Ok(batches) => {
                                    let mut rows = Vec::new();
                                    for batch in batches {
                                        for row in 0..batch.num_rows() {
                                            rows.push(
                                                (0..batch.num_columns())
                                                    .map(|column| {
                                                        array_value_to_string(
                                                            batch.column(column).as_ref(),
                                                            row,
                                                        )
                                                    })
                                                    .collect::<Result<Vec<_>, _>>()?,
                                            );
                                        }
                                    }
                                    actual["rows"] = json!(rows);
                                }
                            }
                            actual["after_execute"] = json!(CALLS.load(Ordering::SeqCst));
                        }
                    }
                    let profile = profiles
                        .iter()
                        .find(|profile| {
                            profile["kind"] == kind
                                && profile["folding"] == folding
                                && profile["surface"] == surface
                                && profile["ansi"] == ansi_mode
                        })
                        .ok_or("missing exact callback key")?;
                    assert_eq!(profile["sql"], sql);
                    assert_eq!(
                        actual, profile["actual"],
                        "{kind} folding={folding} {surface} ANSI={ansi_mode}"
                    );
                    let strict_host_contract = folding
                        || (actual["after_resolve"] == 0
                            && actual
                                .get("after_physical")
                                .is_none_or(|count| count.as_u64() == Some(0)));
                    assert_eq!(profile["strict_host_contract"], strict_host_contract);
                    if !strict_host_contract {
                        assert_eq!(kind, "int");
                        assert_eq!(surface, "regexp_replace");
                        assert!(!folding);
                        assert_eq!(profile["owner"], "#893");
                        host_contract_differences += 1;
                    }
                    observed += 1;
                    eprintln!(
                        "c06 position callback regression: {}",
                        json!({
                            "kind":kind,"folding":folding,"surface":surface,"ansi":ansi_mode,
                            "sql":sql,"actual":actual,"strict_host_contract":strict_host_contract,
                            "owner":profile["owner"],"spark_oracle_credit":false
                        })
                    );
                }
            }
        }
    }
    assert_eq!(observed, 36);
    assert_eq!(host_contract_differences, 2);
    eprintln!(
        "c06 position callback summary: {}",
        json!({
            "observations":observed,"host_contract_differences":host_contract_differences,
            "full_host_contract_pass":false,"spark_oracle_credit":false
        })
    );
    Ok(())
}

//! Single-value UNPIVOT NULL policy, complete Spark Fields, and native isolation.

use std::collections::{BTreeMap, BTreeSet};

use datafusion::execution::SessionStateBuilder;
use datafusion::prelude::{SessionConfig, SessionContext};
use delta_arrow_reader_spark_sql::configure_spark_sql;
use serde_json::{Value, json};

#[path = "support/spark_inputs.rs"]
mod spark_inputs;
#[path = "support/spark_results.rs"]
mod spark_results;
use spark_results::{
    STAGES, TestResult, capture_native_query_observation,
    compare_exact_spark_query_stages_with_optimized, validate_spark_reference_result,
};

fn spark_session(batch: usize) -> SessionContext {
    SessionContext::new_with_state(
        configure_spark_sql(
            SessionStateBuilder::new()
                .with_default_features()
                .with_config(
                    SessionConfig::new()
                        .with_batch_size(batch)
                        .with_target_partitions(2),
                ),
        )
        .build(),
    )
}

fn load_spark_reference_groups() -> TestResult<Vec<(Value, Vec<Value>)>> {
    let mut groups = Vec::new();
    for (source, capture) in [
        (
            include_str!("fixtures/unpivot-null-1076-cases.json"),
            include_str!("fixtures/unpivot-null-1076-spark.jsonl"),
        ),
        (
            include_str!("fixtures/unpivot-nullability-1076-cases.json"),
            include_str!("fixtures/unpivot-nullability-1076-spark.jsonl"),
        ),
    ] {
        let request: Value = serde_json::from_str(source)?;
        let records = capture
            .lines()
            .map(serde_json::from_str::<Value>)
            .collect::<Result<Vec<_>, _>>()?;
        let (header, cases) = records.split_first().ok_or("missing capture header")?;
        let corpora = request["corpora"]
            .as_array()
            .ok_or("missing source corpora")?;
        let receipts = header["corpora"]
            .as_array()
            .ok_or("missing provider receipts")?;
        assert_eq!(request["issue"], 1076);
        assert_eq!(request["spark_version"], "4.2.0");
        assert_eq!(
            request["spark_revision"],
            "32f7299601108917fb01920a54e084595b7b3bf8"
        );
        assert_eq!(request["native_batch_sizes"], json!([1, 32]));
        assert_eq!(request["native_provider_partitions"], 2);
        assert_eq!(request["native_target_partitions"], 2);
        assert_eq!(header["issue"], 1076);
        assert_eq!(header["spark_version"], "4.2.0");
        assert_eq!(
            header["spark_build"]["SPARK_REVISION"]["value"],
            request["spark_revision"]
        );
        assert_eq!(header["spark_only"], true);
        assert_eq!(header["reads_expected_outputs"], false);
        assert_eq!(
            header["effective_settings"]["spark.sql.session.timeZone"],
            "UTC"
        );
        assert_eq!(
            header["schema_stage_protocol"],
            json!(STAGES.map(|(name, _)| name))
        );
        assert_eq!(header["expected_record_count"], json!(cases.len()));
        assert_eq!(request["expected_record_count"], json!(cases.len()));
        assert_eq!(receipts.len(), corpora.len());
        let mut selected = 0;
        for corpus in corpora {
            let receipt = receipts
                .iter()
                .find(|receipt| receipt["corpus"] == corpus["corpus"])
                .ok_or("missing provider receipt")?;
            assert_eq!(receipt["source"], corpus["source"]);
            assert_eq!(receipt["metadata"], corpus["metadata"]);
            let source_cases = corpus["cases"].as_array().ok_or("missing source cases")?;
            let captured = cases
                .iter()
                .filter(|case| case["corpus"] == corpus["corpus"])
                .cloned()
                .collect::<Vec<_>>();
            assert_eq!(captured.len(), source_cases.len());
            for case in &captured {
                validate_spark_reference_result(case)?;
                let source = source_cases
                    .iter()
                    .find(|source| source["id"] == case["id"] && source["ansi"] == case["ansi"])
                    .ok_or("captured identity absent from source")?;
                assert!(
                    source
                        .as_object()
                        .ok_or("invalid source case")?
                        .iter()
                        .all(|(key, value)| case.get(key) == Some(value)),
                    "captured SQL or source context changed"
                );
            }
            selected += captured.len();
            groups.push((receipt["metadata"].clone(), captured));
        }
        assert_eq!(selected, cases.len());
    }
    for (capture, id) in [
        (
            include_str!("fixtures/pivot-source-aliases-supplement-spark.jsonl"),
            "unpivot_source_and_output_aliases",
        ),
        (
            include_str!("fixtures/pivot-source-aliases-relation-order-spark.jsonl"),
            "prior_unpivot_without_source_alias",
        ),
    ] {
        let records = capture
            .lines()
            .map(serde_json::from_str::<Value>)
            .collect::<Result<Vec<_>, _>>()?;
        let (metadata, cases) = records.split_first().ok_or("missing original header")?;
        assert_eq!(metadata["spark_version"], "4.2.0");
        assert_eq!(metadata["timezone"], "UTC");
        let cases = cases
            .iter()
            .filter(|case| case["id"] == id)
            .cloned()
            .collect::<Vec<_>>();
        assert_eq!(cases.len(), 2, "missing original ANSI pair");
        groups.push((metadata.clone(), cases));
    }
    Ok(groups)
}

fn unpivot_error_matches(case: &Value, actual: &Value) -> bool {
    let parameters = &case["spark_error_details"]["parameters"];
    let expected_error = match (case["id"].as_str(), case["expected"]["condition"].as_str()) {
        (Some("unpivot_missing_column_empty"), Some("UNRESOLVED_COLUMN.WITH_SUGGESTION"))
            if case["sql"]
                == "SELECT * FROM empty_inputs UNPIVOT (value FOR label IN (missing,v))"
                && parameters
                    == &json!({"objectName":"`missing`","proposal":"`id`, `g`, `k`, `p`, `v`"}) =>
        {
            "External error: analysis error: attribute ObjectName([Identifier(\"missing\")]) is missing from the schema: cannot resolve attribute"
        }
        (Some("unpivot_incompatible_types_empty"), Some("UNPIVOT_VALUE_DATA_TYPE_MISMATCH"))
            if case["sql"]
                == "SELECT * FROM empty_inputs UNPIVOT INCLUDE NULLS (value FOR label IN (g,p))"
                && parameters == &json!({"types":"\"BOOLEAN\" (`p`), \"INT\" (`g`)"}) =>
        {
            "External error: analysis error: [UNPIVOT_VALUE_DATA_TYPE_MISMATCH] Unpivot value columns must share a least common type, some types do not: Int32, Boolean"
        }
        (Some("unpivot_malformed_clause"), Some("PARSE_SYNTAX_ERROR"))
            if case["sql"] == "SELECT * FROM inputs UNPIVOT (value label IN (g,v))"
                && parameters == &json!({"error":"'label'","hint":": missing 'FOR'"}) =>
        {
            "External error: error in SQL parser: found label at 36:41 expected 'FOR'"
        }
        _ => return false,
    };
    case["spark_error_phase"] == "sql"
        && actual["native_phase"] == "resolve"
        && actual["error"] == expected_error
        && actual["optimized_error"] == expected_error
        && actual.get("observer_error").is_none()
        && actual.get("native_panic").is_none()
        && [
            "public_schema",
            "optimized_schema",
            "before_collect",
            "physical_schema",
            "rows",
            "rows_raw",
            "optimized_observation",
        ]
        .iter()
        .all(|key| actual.get(*key).is_some_and(Value::is_null))
}

#[tokio::test]
async fn unpivot_null_policy_and_fields_match_spark() -> TestResult {
    let groups = load_spark_reference_groups()?;
    let mut identities = BTreeSet::new();
    let mut ansi_modes = BTreeMap::<_, BTreeSet<bool>>::new();
    for (_, cases) in &groups {
        for case in cases {
            let id = case["id"].as_str().ok_or("missing ID")?.to_owned();
            let ansi = case["ansi"].as_bool().ok_or("missing ANSI")?;
            assert!(
                identities.insert((id.clone(), ansi)),
                "duplicate source identity"
            );
            ansi_modes.entry(id).or_default().insert(ansi);
        }
    }
    assert!(
        ansi_modes
            .values()
            .all(|modes| *modes == BTreeSet::from([false, true]))
    );
    let expected = identities
        .iter()
        .flat_map(|(id, ansi)| [1, 32].map(move |batch| (id.clone(), *ansi, batch)))
        .collect::<BTreeSet<_>>();
    let mut completed = BTreeSet::new();
    let mut differences = Vec::new();
    let mut raw_strict_matches = 0;
    let mut native_diagnostic_aliases = 0;
    for (metadata, cases) in &groups {
        for batch in [1, 32] {
            let session = spark_session(batch);
            spark_inputs::register_recorded_inputs(&session, metadata, batch)?;
            for case in cases {
                let actual =
                    compare_exact_spark_query_stages_with_optimized(&session, case).await?;
                let raw_strict = actual["matches"] == true
                    && actual["optimized_matches"] == true
                    && STAGES.iter().all(|(stage, field)| {
                        let recorded = case["schema_stages"].get(*stage).or_else(|| {
                            // The original producer records frame.schema in expected.schema.
                            if *stage == "public" {
                                case["expected"].get("schema")
                            } else {
                                None
                            }
                        });
                        match recorded {
                            Some(recorded) => actual.get(*field) == Some(recorded),
                            None => actual.get(*field).is_some_and(Value::is_null),
                        }
                    });
                let matches = if case["expected"].get("condition").is_some() {
                    unpivot_error_matches(case, &actual)
                        && (raw_strict
                            || matches!(
                                case["id"].as_str(),
                                Some("unpivot_missing_column_empty" | "unpivot_malformed_clause")
                            ))
                } else {
                    raw_strict
                };
                assert!(completed.insert((
                    case["id"].as_str().ok_or("missing ID")?.to_owned(),
                    case["ansi"].as_bool().ok_or("missing ANSI")?,
                    batch
                )));
                raw_strict_matches += usize::from(raw_strict);
                native_diagnostic_aliases += usize::from(matches && !raw_strict);
                eprintln!(
                    "UNPIVOT_OBSERVATION {}",
                    json!({
                        "id":case["id"],"ansi":case["ansi"],"batch":batch,
                        "provider_partitions":2,"target_partitions":2,
                        "reference":case,"actual":actual,"matches":matches,
                        "raw_strict_spark_matches":raw_strict,
                        "native_diagnostic_alias":matches && !raw_strict,
                    })
                );
                if !matches {
                    differences.push(format!(
                        "{} ANSI={} batch={batch}: {actual}",
                        case["id"], case["ansi"]
                    ));
                }
            }
        }
    }
    assert_eq!(completed, expected, "replay context inventory changed");
    assert!(differences.is_empty(), "{}", differences.join("\n"));
    assert_eq!(
        raw_strict_matches + native_diagnostic_aliases,
        completed.len()
    );
    eprintln!(
        "UNPIVOT_SUMMARY {}",
        json!({"observations":completed.len(),
        "raw_strict_spark_matches":raw_strict_matches,"native_diagnostic_aliases":native_diagnostic_aliases})
    );
    Ok(())
}

#[test]
fn unpivot_errors_require_the_recorded_cause_phase_and_no_fields() -> TestResult {
    let groups = load_spark_reference_groups()?;
    let errors = groups
        .iter()
        .flat_map(|(_, cases)| cases)
        .filter(|case| case["expected"].get("condition").is_some())
        .collect::<Vec<_>>();
    assert_eq!(errors.len(), 6);
    for case in errors {
        let error = match case["id"].as_str() {
            Some("unpivot_missing_column_empty") => {
                "External error: analysis error: attribute ObjectName([Identifier(\"missing\")]) is missing from the schema: cannot resolve attribute"
            }
            Some("unpivot_incompatible_types_empty") => {
                "External error: analysis error: [UNPIVOT_VALUE_DATA_TYPE_MISMATCH] Unpivot value columns must share a least common type, some types do not: Int32, Boolean"
            }
            Some("unpivot_malformed_clause") => {
                "External error: error in SQL parser: found label at 36:41 expected 'FOR'"
            }
            _ => return Err("unexpected error source".into()),
        };
        let actual = json!({"native_phase":"resolve","error":error,"optimized_error":error,
            "public_schema":null,"optimized_schema":null,"before_collect":null,
            "physical_schema":null,"rows":null,"rows_raw":null,"optimized_observation":null});
        assert!(unpivot_error_matches(case, &actual));
        for (field, wrong) in [
            ("native_phase", json!("collect")),
            ("error", json!(format!("{error}: unrelated"))),
            ("optimized_error", json!("unrelated error")),
            ("observer_error", json!("observer failed")),
            ("native_panic", json!("panic")),
        ] {
            let mut changed = actual.clone();
            changed[field] = wrong;
            assert!(
                !unpivot_error_matches(case, &changed),
                "accepted wrong {field}"
            );
        }
        for field in [
            "public_schema",
            "optimized_schema",
            "before_collect",
            "physical_schema",
            "rows",
            "rows_raw",
            "optimized_observation",
        ] {
            let mut changed = actual.clone();
            changed[field] = json!({"unexpected":"value"});
            assert!(!unpivot_error_matches(case, &changed), "accepted {field}");
            changed
                .as_object_mut()
                .ok_or("invalid observation")?
                .remove(field);
            assert!(
                !unpivot_error_matches(case, &changed),
                "accepted missing {field}"
            );
        }
        for pointer in [
            "/id",
            "/sql",
            "/expected/condition",
            "/spark_error_phase",
            "/spark_error_details/parameters",
        ] {
            let mut changed = case.clone();
            *changed
                .pointer_mut(pointer)
                .ok_or("missing error source field")? = json!("wrong");
            assert!(
                !unpivot_error_matches(&changed, &actual),
                "accepted wrong {pointer}"
            );
        }
        let mut changed = actual;
        changed["error"] = json!(format!("{error}: unrelated"));
        changed["optimized_error"] = changed["error"].clone();
        assert!(
            !unpivot_error_matches(case, &changed),
            "accepted matching unrelated diagnostics"
        );
    }
    Ok(())
}

#[tokio::test]
async fn unpivot_planning_preserves_native_unnest_filter_and_empty_results() -> TestResult {
    let groups = load_spark_reference_groups()?;
    let (metadata, cases) = groups.first().ok_or("missing source group")?;
    let sqls = [
        "SELECT unnest([1,2]) AS a,unnest([3]) AS b",
        "SELECT id,v FROM inputs WHERE v IS NOT NULL",
        "SELECT id,unnest([g,v]) AS value FROM empty_inputs",
    ];
    for batch in [1, 32] {
        let native = SessionContext::new_with_config(
            SessionConfig::new()
                .with_batch_size(batch)
                .with_target_partitions(2),
        );
        let configured = spark_session(batch);
        for session in [&native, &configured] {
            spark_inputs::register_recorded_inputs(session, metadata, batch)?;
        }
        let mut before = Vec::new();
        for after_spark in [false, true] {
            if after_spark {
                let probes = cases
                    .iter()
                    .filter(|case| {
                        matches!(
                            case["id"].as_str(),
                            Some("unpivot_exclude_mixed" | "unpivot_include_mixed")
                        )
                    })
                    .collect::<Vec<_>>();
                assert_eq!(probes.len(), 4);
                for case in probes {
                    let actual =
                        compare_exact_spark_query_stages_with_optimized(&configured, case).await?;
                    assert_eq!(actual["matches"], true, "{actual}");
                    assert_eq!(actual["optimized_matches"], true, "{actual}");
                }
            }
            for (index, sql) in sqls.iter().enumerate() {
                let expected = capture_native_query_observation(&native, sql).await?;
                let actual = capture_native_query_observation(&configured, sql).await?;
                assert!(expected["error"].is_null(), "{sql}: {expected}");
                assert_eq!(
                    actual, expected,
                    "batch={batch} after_spark={after_spark}: {sql}"
                );
                if after_spark {
                    assert_eq!(
                        actual, before[index],
                        "native behavior changed after Spark planning: {sql}"
                    );
                } else {
                    before.push(actual.clone());
                }
                if index == 0 {
                    assert_eq!(actual["rows"], json!([["1", "3"], ["2", null]]));
                } else if index == 2 {
                    assert_eq!(actual["rows"], json!([]));
                }
                for stage in ["public_schema", "before_collect", "physical_schema"] {
                    assert_eq!(
                        actual[stage]["fields"][1]["nullable"], true,
                        "{sql}: {actual}"
                    );
                }
            }
        }
    }
    Ok(())
}

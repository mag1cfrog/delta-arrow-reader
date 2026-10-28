use super::*;

#[test]
fn manifest_schema_matches_parser_shapes_and_table_count_limits()
-> Result<(), Box<dyn std::error::Error>> {
    let schema = tables_manifest_schema();
    let validator = jsonschema::validator_for(&schema)?;
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("tables.json");
    let baseline =
        json!({"format_version": 1, "tables": [{"alias": "orders", "location": "./orders"}]});
    let mut cases = vec![(baseline.clone(), true)];
    let mut uc_manifest = baseline.clone();
    uc_manifest["tables"][0]["location"] = json!("uc://main.sales.orders");
    uc_manifest["tables"][0]["authorization"] =
        json!({"source":"databricks", "databricks_profile":"work"});
    cases.push((uc_manifest, true));
    std::fs::write(
        directory.path().join("credentials.env"),
        "AWS_ACCESS_KEY_ID=secret-key\nAWS_SECRET_ACCESS_KEY=secret-password\nAWS_REGION=eu-west-1",
    )?;
    for reference in [
        "credentials.env".to_owned(),
        directory
            .path()
            .join("credentials.env")
            .to_str()
            .unwrap()
            .to_owned(),
    ] {
        let mut manifest = baseline.clone();
        manifest["tables"][0]["location"] = json!("s3://bucket/table");
        manifest["tables"][0]["authorization"] = json!({"source":"env-file", "env_file":reference});
        cases.push((manifest, true));
    }
    for version in ["0", "18446744073709551615"] {
        let mut manifest = baseline.clone();
        manifest["tables"][0]["table_version"] = json!(version);
        cases.push((manifest, true));
    }
    for (field, value) in [
        ("alias", json!("MixedCase")),
        ("alias", json!("orders\n")),
        ("location", json!(null)),
        ("table_version", json!(1)),
        ("table_version", json!(null)),
        ("table_version", json!("01")),
        ("table_version", json!("1\n")),
        ("authorization", json!(null)),
        ("authorization", json!([])),
        ("authorization", json!({"source":"unknown"})),
        ("authorization", json!({"source":"json"})),
        ("authorization", json!({"source":"databricks"})),
        ("authorization", json!({"source":"env-file"})),
        (
            "authorization",
            json!({"source":"env-file", "env_file":null}),
        ),
        ("authorization", json!({"source":"env-file", "env_file":""})),
        ("authorization", json!({"source":"env-file", "env_file":[]})),
        (
            "authorization",
            json!({"source":"env-file", "env_file":"credentials.env", "databricks_profile":"work"}),
        ),
        (
            "authorization",
            json!({"source":"env", "env_file":"credentials.env"}),
        ),
        (
            "authorization",
            json!({"source":"json", "env_file":"credentials.env", "storage_options_file":"options.json"}),
        ),
        (
            "authorization",
            json!({"source":"databricks", "env_file":"credentials.env", "databricks_profile":"work"}),
        ),
        (
            "authorization",
            json!({"source":"databricks", "databricks_profile":null}),
        ),
        (
            "authorization",
            json!({"source":"databricks", "databricks_profile":""}),
        ),
        (
            "authorization",
            json!({"source":"env", "databricks_profile":"work"}),
        ),
        (
            "authorization",
            json!({"source":"env", "access_key_id":"secret"}),
        ),
        (
            "authorization",
            json!({"source":"env", "storage_options_file":null}),
        ),
    ] {
        let mut manifest = baseline.clone();
        manifest["tables"][0][field] = value;
        cases.push((manifest, false));
    }
    for count in [0, super::super::MAX_TABLES, super::super::MAX_TABLES + 1] {
        let tables: Vec<_> = (0..count)
            .map(|index| json!({"alias":format!("table_{index}"), "location":"./table"}))
            .collect();
        cases.push((
            json!({"format_version":1,"tables":tables}),
            count == super::super::MAX_TABLES,
        ));
    }
    for (manifest, accepted) in cases {
        assert_eq!(validator.is_valid(&manifest), accepted, "{manifest}");
        std::fs::write(&path, manifest.to_string())?;
        assert_eq!(
            super::super::read_table_requests(path.to_str().unwrap(), &mut None).is_ok(),
            accepted,
            "{manifest}",
        );
    }
    Ok(())
}

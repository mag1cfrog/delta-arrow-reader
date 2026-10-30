use super::*;
use serde_json::json;

fn table_metadata_response() -> Value {
    json!({
        "table_id": "12345678-1234-1234-1234-123456789abc",
        "full_name": "main.sales.orders",
        "table_type": "MANAGED",
        "data_source_format": "DELTA",
        "storage_location": "s3://bucket/table",
        "securable_kind_manifest": {
            "securable_type": "TABLE",
            "securable_kind": "TABLE_DELTA",
            "capabilities": []
        }
    })
}

#[test]
fn admission_rejects_unsupported_and_mismatched_tables() {
    for kind in ["MANAGED", "EXTERNAL"] {
        let mut value = table_metadata_response();
        value["table_type"] = json!(kind);
        TableMetadataResponse::parse(&serde_json::to_vec(&value).unwrap(), "MAIN.sales.orders")
            .unwrap();
    }
    for (field, invalid) in [
        ("table_id", json!("not-a-uuid")),
        ("full_name", json!("other.schema.table")),
        ("table_type", json!("VIEW")),
        ("table_type", json!("MANAGED_SHALLOW_CLONE")),
        ("data_source_format", json!("ICEBERG")),
        ("row_filter", json!({})),
        ("columns", json!([{"mask": {}}])),
        ("browse_only", json!(true)),
        ("pipeline_id", json!("pipeline")),
        (
            "properties",
            json!({"delta.feature.catalogManaged": "unknown"}),
        ),
        (
            "properties",
            json!({"delta.coordinatedCommits.commitCoordinator-preview": "unity-catalog"}),
        ),
        (
            "securable_kind_manifest",
            json!({"securable_type": "TABLE", "securable_kind": "TABLE_VIEW"}),
        ),
        (
            "securable_kind_manifest",
            json!({
                "securable_type": "TABLE",
                "securable_kind": "TABLE_DELTA",
                "capabilities": [""]
            }),
        ),
    ] {
        let mut value = table_metadata_response();
        value[field] = invalid;
        assert!(
                TableMetadataResponse::parse(
                    &serde_json::to_vec(&value).unwrap(),
                    "main.sales.orders",
                )
                .is_err(),
                "{field}"
            );
    }
    for field in [
        "table_id",
        "data_source_format",
        "storage_location",
        "securable_kind_manifest",
    ] {
        let mut value = table_metadata_response();
        value.as_object_mut().unwrap().remove(field);
        assert!(
            matches!(
                TableMetadataResponse::parse(
                    &serde_json::to_vec(&value).unwrap(),
                    "main.sales.orders",
                ),
                Err(Error::InvalidDatabricksResponse)
            ),
            "{field}"
        );
    }
}

#[test]
fn capability_only_manifests_require_direct_read_support_and_preserve_policy_checks() {
    let mut value = table_metadata_response();
    value["securable_kind_manifest"] = json!({
        "capabilities": ["HAS_DIRECT_EXTERNAL_ENGINE_READ_SUPPORT", "HAS_STORAGE"]
    });
    TableMetadataResponse::parse(&serde_json::to_vec(&value).unwrap(), "main.sales.orders")
        .unwrap();
    for (field, invalid) in [
        ("table_type", json!("VIEW")),
        ("table_type", json!("MANAGED_SHALLOW_CLONE")),
        ("row_filter", json!({})),
        ("columns", json!([{"mask": {}}])),
        (
            "properties",
            json!({"delta.feature.catalogManaged": "unknown"}),
        ),
        (
            "securable_kind_manifest",
            json!({"capabilities": ["HAS_STORAGE"]}),
        ),
        ("securable_kind_manifest", json!({"capabilities": []})),
        (
            "securable_kind_manifest",
            json!({
                "securable_type": "VIEW",
                "capabilities": ["HAS_DIRECT_EXTERNAL_ENGINE_READ_SUPPORT"]
            }),
        ),
        (
            "securable_kind_manifest",
            json!({
                "securable_kind": "TABLE_VIEW",
                "capabilities": ["HAS_DIRECT_EXTERNAL_ENGINE_READ_SUPPORT"]
            }),
        ),
    ] {
        let mut invalid_response = value.clone();
        invalid_response[field] = invalid;
        assert!(
            TableMetadataResponse::parse(
                &serde_json::to_vec(&invalid_response).unwrap(),
                "main.sales.orders"
            )
            .is_err(),
            "{field}"
        );
    }
}

#[test]
fn catalog_managed_metadata_still_requires_a_managed_table_without_read_policies() {
    let mut value = table_metadata_response();
    value["properties"] = json!({"delta.feature.catalogManaged": "supported"});
    let metadata =
        TableMetadataResponse::parse(&serde_json::to_vec(&value).unwrap(), "main.sales.orders")
            .unwrap();
    assert!(metadata.is_catalog_managed());
    for (field, invalid) in [
        ("table_type", json!("EXTERNAL")),
        ("row_filter", json!({})),
        ("columns", json!([{"mask":{}}])),
    ] {
        let mut invalid_response = value.clone();
        invalid_response[field] = invalid;
        assert!(
            TableMetadataResponse::parse(
                &serde_json::to_vec(&invalid_response).unwrap(),
                "main.sales.orders"
            )
            .is_err()
        );
    }
}

#[test]
fn pipeline_admission_uses_original_policies_and_checks_the_published_identity() {
    for kind in ["MATERIALIZED_VIEW", "STREAMING_TABLE"] {
        let mut original = table_metadata_response();
        original["table_type"] = json!(kind);
        original["data_source_format"] = Value::Null;
        original["storage_location"] = Value::Null;
        original["pipeline_id"] = json!("pipeline-id");
        original["securable_kind_manifest"] = json!({
            "capabilities": ["HAS_DIRECT_EXTERNAL_ENGINE_READ_SUPPORT"]
        });
        let metadata = TableMetadataResponse::parse(
            &serde_json::to_vec(&original).unwrap(),
            "main.sales.orders",
        )
        .unwrap();
        assert!(metadata.is_pipeline_dataset());
        let published = json!({
            "table_id": original["table_id"], "catalog_name": "main", "schema_name": "sales", "name": "orders",
            "table_type": "MANAGED", "data_source_format": "DELTA", "storage_location": "s3://bucket/table/_external_metadata"
        });
        assert_eq!(
            metadata
                .parse_external_delta_location(&serde_json::to_vec(&published).unwrap())
                .unwrap()
                .as_url()
                .as_str(),
            "s3://bucket/table/_external_metadata/"
        );
        for (field, value) in [
            ("row_filter", json!({})),
            ("columns", json!([{"mask":{}}])),
            ("browse_only", json!(true)),
            ("table_type", json!("VIEW")),
            ("securable_kind_manifest", json!({"capabilities":[]})),
            ("data_source_format", json!("ICEBERG")),
            (
                "properties",
                json!({"delta.feature.catalogManaged":"supported"}),
            ),
        ] {
            let mut denied = original.clone();
            denied[field] = value;
            assert!(
                TableMetadataResponse::parse(
                    &serde_json::to_vec(&denied).unwrap(),
                    "main.sales.orders"
                )
                .is_err(),
                "{kind}: {field}"
            );
        }
        for (field, value) in [
            ("table_id", json!("12345678-1234-1234-1234-123456789abd")),
            ("name", json!("other")),
            ("catalog_name", json!("other")),
            ("schema_name", json!("other")),
            ("storage_location", json!("s3://bucket/table?secret=token")),
        ] {
            let mut wrong = published.clone();
            wrong[field] = value;
            assert!(
                matches!(
                    metadata.parse_external_delta_location(&serde_json::to_vec(&wrong).unwrap()),
                    Err(Error::InvalidDatabricksResponse)
                ),
                "{kind}: {field}"
            );
        }
        for (field, value) in [
            ("table_type", json!(kind)),
            ("data_source_format", Value::Null),
            ("storage_location", Value::Null),
        ] {
            let mut unavailable = published.clone();
            unavailable[field] = value;
            assert!(matches!(
                metadata.parse_external_delta_location(&serde_json::to_vec(&unavailable).unwrap()),
                Err(Error::PipelineExternalAccessUnavailable)
            ));
        }
    }
}

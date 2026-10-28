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
            json!({"delta.feature.catalogManaged": "supported"}),
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
    for field in ["table_id", "storage_location", "securable_kind_manifest"] {
        let mut value = table_metadata_response();
        value.as_object_mut().unwrap().remove(field);
        assert!(
                TableMetadataResponse::parse(
                    &serde_json::to_vec(&value).unwrap(),
                    "main.sales.orders",
                )
                .is_err(),
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
            json!({"delta.feature.catalogManaged": "supported"}),
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

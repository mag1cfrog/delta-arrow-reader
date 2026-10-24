//! Check pinned reads, repair, and complete saved results using independent fixture values.

use std::fs;

use serde_json::{Value, json};

use super::PackagedCli;
use crate::{
    Result,
    artifacts::file_sha256,
    scan::ipc::{IpcTable, check_completed_query},
};

impl PackagedCli<'_> {
    pub(super) fn check_read_and_repair(&mut self) -> Result<()> {
        self.read_json(&["capabilities"], 2048)?;
        self.read_json(&["capabilities", "--command", "inspect"], 4096)?;
        let summary = self.read_json(&["inspect", "table", "--view", "summary"], 4096)?;
        assert_eq!(summary["table_version"], "0");
        assert_eq!(summary["kind"], "inspection_summary");
        assert!(summary.get("fields").is_none());
        let names = self.read_json(
            &[
                "inspect",
                "table",
                "--view",
                "summary",
                "--table-version",
                "0",
                "--column-prefix",
                "id",
            ],
            4096,
        )?;
        assert_eq!(names["column_names"], json!(["id"]));
        let fields = self.read_json(
            &[
                "inspect",
                "table",
                "--view",
                "summary",
                "--table-version",
                "0",
                "--column",
                "id",
                "--column",
                "amount",
                "--column",
                "day",
            ],
            4096,
        )?;
        assert_eq!(
            fields["fields"],
            json!([
                {"name": "id", "data_type": "Int64", "nullable": true},
                {"name": "amount", "data_type": {"Decimal128": [18, 4]}, "nullable": true},
                {"name": "day", "data_type": "Date32", "nullable": true},
            ])
        );
        assert!(!fields.to_string().contains("private-metadata"));
        self.read_json(&["capabilities", "--schema", "predicate"], 2048)?;
        let filter = self.directory.join("predicate.json");
        fs::write(
            &filter,
            r#"{"op":"eq","column":"id","value":{"type":"int64","value":9007199254740993}}"#,
        )?;
        let preview = [
            "preview",
            "table",
            "--table-version",
            "0",
            "--column",
            "id",
            "--column",
            "amount",
            "--column",
            "day",
            "--predicate-file",
            "predicate.json",
            "--limit",
            "3",
        ];
        let rejected = self.check_failure(&preview, 2, "invalid_input_json")?;
        assert_eq!(rejected["recovery"], "fix_input");
        assert_eq!(rejected["field_path"], "/value/value");
        let scalar = self.read_json(
            &["capabilities", "--schema", "predicate", "--detail", "int64"],
            2048,
        )?;
        assert_eq!(
            scalar["definition"]["properties"]["value"]["type"],
            "string"
        );
        fs::write(
            &filter,
            r#"{"op":"eq","column":"id","value":{"type":"int64","value":"9007199254740993"}}"#,
        )?;
        let sample = self.read_json(&preview, 8192)?;
        assert_eq!(sample["completion"], "sample");
        assert_eq!(sample["stop_reason"], "end_of_input");
        assert_eq!(sample["has_more"], false);
        assert_eq!(
            sample["bindings"],
            json!([{"alias": "t", "table_version": "0"}])
        );
        assert_eq!(sample["rows"], json!([["9007199254740993", "12345", "0"]]));
        assert_eq!(sample["schema"]["fields"], fields["fields"]);
        for detail in ["decimal", "rows"] {
            self.read_json(
                &["capabilities", "--schema", "rows", "--detail", detail],
                2048,
            )?;
        }

        fs::write(
            self.directory.join("query.sql"),
            "SELECT id, amount, day FROM t ORDER BY id",
        )?;
        let query = [
            "query",
            "table",
            "--table-version",
            "0",
            "--sql-file",
            "query.sql",
            "--format",
            "json",
        ];
        let rows = self.read_json(&query, 8192)?;
        assert_eq!(rows["completion"], "complete");
        assert_eq!(rows["scope"], "query");
        assert_eq!(
            rows["rows"],
            json!([
                ["9007199254740993", "12345", "0"],
                ["9007199254740994", null, "1"]
            ])
        );
        assert_eq!(rows["row_count"], "2");
        assert_eq!(rows["schema"]["fields"], fields["fields"]);
        assert_eq!(
            rows["bindings"],
            json!([{"alias":"t", "table_version":"0"}])
        );
        let plan = self.read_json(
            &[
                "explain",
                "table",
                "--table-version",
                "0",
                "--sql-file",
                "query.sql",
            ],
            4096,
        )?;
        assert_eq!(plan["execution"], "not_started");
        assert_eq!(plan["bindings"], rows["bindings"]);
        let detail = self.read_json(
            &[
                "explain",
                "table",
                "--table-version",
                "0",
                "--sql-file",
                "query.sql",
                "--node",
                "0",
                "--field",
                "0",
                "--scan",
                "0",
                "--statistics",
            ],
            4096,
        )?;
        assert!(detail.to_string().contains("Int64"));
        assert!(!detail.to_string().contains("private-metadata"));
        let saved = self.read_json(
            &[query.as_slice(), &["--output", "result.json"]].concat(),
            8192,
        )?;
        let manifest = self.read_verified_manifest(&saved)?;
        assert_eq!(manifest["rows"], "2");
        assert_eq!(manifest["bindings"], rows["bindings"]);
        assert_eq!(
            serde_json::from_slice::<Value>(&fs::read(self.directory.join("result.json"))?)?,
            rows
        );
        let original = fs::read(self.directory.join("result.json"))?;
        let original_manifest = fs::read(self.directory.join("result.json.manifest.json"))?;
        self.check_failure(
            &[query.as_slice(), &["--output", "result.json"]].concat(),
            3,
            "artifact_io",
        )?;
        assert_eq!(fs::read(self.directory.join("result.json"))?, original);
        assert_eq!(
            fs::read(self.directory.join("result.json.manifest.json"))?,
            original_manifest
        );

        fs::write(
            self.directory.join("tables.json"),
            r#"{"format_version":1,"tables":[{"alias":"left_table","location":"table","table_version":"0"},{"alias":"right_table","location":"table","table_version":"0"}]}"#,
        )?;
        fs::write(
            self.directory.join("join.sql"),
            "WITH matches AS (SELECT l.day FROM left_table l JOIN right_table r ON l.id = r.id) SELECT day, COUNT(*) AS n FROM matches GROUP BY day ORDER BY day",
        )?;
        let joined = self.read_json(
            &[
                "query",
                "--tables-file",
                "tables.json",
                "--sql-file",
                "join.sql",
                "--format",
                "json",
                "--memory-limit-bytes",
                "268435456",
            ],
            8192,
        )?;
        assert_eq!(joined["rows"], json!([["0", "1"], ["1", "1"]]));
        assert_eq!(
            joined["bindings"],
            json!([{"alias": "left_table", "table_version": "0"}, {"alias": "right_table", "table_version": "0"}])
        );
        let output = self.invoke(
            &[
                "query",
                "table",
                "--table-version",
                "0",
                "--sql-file",
                "query.sql",
            ],
            0,
            1024 * 1024,
        )?;
        let expected_query = self
            .expected_table
            .project(&[0, 1, 2], &self.directory.join("table"))?;
        check_completed_query(&output, &[("t", 0)])?.check_matches(&expected_query, None)?;
        let spill = self.directory.join("spill");
        fs::create_dir(&spill)?;
        fs::write(spill.join("keep"), b"existing file")?;
        let spilling = self.read_json(
            &[
                query.as_slice(),
                &["--spill-dir", "spill", "--spill-limit-bytes", "1048576"],
            ]
            .concat(),
            8192,
        )?;
        assert_eq!(spilling, rows);
        assert_eq!(fs::read(spill.join("keep"))?, b"existing file");
        assert_eq!(fs::read_dir(spill)?.count(), 1);
        Ok(())
    }

    pub(super) fn check_output_boundaries(&mut self) -> Result<()> {
        let selected = self.read_json(
            &[
                "inspect", "table", "--view", "summary", "--column", "nested",
            ],
            4096,
        )?;
        assert!(selected["fields"][0]["data_type"].is_object());
        let full = self.read_json(&["inspect", "table"], 1024 * 1024)?;
        assert_eq!(
            full["schema"]["fields"]
                .as_array()
                .ok_or("missing full schema")?
                .len(),
            1000
        );
        assert!(full.to_string().contains("private-metadata"));
        for args in [
            vec![
                "inspect",
                "table",
                "--view",
                "summary",
                "--column-prefix",
                "column_",
            ],
            vec![
                "inspect",
                "table",
                "--view",
                "summary",
                "--column",
                "id",
                "--include-metadata",
            ],
        ] {
            self.check_failure(&args, 1, "output_limit")?;
        }
        self.check_failure(
            &["preview", "table", "--column", "large"],
            1,
            "output_limit",
        )?;
        fs::write(self.directory.join("large.sql"), "SELECT large FROM t")?;
        let query = [
            "query",
            "table",
            "--table-version",
            "0",
            "--sql-file",
            "large.sql",
            "--format",
            "json",
        ];
        let failure = self.check_failure(&query, 1, "output_limit")?;
        assert_eq!(
            failure["bindings"],
            json!([{"alias": "t", "table_version": "0"}])
        );
        let receipt = self.read_json(
            &[query.as_slice(), &["--output", "large.json"]].concat(),
            8192,
        )?;
        let manifest = self.read_verified_manifest(&receipt)?;
        assert_eq!(manifest["rows"], "2");
        let saved_rows: Value =
            serde_json::from_slice(&fs::read(self.directory.join("large.json"))?)?;
        assert_eq!(
            saved_rows["rows"],
            json!([["large-cell-".repeat(4096)], ["large-cell-".repeat(4096)]])
        );
        assert!(fs::metadata(self.directory.join("large.json"))?.len() > 8192);
        assert!(!receipt.to_string().contains("large-cell"));
        assert!(!manifest.to_string().contains("large-cell"));
        fs::write(self.directory.join("wide.sql"), "SELECT * FROM t")?;
        self.check_failure(
            &["explain", "table", "--sql-file", "wide.sql", "--field", "3"],
            2,
            "invalid_cli_argument",
        )?;
        let positions: Vec<_> = (0..1000)
            .filter(|&index| index != 3)
            .map(|index| index.to_string())
            .collect();
        let mut fields = vec!["explain", "table", "--sql-file", "wide.sql"];
        for position in &positions {
            fields.extend(["--field", position]);
        }
        self.check_failure(&fields, 1, "output_limit")?;
        self.read_json(
            &["explain", "table", "--sql-file", "wide.sql", "--field", "0"],
            4096,
        )?;
        let receipt = self.read_json(
            &[
                "scan",
                "table",
                "--table-version",
                "0",
                "--output",
                "complete.arrow",
            ],
            8192,
        )?;
        let manifest = self.read_verified_manifest(&receipt)?;
        let bytes = fs::read(self.directory.join("complete.arrow"))?;
        let arrow = IpcTable::read_stream(&bytes, &self.directory.join("complete.arrow"))?;
        arrow.check_matches(&self.expected_table, None)?;
        assert_eq!(manifest["scope"], "scan");
        assert_eq!(manifest["has_more"], false);
        for (option, value, code) in [
            ("--max-rows", "1", "output_limit"),
            ("--max-output-bytes", "1", "output_limit"),
            ("--memory-limit-bytes", "1", "sql_execution"),
        ] {
            let args = [
                "query",
                "table",
                "--table-version",
                "0",
                "--sql-file",
                "query.sql",
                "--format",
                "json",
                option,
                value,
            ];
            self.check_failure(&args, 1, code)?;
            self.check_failure(
                &[args.as_slice(), &["--output", "failed.json"]].concat(),
                1,
                code,
            )?;
            assert!(!self.directory.join("failed.json").exists());
            assert!(!self.directory.join("failed.json.manifest.json").exists());
        }
        fs::write(self.directory.join("forbidden.sql"), "DELETE FROM t")?;
        self.check_failure(
            &[
                "query",
                "table",
                "--sql-file",
                "forbidden.sql",
                "--format",
                "json",
            ],
            2,
            "invalid_sql_input",
        )?;
        // Configuration errors must not start implicit credential discovery or echo file contents.
        fs::write(
            self.directory.join("credentials.json"),
            "{\"token\":\"credential-canary\",",
        )?;
        self.check_failure(
            &[
                "inspect",
                "table",
                "--storage-options-file",
                "credentials.json",
            ],
            2,
            "invalid_input_json",
        )?;
        fs::write(
            self.directory.join("credentials.env"),
            "AWS_ACCESS_KEY_ID=credential-canary\nAWS_SECRET_ACCESS_KEY=credential-canary\n",
        )?;
        for args in [
            vec!["inspect", "table", "--env-file", "credentials.env"],
            vec!["inspect", "s3://fixture/table", "--env-file", "missing.env"],
        ] {
            let output = self.invoke(&args, 2, 0)?;
            let error: Value = serde_json::from_slice(&output.stderr)?;
            assert_eq!(error["recovery"], "fix_input");
        }
        Ok(())
    }

    pub(super) fn read_verified_manifest(&self, receipt: &Value) -> Result<Value> {
        assert_eq!(receipt["kind"], "result_artifact");
        assert_eq!(receipt.as_object().ok_or("invalid receipt")?.len(), 4);
        let result = self
            .directory
            .join(receipt["result"].as_str().ok_or("missing result path")?);
        let manifest_path = self.directory.join(
            receipt["manifest"]
                .as_str()
                .ok_or("missing manifest path")?,
        );
        let manifest: Value = serde_json::from_slice(&fs::read(manifest_path)?)?;
        assert_eq!(manifest["result"]["sha256"], file_sha256(&result)?);
        assert_eq!(
            manifest["output_bytes"],
            fs::metadata(result)?.len().to_string()
        );
        assert_eq!(manifest["completion"], "complete");
        assert_eq!(
            manifest["bindings"],
            json!([{"alias":"t", "table_version":"0"}])
        );
        assert_eq!(manifest["stop_reason"], "end_of_input");
        assert_eq!(manifest["has_more"], false);
        assert_eq!(manifest["build"]["cli"], self.version);
        assert_eq!(manifest["replay"]["options"]["table-version"], "0");
        assert!(!manifest.to_string().contains("credential-canary"));
        Ok(manifest)
    }
}

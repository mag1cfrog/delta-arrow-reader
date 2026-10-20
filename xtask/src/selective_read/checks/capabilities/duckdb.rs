use super::*;
use std::os::unix::fs::PermissionsExt;

impl Capabilities {
    pub(super) fn duckdb(&mut self, python: &Path, observations: &mut Vec<Value>) -> Result<()> {
        let mut payload =
            self.check
                .request("li.clustered.eq2-in20", "open", "diagnostic", "streaming")?;
        payload["canonical_sql"] = json!(
            "SELECT CASE WHEN i=999999 THEN error('late-stream-probe') ELSE i END AS id FROM range(1000000) t(i)"
        );
        let record = self.probe(&payload, "late-stream-error")?;
        require(
            record["status"] == "operational_failure"
                && text(&record["failure_reason"])?.contains("late-stream-probe")
                && (1..1_000_000).contains(&count(&record["partial_query"]["output_rows"])?),
            "late DuckDB failure did not follow streamed rows",
        )?;
        observations.push(
            json!({"path":"late-stream-error","status":"passed","feature":"streaming",
            "rows_before_error":record["partial_query"]["output_rows"]}),
        );
        for (name, key, value) in [
            ("unknown-field", "surprise", json!(true)),
            ("negative-version", "snapshot_version", json!(-1)),
            ("bad-budget", "resource_budget", json!({})),
            (
                "credential-url",
                "table_uri",
                json!("s3://user:secret@bucket/table"),
            ),
        ] {
            let mut invalid = payload.clone();
            invalid[key] = value;
            require(
                self.probe(&invalid, name)?["status"] == "invalid_input",
                "invalid DuckDB request accepted",
            )?;
            observations
                .push(json!({"path":name,"status":"passed","feature":"invalid request rejected"}));
        }
        for sql in [
            "SELECT 1; SELECT 2",
            "CREATE TABLE bad AS SELECT 1",
            "INSTALL delta",
        ] {
            let name = format!("sql-rejected-{}", observations.len());
            let mut invalid = payload.clone();
            invalid["canonical_sql"] = json!(sql);
            let record = self.probe(&invalid, &name)?;
            require(
                record["status"] == "operational_failure" && record["phase"] == "setup",
                "DuckDB accepted SQL outside the read-only contract",
            )?;
            observations
                .push(json!({"path":name,"status":"passed","feature":"read-only SQL","sql":sql}));
        }
        // A missing proof must stop the query before the unreachable endpoint is used.
        let secrets = BTreeMap::from([
            ("AWS_ACCESS_KEY_ID".into(), "probe-access".into()),
            ("AWS_SECRET_ACCESS_KEY".into(), "probe-secret".into()),
            ("AWS_SESSION_TOKEN".into(), "probe-token".into()),
            ("AWS_REGION".into(), "us-east-1".into()),
            ("AWS_ENDPOINT_URL".into(), "http://127.0.0.1:9".into()),
        ]);
        let mut remote = payload.clone();
        remote["table_uri"] = json!("s3://probe-bucket/table");
        remote["purpose"] = json!("timing");
        remote["correctness_file"] = Value::Null;
        let record = probe(
            &self.binary,
            &remote,
            &self.check.output.join("s3-secret-setup"),
            Some(&secrets),
            None,
        )?;
        require(
            record["status"] == "validation_failed"
                && record["phase"] == "correctness_gate"
                && array(&record["queries"])?.is_empty()
                && record["settings"]["storage"]["url_style"] == "path",
            "DuckDB storage setup or proof gate changed",
        )?;
        let encoded = record.to_string();
        for key in [
            "AWS_ACCESS_KEY_ID",
            "AWS_SECRET_ACCESS_KEY",
            "AWS_SESSION_TOKEN",
        ] {
            require(
                !encoded.contains(&secrets[key]),
                "credential leaked into capability record",
            )?;
        }
        observations.push(json!({"path":"s3-secret-setup","status":"passed","feature":"S3 configuration before table I/O"}));

        let changed = self.check.output.join("changed-extension");
        fs::create_dir(&changed)?;
        let build = read_json(&self.binary.with_file_name("build.json"))?;
        let name = self
            .binary
            .file_name()
            .ok_or("missing reader executable name")?;
        for source in [
            self.binary.clone(),
            self.binary.with_file_name("lock.json"),
            self.binary.with_file_name("build.json"),
        ]
        .into_iter()
        .chain(
            super::super::super::object(&build["bundled_sha256"])?
                .keys()
                .map(|name| self.binary.with_file_name(name)),
        ) {
            fs::copy(
                &source,
                changed.join(source.file_name().ok_or("missing bundled name")?),
            )?;
        }
        fs::write(
            changed.join("httpfs.duckdb_extension"),
            b"not the locked extension",
        )?;
        let record = probe(
            &changed.join(name),
            &payload,
            &self.check.output.join("extension-rejected"),
            None,
            None,
        )?;
        require(
            record["status"] == "operational_failure"
                && record["phase"] == "setup"
                && text(&record["failure_reason"])?.contains("extension checksum mismatch")
                && array(&record["queries"])?.is_empty(),
            "modified DuckDB extension was not rejected at setup",
        )?;
        observations.push(json!({"path":"extension-rejected","status":"passed","feature":"extension hash verification"}));

        let broken = self.check.output.join("missing-dependency");
        fs::write(
            &broken,
            format!(
                "#!{}\nimport __selective_read_missing_dependency__\n",
                std::path::absolute(python)?.display()
            ),
        )?;
        fs::set_permissions(&broken, fs::Permissions::from_mode(0o755))?;
        let record = self
            .check
            .invoke(&broken, &payload, "import-failure", None)?;
        require(
            record["status"] == "operational_failure"
                && text(&record["failure_reason"])?.contains("ModuleNotFoundError"),
            "Python startup failure lost its classification",
        )?;
        observations.push(json!({"path":"import-failure","status":"passed","feature":"startup failure classification"}));
        let mut invalid = payload;
        invalid["surprise"] = json!(true);
        let record = self
            .check
            .invoke(&self.binary, &invalid, "launcher-invalid-input", None)?;
        require(
            record["status"] == "invalid_input",
            "launcher lost input rejection",
        )?;
        observations.push(json!({"path":"launcher-invalid-input","status":"passed","feature":"input rejection classification"}));

        let reference = self
            .check
            .output
            .join("public-cases/reference-li.clustered.eq2-in20");
        let mut payload = self.check.request(
            "li.clustered.eq2-in20",
            "open",
            "validation",
            "wrong-values",
        )?;
        payload["canonical_sql"] = json!(text(&payload["canonical_sql"])?.replacen(
            "SELECT l_orderkey,",
            "SELECT l_orderkey + 1 AS l_orderkey,",
            1
        ));
        require(
            text(&payload["canonical_sql"])?.contains("l_orderkey + 1"),
            "corruption probe did not change the query",
        )?;
        let record = self.probe(&payload, "wrong-values")?;
        success(&record)?;
        // Preserve plausible provenance so the independent value comparison must catch the corruption.
        let output = self.check.output.join("wrong-values");
        let identity = output.join("reader/query-0.identity.json");
        let mut changed = read_json(&identity)?;
        changed["canonical_sql_sha256"] =
            read_json(&reference.join("reference.json"))?["canonical_sql_sha256"].clone();
        fs::write(&identity, serde_json::to_vec(&changed)?)?;
        let record = self
            .check
            .validate(record, &payload, &output, Some(&reference))?;
        require(
            record["status"] == "validation_failed"
                && ["reference", "row"].iter().any(|s| {
                    record["failure_reason"]
                        .as_str()
                        .is_some_and(|reason| reason.contains(s))
                }),
            "oracle accepted incorrect projected values",
        )?;
        observations.push(json!({"path":"wrong-values","status":"passed","feature":"wrong values rejected","reason":record["failure_reason"]}));
        Ok(())
    }
}

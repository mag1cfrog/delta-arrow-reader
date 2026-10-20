use super::*;

impl Capabilities {
    pub(super) fn polars(&mut self, harness: &Path, observations: &mut Vec<Value>) -> Result<()> {
        let mut base =
            self.check
                .request("li.clustered.eq2-in20", "open", "validation", "probe")?;
        base["table_uri"] = json!(uri(&self.check.output.join("no-dv"))?);
        base["fixture_manifest_sha256"] = json!(digest(&self.corpus.join("manifest.json"))?);
        base["case_id"] = json!("probe.polars-types");
        for (sql, semantics) in [
            (
                "SELECT label, id, value FROM bench WHERE value IS NULL OR label IS NULL",
                "nulls",
            ),
            (
                "SELECT id, value, label FROM bench WHERE id IN (2, 2, NULL)",
                "duplicate",
            ),
        ] {
            let name = format!("types-{}", observations.len());
            let mut payload = base.clone();
            payload["canonical_sql"] = json!(sql);
            let record = self.probe(&payload, &name)?;
            self.check.worker.call(&json!({"action":"capability-export","record":record,
                "output":self.check.output.join(&name),"deleted":false,"ids":null,"semantics":semantics}))?;
            require(
                !text(&record["identity"]["native_expression_sha256"])?.is_empty(),
                "missing Polars expression identity",
            )?;
            observations.push(
                json!({"path":name,"status":"passed","feature":"null/IN/projection semantics"}),
            );
        }
        self.check
            .worker
            .call(&json!({"action":"capability-stream","output":self.check.output}))?;
        let mut payload = base;
        payload["table_uri"] = json!(uri(&self.check.output.join("stream-table"))?);
        payload["purpose"] = json!("diagnostic");
        payload["canonical_sql"] = json!("SELECT id FROM bench WHERE CAST(label AS BIGINT) > 0");
        let record = self.probe(&payload, "late-stream-error")?;
        require(
            record["status"] == "operational_failure"
                && text(&record["failure_reason"])?.contains("conversion")
                && (1..32 * 8192).contains(&count(&record["partial_query"]["output_rows"])?),
            "late Polars failure did not follow streamed batches",
        )?;
        observations.push(
            json!({"path":"late-stream-error","status":"passed","feature":"streaming",
            "rows_before_error":record["partial_query"]["output_rows"]}),
        );

        let log = fs::File::create_new(self.check.output.join("native-probes.log"))?;
        let mut process = Process(
            Command::new(self.binary.with_file_name("venv/bin/python"))
                .args(["-I", "-B"])
                .arg(fs::canonicalize(harness.join("runners/polars/probe.py"))?)
                .arg(&self.binary)
                .arg(&self.check.output)
                .stdout(log.try_clone()?)
                .stderr(log)
                .stdin(Stdio::null())
                .process_group(0)
                .spawn()?,
        );
        require(
            wait_with_timeout(&mut process.0, Duration::from_secs(600))?.success(),
            "native Polars reuse probe failed; inspect native-probes.log",
        )?;
        observations.push(
            json!({"path":"native-reuse.json","status":"passed","feature":"native snapshot reuse"}),
        );

        let payload = self.check.request(
            "li.clustered.eq2-in20",
            "open",
            "diagnostic",
            "failure-probe",
        )?;
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
            let record = self.check.invoke(&self.binary, &invalid, name, None)?;
            require(
                record["status"] == "invalid_input",
                "invalid Polars request accepted",
            )?;
            observations
                .push(json!({"path":name,"status":"passed","feature":"invalid request rejected"}));
        }
        for sql in [
            "SELECT id FROM bench; SELECT id FROM bench",
            "DELETE FROM bench",
            "SELECT id FROM other",
        ] {
            let name = format!("sql-rejected-{}", observations.len());
            let mut invalid = payload.clone();
            invalid["canonical_sql"] = json!(sql);
            let record = self.probe(&invalid, &name)?;
            require(
                record["status"] == "operational_failure" && array(&record["queries"])?.is_empty(),
                "Polars accepted unsupported SQL",
            )?;
            observations.push(
                json!({"path":name,"status":"passed","feature":"unsupported query rejected"}),
            );
        }
        let env = BTreeMap::from([
            ("AWS_ACCESS_KEY_ID".into(), "probe-access".into()),
            ("AWS_SECRET_ACCESS_KEY".into(), "probe-secret".into()),
            ("AWS_SESSION_TOKEN".into(), "probe-token".into()),
            ("AWS_ENDPOINT_URL".into(), "http://127.0.0.1:9".into()),
            ("POLARS_MAX_THREADS".into(), "1".into()),
            ("POLARS_VERBOSE".into(), "1".into()),
        ]);
        let mut remote = payload;
        remote["table_uri"] = json!("s3://probe-bucket/table");
        remote["purpose"] = json!("timing");
        let record = probe(
            &self.binary,
            &remote,
            &self.check.output.join("s3-configuration"),
            Some(&env),
        )?;
        require(
            record["status"] == "validation_failed"
                && record["phase"] == "correctness_gate"
                && record["settings"]["thread_pool_size"] == 8
                && record["settings"]["storage"]["url_style"] == "path",
            "Polars storage setup or frozen thread count changed",
        )?;
        let encoded = record.to_string();
        for key in [
            "AWS_ACCESS_KEY_ID",
            "AWS_SECRET_ACCESS_KEY",
            "AWS_SESSION_TOKEN",
        ] {
            require(
                !encoded.contains(&env[key]),
                "credential leaked into Polars record",
            )?;
        }
        observations.push(json!({"path":"s3-configuration","status":"passed","feature":"storage setup and frozen environment"}));

        let public = self
            .check
            .output
            .join("public-cases/polars-li.clustered.eq2-in20-open-validation");
        let mut proof = read_json(&public.join("correctness.json"))?;
        proof["checks"][0]["native_expression_sha256"] = json!("0".repeat(64));
        let wrong = self.check.output.join("stale-expression.json");
        save(&wrong, &proof)?;
        let mut payload = self.check.request(
            "li.clustered.eq2-in20",
            "open",
            "timing",
            "stale-expression",
        )?;
        payload["correctness_file"] = json!(wrong);
        let record = self
            .check
            .invoke(&self.binary, &payload, "expression-rejected", None)?;
        require(
            record["status"] == "validation_failed" && array(&record["queries"])?.is_empty(),
            "stale Polars expression proof reached query execution",
        )?;
        observations.push(json!({"path":"expression-rejected","status":"passed","feature":"native expression certificate binding"}));

        let corrupt = self.check.output.join("wrong-values.arrow");
        self.check.worker.call(&json!({"action":"capability-corrupt","input":public.join("reader/query-0.arrow"),"output":corrupt}))?;
        let mut identity = read_json(&public.join("reader/query-0.identity.json"))?;
        identity["result_sha256"] = json!(digest(&corrupt)?);
        let identity_path = self.check.output.join("wrong-values.identity.json");
        save(&identity_path, &identity)?;
        let checked = self.check.worker.call(&json!({"action":"check-export",
            "reference":self.check.output.join("public-cases/reference-li.clustered.eq2-in20"),
            "fixtures":self.check.fixtures,"result":corrupt,"identity":identity_path}));
        let reason = checked
            .err()
            .ok_or("oracle accepted incorrect Polars values")?
            .to_string();
        require(
            reason.contains("reference") || reason.contains("row"),
            "wrong-value check failed before comparing values",
        )?;
        observations.push(json!({"path":"wrong-values.arrow","status":"passed","feature":"wrong values rejected"}));
        Ok(())
    }
}

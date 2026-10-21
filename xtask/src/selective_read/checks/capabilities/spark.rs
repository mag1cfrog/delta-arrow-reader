use super::*;

fn timing(record: &Value, reuse: bool) -> Result<()> {
    success(record)?;
    require(
        record["correctness"]["status"] == "passed",
        "Spark timing lost its correctness gate",
    )?;
    let queries = array(&record["queries"])?;
    require(
        queries.len() == if reuse { 2 } else { 1 },
        "wrong Spark timing query count",
    )?;
    let mut total = 0u64;
    for query in queries {
        let completion = count(&query["completion_ns"])?;
        require(
            completion > 0 && query["first_batch_ns"].is_null(),
            "invalid Spark query clocks",
        )?;
        total = total
            .checked_add(completion)
            .ok_or("Spark clock overflow")?;
    }
    if reuse {
        require(
            record["initialization_plus_all_queries_ns"]
                == count(&record["initialization_ns"])?
                    .checked_add(total)
                    .ok_or("Spark clock overflow")?,
            "inconsistent Spark reuse clocks",
        )?;
    } else {
        require(
            record["open_query_ns"] == queries[0]["completion_ns"],
            "inconsistent Spark open clock",
        )?;
    }
    count(&record["cleanup_ns"])?;
    require(
        count(&record["startup_ns"])? > 0 && count(&record["session_elapsed_ns"])? > 0,
        "missing Spark startup or session clock",
    )
}

impl Capabilities {
    pub(super) fn spark(&mut self, observations: &mut Vec<Value>) -> Result<()> {
        let base = self
            .check
            .request("li.clustered.eq2-in20", "open", "diagnostic", "invalid")?;
        for (name, key, value) in [
            ("unknown-field", "surprise", json!(true)),
            ("negative-version", "snapshot_version", json!(-1)),
            ("bad-budget", "resource_budget", json!({})),
            (
                "credential-uri",
                "table_uri",
                json!("s3://user:secret@bucket/table"),
            ),
            ("timing-rejected", "purpose", json!("timing")),
            ("campaign-rejected", "campaign_id", json!("formal")),
            (
                "sql-rejected",
                "canonical_sql",
                json!("SELECT * FROM bench; DROP TABLE bench"),
            ),
        ] {
            let mut invalid = base.clone();
            invalid[key] = value;
            let record = self.probe(&invalid, name)?;
            require(
                record["status"] == "invalid_input" && !text(&record["failure_reason"])?.is_empty(),
                "Spark accepted an invalid or historical formal request",
            )?;
            observations.push(
                json!({"path":name,"status":"passed","feature":"invalid/formal request rejected"}),
            );
        }
        let mut base =
            self.check
                .request("li.clustered.eq2-in20", "open", "validation", "probe")?;
        base["comparison_revision"] = json!(6);
        base["sampling_stage"] = json!("pilot");
        let corpus_hash = json!(digest(&self.corpus.join("manifest.json"))?);
        base["workload_manifest_sha256"] = corpus_hash.clone();
        base["fixture_manifest_sha256"] = corpus_hash;
        base["case_id"] = json!("probe.spark-revision6");
        base["table_uri"] = json!(uri(&self.corpus.join("deletion_vectors/table"))?);
        base["snapshot_version"] = json!(1);
        base["canonical_sql"] = json!("SELECT id, value, label FROM bench");
        base["workload_contract"] = json!({"comparison_revision":6,"sampling_stage":"pilot",
            "scope":"capability", "cases":[{"case_id":base["case_id"],
                "canonical_sql":base["canonical_sql"],"snapshot_version":base["snapshot_version"],
                "fixture_manifest_sha256":base["fixture_manifest_sha256"]}]});
        for mode in ["open", "reuse"] {
            let mut payload = base.clone();
            payload["execution_mode"] = json!(mode);
            payload["run_id"] = json!(format!("{mode}-validation"));
            let name = format!("revision6-{mode}");
            let record = self.probe(&payload, &name)?;
            let rows = self.exported(&record, &name, true, None)?;
            let queries = array(&record["queries"])?;
            require(
                queries.len() == if mode == "open" { 1 } else { 2 },
                "wrong revision 6 Spark export count",
            )?;
            // This proof authorizes only the exactly checked bounded corpus.
            let mut check = record["identity"].clone();
            check["status"] = json!("passed");
            check["output_rows"] = rows;
            let certificate = self
                .check
                .output
                .join(format!("{mode}-bounded-correctness.json"));
            save(
                &certificate,
                &json!({"status":"passed","checks":vec![check;queries.len()]}),
            )?;
            payload["purpose"] = json!("timing");
            payload["correctness_file"] = json!(certificate);
            timing(
                &self.probe(&payload, &format!("{mode}-timing"))?,
                mode == "reuse",
            )?;
            for (failure, key, value) in [
                ("missing-proof", "correctness_file", Value::Null),
                ("changed-snapshot", "snapshot_version", json!(0)),
                (
                    "changed-sql",
                    "canonical_sql",
                    json!("SELECT id FROM bench"),
                ),
            ] {
                let mut invalid = payload.clone();
                invalid[key] = value;
                let record = self.probe(&invalid, &format!("{mode}-{failure}"))?;
                require(
                    record["status"] == "validation_failed"
                        && array(&record["queries"])?.is_empty(),
                    "stale Spark correctness proof reached a query",
                )?;
            }
            for name in [
                name,
                format!("{mode}-timing"),
                format!("{mode}-missing-proof"),
                format!("{mode}-changed-snapshot"),
                format!("{mode}-changed-sql"),
            ] {
                observations.push(json!({"path":name,"status":"passed","feature":"revision 6 exact values, clocks and proof identity"}));
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn spark_clocks_require_both_reuse_queries_and_account_for_initialization() -> Result<()> {
        let value = json!({"status":"success","correctness":{"status":"passed"},
            "queries":[{"completion_ns":20,"first_batch_ns":null},{"completion_ns":30,"first_batch_ns":null}],
            "initialization_ns":10,"initialization_plus_all_queries_ns":60,
            "startup_ns":1,"cleanup_ns":0,"session_elapsed_ns":100});
        timing(&value, true)?;
        for (pointer, replacement) in [
            ("/queries/0/completion_ns", json!(0)),
            ("/queries/0/first_batch_ns", json!(1)),
            ("/queries", json!([value["queries"][0]])),
            ("/initialization_plus_all_queries_ns", json!(50)),
            ("/startup_ns", json!(0)),
            ("/cleanup_ns", json!(-1)),
        ] {
            let mut changed = value.clone();
            *changed.pointer_mut(pointer).unwrap() = replacement;
            assert!(timing(&changed, true).is_err(), "{pointer}");
        }
        let mut open = value;
        open["queries"] = json!([open["queries"][0]]);
        open["open_query_ns"] = json!(20);
        timing(&open, false)?;
        open["open_query_ns"] = json!(21);
        assert!(timing(&open, false).is_err());
        Ok(())
    }
}

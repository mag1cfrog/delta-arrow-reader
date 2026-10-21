use super::{
    Result, State, files,
    limits::Limits,
    require,
    rows::{Layout, Rows},
    rules::{integer, strings},
    sort,
};
use crate::selective_read::{array, campaign, contract, storage::read_json, text, workload};
use arrow_ipc::reader::StreamReader;
use serde_json::{Value, json};
use std::{
    fs::{self, File},
    io::Read,
    path::Path,
};

pub(super) const IDENTITY_FIELDS: [&str; 5] = [
    "comparison_revision",
    "fixture_manifest_sha256",
    "case_id",
    "snapshot_version",
    "canonical_sql",
];
pub(in crate::selective_read) fn build(metadata: &Value) -> Result<()> {
    require(
        metadata["format"] == "selective-read-reference-v2" && metadata["status"] == "complete",
        "incomplete reference",
    )?;
    require(
        metadata["pyarrow"] == "25.0.1"
            && (metadata["sort_backend"] == "rust-external-merge-v1"
                || metadata["duckdb_sort"] == "1.5.5"),
        "unsupported oracle runtime",
    )
}
impl State {
    pub(super) fn compare_file(
        &mut self,
        result: &Path,
        reference: &Path,
        metadata: &Value,
        quota: Option<u64>,
        subset: bool,
    ) -> Result<(u64, Value)> {
        let projection = strings(&metadata["projection"])?;
        let layout = Layout::new(&projection)?;
        let mut stream = StreamReader::try_new(File::open(result)?, None)?;
        layout.check(stream.schema().as_ref())?;
        let nullability: serde_json::Map<_, _> = stream
            .schema()
            .fields()
            .iter()
            .map(|f| (f.name().clone(), json!(f.is_nullable())))
            .collect();
        let sorted = sort::Sorted::new(
            &layout,
            stream.by_ref().map(|b| Ok(b?)),
            reference.parent().ok_or("reference has no parent")?,
            quota,
            super::MEMORY,
        )?;
        require(
            stream.get_mut().read(&mut [0])? == 0,
            "trailing bytes after Arrow stream",
        )?;
        let mut expected = self
            .decoder
            .open(&reference.join("reference.parquet"), &[])?;
        layout.check(expected.schema().as_ref())?;
        let count = sort::compare(
            sorted,
            Rows::new(expected.by_ref(), &layout),
            integer(&metadata["output_rows"])?,
            subset,
        )?;
        expected.finish()?;
        Ok((count, Value::Object(nullability)))
    }
    pub(super) fn check(&mut self, request: &Value) -> Result<Value> {
        let reference = Path::new(text(&request["reference"])?);
        let fixtures = Path::new(text(&request["fixtures"])?);
        let result = Path::new(text(&request["result"])?);
        let identity = Path::new(text(&request["identity"])?);
        let metadata = read_json(&reference.join("reference.json"))?;
        let context = if integer(&metadata["comparison_revision"])? >= 3 {
            let context = workload::resolve(
                Path::new(text(&metadata["workload_manifest"])?),
                fixtures,
                text(&metadata["case_id"])?,
                &self.harness,
            )?;
            require(
                metadata["oracle_limits"] == context["workload"]["oracle_limits"],
                "oracle limits differ from workload",
            )?;
            context
        } else {
            Value::Null
        };
        let limits = Limits::enter(
            reference.parent().ok_or("reference has no parent")?,
            &context["workload"]["oracle_limits"],
        )?;
        build(&metadata)?;
        let comparison = contract::comparison(&metadata)?;
        if !context.is_null() {
            require(
                contract::same_comparison(&comparison, &context["identity"]),
                "stale workload manifest",
            )?;
            require(
                metadata["canonical_sql"] == context["case"]["canonical_sql"]
                    && metadata["projection"] == context["case"]["projection"],
                "reference workload query changed",
            )?;
        }
        require(
            metadata["fixture_manifest_sha256"]
                == self.digests.digest(&fixtures.join("manifest.json"))?,
            "stale fixture manifest",
        )?;
        require(
            metadata["reference_sha256"]
                == self.digests.digest(&reference.join("reference.parquet"))?,
            "reference Parquet changed",
        )?;
        for item in array(&metadata["verified_objects"])? {
            self.digests.verify(fixtures, item)?;
        }
        let provenance = read_json(identity)?;
        let reader = text(&provenance["reader_id"])?;
        require(
            campaign::roster(&metadata)?.contains(&reader) || self.spark && reader == "spark",
            "unknown reader identity",
        )?;
        require(
            contract::same_comparison(&comparison, &contract::comparison(&provenance)?),
            "result comparison mismatch",
        )?;
        for name in IDENTITY_FIELDS {
            require(
                provenance[name] == metadata[name],
                &format!("result identity mismatch: {name}"),
            )?;
        }
        for name in ["reader_build_sha256", "result_sha256"] {
            require(
                provenance[name].as_str().is_some_and(|s| {
                    s.len() == 64
                        && s.bytes()
                            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
                }),
                &format!("invalid {name}"),
            )?;
        }
        require(
            provenance["reader_config"].is_object() && provenance["reader_artifact"].is_object(),
            "missing reader execution contract; rerun validation",
        )?;
        let expression = provenance
            .get("native_expression")
            .ok_or("missing native expression")?;
        require(
            if ["polars", "daft"].contains(&reader) {
                expression.is_object()
            } else {
                expression.is_null()
            },
            "invalid native expression",
        )?;
        if !context.is_null() {
            require(
                expression
                    == &context["workload"]["translations"][reader]["expressions"]
                        [text(&metadata["case_id"])?]["native_expression"],
                "native translation differs from workload",
            )?;
        }
        require(
            self.digests.digest(result)? == provenance["result_sha256"],
            "result checksum mismatch",
        )?;
        if let Some(quota) = limits.quota {
            let name = result
                .file_name()
                .and_then(|s| s.to_str())
                .ok_or("invalid export filename")?;
            let query = name
                .strip_prefix("query-")
                .and_then(|n| n.strip_suffix(".arrow"))
                .is_some_and(|s| !s.is_empty() && s.bytes().all(|b| b.is_ascii_digit()));
            let bytes = if query {
                fs::read_dir(result.parent().ok_or("export has no parent")?)?.try_fold(
                    0u64,
                    |sum, e| -> Result<u64> {
                        let e = e?;
                        let n = e.file_name();
                        let n = n.to_string_lossy();
                        Ok(sum
                            + if n.starts_with("query-") && n.ends_with(".arrow") {
                                e.metadata()?.len()
                            } else {
                                0
                            })
                    },
                )?
            } else {
                fs::metadata(result)?.len()
            };
            require(
                fs::metadata(reference.join("reference.parquet"))?.len() <= quota / 4
                    && bytes <= quota / 2,
                "reference/session exports exhaust the oracle disk allowance",
            )?;
        }
        let (count, nullability) = self.compare_file(
            result,
            reference,
            &metadata,
            limits.quota.map(|n| n / 4),
            !metadata["limit"].is_null(),
        )?;
        require(
            self.digests.digest(result)? == provenance["result_sha256"]
                && self.digests.digest(&reference.join("reference.parquet"))?
                    == metadata["reference_sha256"]
                && self.digests.digest(&fixtures.join("manifest.json"))?
                    == metadata["fixture_manifest_sha256"],
            "validation input changed during comparison",
        )?;
        for item in array(&metadata["verified_objects"])? {
            self.digests.verify(fixtures, item)?;
        }
        let mut proof = contract::comparison(&provenance)?;
        for name in IDENTITY_FIELDS {
            proof[name] = metadata[name].clone();
        }
        for name in [
            "reader_id",
            "reader_build_sha256",
            "reader_artifact",
            "reader_config",
            "native_expression",
            "result_sha256",
        ] {
            proof[name] = provenance[name].clone();
        }
        proof.as_object_mut().ok_or("invalid correctness proof")?.extend(serde_json::from_value::<serde_json::Map<String,Value>>(json!({"status":"passed","reference_sha256":metadata["reference_sha256"],
            "identity_sha256":self.digests.digest(identity)?,"output_rows":count,"qualifying_rows":metadata["qualifying_rows"],"reported_nullability":nullability}))?);
        Ok(proof)
    }
    pub(super) fn intra(&mut self, request: &Value) -> Result<Value> {
        let reference = Path::new(text(&request["reference"])?);
        let metadata = read_json(&reference.join("reference.json"))?;
        let record = &request["record"];
        build(&metadata)?;
        for name in IDENTITY_FIELDS {
            require(
                record["identity"][name] == metadata[name],
                &format!("reference identity mismatch: {name}"),
            )?;
        }
        let mut checks = Vec::new();
        for query in array(&record["queries"])? {
            let result = files::inside(
                &Path::new(text(&request["output"])?).join("reader"),
                text(&query["result"])?,
            )?;
            let (count, _) = self.compare_file(&result, reference, &metadata, None, false)?;
            let mut check = record["identity"].clone();
            check["status"] = json!("passed");
            check["output_rows"] = json!(count);
            check["reference_sha256"] = metadata["reference_sha256"].clone();
            check["result_sha256"] = json!(self.digests.digest(&result)?);
            checks.push(check);
        }
        Ok(json!({"status":"passed","checks":checks}))
    }
}

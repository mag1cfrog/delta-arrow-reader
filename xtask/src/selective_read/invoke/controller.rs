use std::{fs, path::PathBuf};

use serde_json::json;

use super::{Request, require};
use crate::selective_read::{array, observe, request, storage, text, validation::Worker};
use crate::{Result, selective_read_build::digest};
use storage::{read_json, save};

#[derive(clap::Args)]
pub(crate) struct Args {
    #[arg(long)]
    binary: PathBuf,
    #[arg(long)]
    output: PathBuf,
    #[arg(long, required_unless_present = "request")]
    fixtures: Option<PathBuf>,
    #[arg(long, required_unless_present = "request", conflicts_with = "request")]
    case: Option<String>,
    #[arg(long, conflicts_with = "request")]
    workload: Option<PathBuf>,
    #[arg(long, default_value = "open", value_parser = ["open", "reuse"], conflicts_with = "request")]
    execution: String,
    #[arg(long, required_unless_present = "request", value_parser = ["validation", "timing", "diagnostic", "io"], conflicts_with = "request")]
    purpose: Option<String>,
    #[arg(long)]
    reference: Option<PathBuf>,
    #[arg(long, conflicts_with = "request")]
    correctness: Option<PathBuf>,
    #[arg(long, conflicts_with_all = ["request", "upload"])]
    table_uri: Option<String>,
    /// Invoke an existing saved request instead of constructing one.
    #[arg(long)]
    request: Option<PathBuf>,
    /// Capture requests against the dedicated MinIO server.
    #[arg(long)]
    state: Option<PathBuf>,
    /// Bind the remote table URI to a verified upload receipt.
    #[arg(long, requires_all = ["state", "fixtures", "case"])]
    upload: Option<PathBuf>,
    /// Allow Spark's historical export identity only for this untimed pilot.
    #[arg(long, requires_all = ["upload", "workload"], conflicts_with = "request")]
    spark_pilot: bool,
    /// Pinned independent oracle interpreter, required for validation.
    #[arg(long)]
    python: Option<PathBuf>,
    #[arg(long, default_value = "benches/selective_read")]
    harness: PathBuf,
}

pub(crate) fn run(mut args: Args) -> Result<()> {
    args.harness = fs::canonicalize(args.harness)?;
    args.fixtures = args.fixtures.map(fs::canonicalize).transpose()?;
    let protocols = args.harness.join("../../docs/content/benchmarks");
    let mut payload = if let Some(path) = &args.request {
        read_json(path)?
    } else {
        let mut value = request::run(request::Request {
            fixtures: args.fixtures.clone().ok_or("missing fixtures")?,
            case_id: args.case.clone().ok_or("missing case")?,
            execution_mode: args.execution,
            workload: args.workload,
            harness: args.harness.clone(),
        })?;
        value["purpose"] = json!(args.purpose.ok_or("missing purpose")?);
        value["run_id"] = json!(
            args.output
                .file_name()
                .ok_or("missing run ID")?
                .to_str()
                .ok_or("invalid run ID")?
        );
        value["correctness_file"] = json!(args.correctness.map(fs::canonicalize).transpose()?);
        if let Some(uri) = args.table_uri {
            value["table_uri"] = json!(uri);
        }
        value
    };
    let validation = payload["purpose"] == "validation";
    require(
        !validation || args.fixtures.is_some() && args.reference.is_some() && args.python.is_some(),
        "validation requires --fixtures, --reference and --python",
    )?;
    require(
        payload["purpose"] != "timing" || !payload["correctness_file"].is_null(),
        "timing requires a correctness certificate",
    )?;
    if args.spark_pilot {
        require(
            read_json(&args.binary.with_file_name("build.json"))?["reader_id"] == "spark",
            "provide the Spark pilot build",
        )?;
        require(
            ["validation", "diagnostic", "io"].contains(&text(&payload["purpose"])?),
            "Spark pilot cannot run timings",
        )?;
    }
    let _lock = args.state.as_deref().map(storage::exclusive).transpose()?;
    if let Some(upload) = &args.upload {
        let state = args
            .state
            .as_deref()
            .ok_or("upload requires storage state")?;
        let fixtures = args.fixtures.as_deref().ok_or("upload requires fixtures")?;
        let prepared =
            storage::upload::prepare(state, fixtures, upload, &[], storage::upload::hashes)?;
        storage::upload::verified(state, fixtures, &prepared)?;
        let receipt = &prepared["receipt"];
        if args.spark_pilot {
            require(
                array(&receipt["table_ids"])?.contains(&payload["case_id"]),
                "pilot snapshot is absent from the verified upload",
            )?;
        }
        let path = url::Url::parse(text(&payload["table_uri"])?)?
            .to_file_path()
            .map_err(|_| "expected a local fixture URI")?;
        let path = path
            .strip_prefix(fixtures)?
            .to_str()
            .ok_or("invalid fixture path")?;
        require(
            array(&receipt["objects"])?.iter().any(|item| {
                item["path"]
                    .as_str()
                    .is_some_and(|p| p.starts_with(&format!("{path}/")))
            }),
            "table is absent from verified upload",
        )?;
        payload["table_uri"] = json!(format!("{}/{path}", text(&receipt["table_root"])?));
    }
    let mut record = if let Some(state) = &args.state {
        observe::run(serde_json::from_value(
            json!({"action":"invoke","directory":state,
            "binary":args.binary,"payload":payload,"output":args.output,"reference":args.reference,
            "protocols":protocols,"traced":null}),
        )?)?
    } else {
        super::run(&Request {
            binary: args.binary,
            payload: payload.clone(),
            output: args.output.clone(),
            reference: args.reference.clone(),
            env: None,
            command_prefix: vec![],
            supervised: false,
            protocols,
        })?
    };
    if record["status"] == "success" && validation {
        let mut worker = Worker::start(
            args.python.as_deref().ok_or("missing oracle interpreter")?,
            &args.harness,
            &args.output,
        )?;
        if args.spark_pilot {
            worker.call(&json!({"action":"spark-pilot"}))?;
        }
        record = worker.check(
            record,
            &payload,
            &args.output,
            args.fixtures.as_deref().ok_or("missing fixtures")?,
            args.reference.as_deref().ok_or("missing reference")?,
        )?;
    }
    let path = args.output.join(if args.state.is_some() {
        "storage-observation.json"
    } else {
        "observation.json"
    });
    save(&path, &record)?;
    let mut result = json!({"status":record["status"],"observation":path});
    if args.spark_pilot {
        save(
            &args.output.join("pilot-inputs.json"),
            &json!({"upload_sha256":digest(args.upload.as_deref().ok_or("missing upload")?)?,
            "pilot_only":true,"publication_ready":false}),
        )?;
        result["pilot_only"] = json!(true);
        result["publication_ready"] = json!(false);
    }
    println!("{result}");
    require(
        record["status"] == "success",
        "reader invocation failed; inspect its observation",
    )
}

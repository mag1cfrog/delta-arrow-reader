use super::*;
use crate::selective_read::workload;

#[derive(clap::Args)]
pub(crate) struct Batch {
    #[arg(long)]
    campaign: PathBuf,
    #[arg(long)]
    output: PathBuf,
    #[arg(long, default_value = "benches/selective_read")]
    harness: PathBuf,
}

#[derive(clap::Args)]
pub(crate) struct Production {
    #[arg(long)]
    definition: PathBuf,
    #[arg(long = "campaign")]
    campaigns: Vec<PathBuf>,
    #[arg(long)]
    output: PathBuf,
    #[arg(long, default_value = "benches/selective_read")]
    harness: PathBuf,
}

fn observed_cases(jobs: &Value, journal: &str) -> Result<BTreeSet<String>> {
    let jobs = array(jobs)?
        .iter()
        .map(|job| Ok((text(&job["id"])?, text(&job["case_id"])?)))
        .collect::<Result<BTreeMap<_, _>>>()?;
    journal
        .lines()
        .map(|line| {
            let row: Value = serde_json::from_str(line)?;
            Ok(jobs
                .get(text(&row["job_id"])?)
                .ok_or("observation has an unknown job ID")?
                .to_string())
        })
        .collect()
}

pub(crate) fn workload_report(args: Batch) -> Result<Value> {
    let harness = fs::canonicalize(args.harness)?;
    let config = read_json(&args.campaign.join("campaign.json"))?;
    let (frozen, identity) = workload::load(Path::new(text(&config["workload"])?), &harness)?;
    let cases = observed_cases(
        &config["jobs"],
        &fs::read_to_string(args.campaign.join("observations.jsonl"))?,
    )?;
    let bindings = cases
        .into_iter()
        .map(|case| {
            let row = workload::binding(&frozen, Path::new(text(&config["fixtures"])?), &case)?;
            Ok((case, row))
        })
        .collect::<Result<Map<_, _>>>()?;
    batch::run(serde_json::from_value(
        json!({"campaign":args.campaign,"output":args.output,
        "format":{"kind":"workload","frozen":frozen,"identity":identity,"bindings":bindings,
        "protocols":harness.join("../../docs/content/benchmarks")}}),
    )?)
}

pub(crate) fn production_report(args: Production) -> Result<Value> {
    require(!args.output.exists(), "report output already exists")?;
    let harness = fs::canonicalize(args.harness)?;
    let (frozen, identity) = workload::load(&args.definition, &harness)?;
    let parent = args
        .output
        .parent()
        .filter(|p| !p.as_os_str().is_empty())
        .unwrap_or(Path::new("."));
    let scratch = tempfile::Builder::new()
        .prefix("production-report-")
        .tempdir_in(parent)?;
    let campaigns = args
        .campaigns
        .into_iter()
        .enumerate()
        .map(|(index, path)| {
            let audited = workload_report(Batch {
                campaign: path.clone(),
                output: scratch.path().join(index.to_string()),
                harness: harness.clone(),
            })?;
            Ok((path, audited))
        })
        .collect::<Result<Vec<_>>>()?;
    super::run(Request {
        definition: args.definition,
        frozen,
        identity,
        campaigns,
        output: args.output,
        protocols: harness.join("../../docs/content/benchmarks"),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn report_binds_each_observed_case_once_and_rejects_unknown_jobs() -> Result<()> {
        let jobs = json!([{"id":"open","case_id":"case"},{"id":"reuse","case_id":"case"},{"id":"next","case_id":"other"}]);
        let journal = "{\"job_id\":\"open\"}\n{\"job_id\":\"reuse\"}\n{\"job_id\":\"next\"}\n";
        assert_eq!(
            observed_cases(&jobs, journal)?,
            BTreeSet::from(["case".into(), "other".into()])
        );
        assert!(observed_cases(&jobs, "{\"job_id\":\"unknown\"}").is_err());
        assert!(observed_cases(&jobs, "{}").is_err());
        assert!(observed_cases(&jobs, "{").is_err());
        assert!(observed_cases(&jobs, "")?.is_empty());
        Ok(())
    }
}

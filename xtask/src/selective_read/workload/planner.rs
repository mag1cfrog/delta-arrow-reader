use std::{
    collections::BTreeMap,
    fs,
    io::Write,
    process::{Command, Stdio},
    time::{Duration, Instant},
};

use super::*;
use crate::{
    command::wait_with_timeout,
    selective_read::storage::{read_json, save},
};

#[derive(clap::Args)]
pub(crate) struct Args {
    #[arg(long)]
    source: PathBuf,
    #[arg(long)]
    output: PathBuf,
    #[arg(long)]
    python: PathBuf,
    #[arg(long, default_value_t = 512)]
    file_target_mib: u64,
    #[arg(long, default_value_t = 20000)]
    page_rows: u64,
    #[arg(long, default_value_t = 1048576)]
    page_bytes: u64,
    #[arg(long, default_value_t = 1024)]
    write_batch_rows: u64,
    #[arg(long, default_value = "benches/selective_read")]
    harness: PathBuf,
}

fn file_counts(shape: &Value) -> Result<(u64, u64)> {
    let (files, stripes) = (count(&shape["files"])?, count(&shape["stripes"])?);
    require(
        files >= stripes && stripes > 0,
        "invalid file/stripe counts",
    )?;
    Ok((files, stripes))
}

pub(super) fn layout_query(shape: &Value) -> Result<String> {
    let (files, stripes) = file_counts(shape)?;
    Ok(format!(
        "WITH striped AS (
            SELECT *, l_orderkey % {stripes} AS stripe FROM source
        ), ordered AS (
            SELECT *, row_number() OVER (
                PARTITION BY stripe ORDER BY l_shipdate, l_shipmode, l_linenumber, l_orderkey
            ) - 1 AS ordinal, count(*) OVER (PARTITION BY stripe) AS stripe_rows
            FROM striped
        ), assigned AS (
            SELECT *, (ordinal * ({per_stripe} + CAST(stripe < {remainder} AS BIGINT)))
                // stripe_rows AS file_index FROM ordered
        )
        SELECT stripe, file_index, count(*) AS rows,
            min(l_shipdate) AS date_min, max(l_shipdate) AS date_max,
            min(l_shipmode) AS mode_min, max(l_shipmode) AS mode_max,
            min(l_linenumber) AS line_min, max(l_linenumber) AS line_max,
            count(*) FILTER (WHERE {PREDICATE}) AS matching_rows
        FROM assigned GROUP BY stripe, file_index ORDER BY stripe, file_index",
        per_stripe = files / stripes,
        remainder = files % stripes,
    ))
}

pub(super) fn layout_geometry(shape: &Value, statistics: &Value) -> Result<Value> {
    #[derive(Deserialize)]
    #[serde(deny_unknown_fields)]
    struct File {
        stripe: u64,
        file_index: u64,
        rows: u64,
        date_min: String,
        date_max: String,
        mode_min: String,
        mode_max: String,
        line_min: i64,
        line_max: i64,
        matching_rows: u64,
    }
    let (files, stripes) = file_counts(shape)?;
    let statistics: Vec<File> = serde_json::from_value(statistics.clone())?;
    require(
        statistics.len() as u64 == files,
        "source cannot populate the declared file count",
    )?;
    let expected = (0..stripes).flat_map(|stripe| {
        (0..files / stripes + u64::from(stripe < files % stripes)).map(move |index| (stripe, index))
    });
    let mut geometry = Vec::new();
    for (file, (stripe, index)) in statistics.into_iter().zip(expected) {
        require(
            (file.stripe, file.file_index) == (stripe, index)
                && file.rows > 0
                && file.matching_rows <= file.rows,
            "invalid file membership statistics",
        )?;
        let candidate = file.date_min.as_str() <= "1995-03-15"
            && "1995-03-15" <= file.date_max.as_str()
            && file.mode_min.as_str() <= "AIR"
            && "AIR" <= file.mode_max.as_str()
            && file.line_min <= 1
            && 1 <= file.line_max;
        require(
            file.matching_rows == 0 || candidate,
            "file bounds exclude qualifying rows",
        )?;
        geometry.push(json!({"stripe":stripe,"file_index":index,"rows":file.rows,
            "candidate":candidate,"matching_rows":file.matching_rows}));
    }
    Ok(json!(geometry))
}

fn check_counts(counts: &Value, source: &Value) -> Result<()> {
    let counts: Vec<_> = array(counts)?.iter().map(count).collect::<Result<_>>()?;
    require(
        counts.len() == 4 && counts[0] == count(&source["rows"])?,
        "source row count differs",
    )?;
    require(
        counts
            .windows(2)
            .all(|pair| pair[0] > pair[1] && pair[1] > 0),
        "each predicate must reduce the source",
    )?;
    require(
        (300..=2000).contains(&counts[3]),
        "query does not have the declared hundreds-of-rows shape",
    )
}

fn check_geometry(geometry: &Value, shape: &Value, counts: &Value) -> Result<(usize, usize)> {
    let files = array(geometry)?;
    let total = count(&shape["files"])?;
    let stripes = count(&shape["stripes"])?;
    require(
        files.len() as u64 == total
            && fixtures::sum(files, "rows")? == count(&counts[0])?
            && fixtures::sum(files, "matching_rows")? == count(&counts[3])?,
        "layout lost source or matching rows",
    )?;
    let candidates = files
        .iter()
        .filter(|file| file["candidate"] == true)
        .count();
    let matching = files
        .iter()
        .filter(|file| file["matching_rows"].as_u64().is_some_and(|n| n > 0))
        .count();
    require(
        (stripes..=3 * stripes).contains(&(candidates as u64)) && matching as u64 >= stripes,
        "query does not select a few files across every stripe",
    )?;
    Ok((candidates, matching))
}

pub(crate) fn run(args: Args) -> Result<()> {
    let started = Instant::now();
    let deadline = started + Duration::from_secs(600);
    let mut definitions = shapes(
        args.file_target_mib,
        args.page_rows,
        args.page_bytes,
        args.write_batch_rows,
    )?;
    let root = fs::canonicalize(&args.source)?;
    let harness = fs::canonicalize(&args.harness)?;
    let manifest_hash = fixtures::hash_before(&root.join("manifest.json"), deadline)?;
    let (source, paths) = fixtures::source(&root, |path| fixtures::hash_before(path, deadline))?;
    fs::create_dir(&args.output)?;
    let output = fs::canonicalize(&args.output)?;
    let queries: Map<_, _> = object(&definitions)?
        .iter()
        .map(|(name, shape)| Ok((name.clone(), json!(layout_query(shape)?))))
        .collect::<Result<_>>()?;
    let result_path = output.join("statistics.json");
    let mut child = fixtures::spawn(
        Command::new(&args.python)
            .args(["-I", "-B"])
            .arg(harness.join("production_shapes.py"))
            .arg("--analyze")
            .arg(&output)
            .env("SELECTIVE_READ_XTASK", std::env::current_exe()?)
            .stdin(Stdio::piped())
            .stdout(fs::File::create_new(&result_path)?)
            .stderr(fs::File::create_new(output.join("planner.stderr.log"))?),
        16 * 1024u64.pow(3),
    )?;
    writeln!(
        child.0.stdin.take().ok_or("missing planner input")?,
        "{}",
        json!({"paths":paths,"queries":queries})
    )?;
    require(
        wait_with_timeout(
            &mut child.0,
            Duration::from_secs(600).saturating_sub(started.elapsed()),
        )?
        .success(),
        "independent planner failed; see planner.stderr.log",
    )?;
    let analysis = read_json(&result_path)?;
    let counts = &analysis["counts"];
    check_counts(counts, &source)?;
    let mut sql = Map::new();
    let mut geometries = Map::new();
    for (name, shape) in definitions.as_object_mut().unwrap() {
        let geometry = layout_geometry(shape, &analysis["statistics"][name])?;
        let (candidates, matching) = check_geometry(&geometry, shape, counts)?;
        let path = output.join(format!("{name}-files.json"));
        save(&path, &geometry)?;
        geometries.insert(name.clone(), geometry);
        shape["candidate_files"] = json!(candidates);
        shape["matching_files"] = json!(matching);
        shape["output_rows"] = counts[3].clone();
        shape["file_geometry_sha256"] = json!(digest(&path)?);
        sql.insert(name.clone(), shape["canonical_sql"].clone());
    }
    save(
        &output.join("analysis.json"),
        &json!({
            "counts":counts, "geometry":geometries, "duckdb":analysis["duckdb"],
        }),
    )?;
    require(
        fixtures::hash_before(&root.join("manifest.json"), deadline)? == manifest_hash,
        "source manifest changed",
    )?;
    save(&output.join("sql.json"), &json!(sql))?;
    let result = json!({"format":"selective-read-production-shape-plan-v1","status":"logical_geometry_checked",
        "publication_ready":false,"native_campaign_ready":false,"contract_sha256":digest(&fixtures::contract(&harness))?,
        "planner_sha256":digest(&harness.join("production_shapes.py"))?,
        "planner_controller_sha256":digest(&std::env::current_exe()?)?,
        "source_manifest_sha256":manifest_hash,"source_scale":10,"predicate_stage_rows":counts,
        "file_target_mib":args.file_target_mib,"data_page_rows":args.page_rows,"data_page_bytes":args.page_bytes,
        "write_batch_rows":args.write_batch_rows,"duckdb":analysis["duckdb"],"shapes":definitions,"cases":cases(),
        "scope":"Source predicates and proposed file membership only. No Delta generation, physical page or performance acceptance.",
        "pending":["bounded compression/capacity probe","Delta fixtures and actual group/page geometry",
            "independent exact oracle and real DV pairs","five-reader pilot and recorded storage transport"]});
    require(
        started.elapsed() < Duration::from_secs(600),
        "production planning deadline exceeded",
    )?;
    save(&output.join("plan.json"), &result)?;
    println!(
        "{}",
        json!({"status":result["status"],"predicate_stage_rows":counts,
        "shapes":object(&definitions)?.iter().map(|(name,shape)| (name, json!({"files":shape["files"],
            "candidate_files":shape["candidate_files"],"matching_files":shape["matching_files"],"output_rows":shape["output_rows"]})))
            .collect::<BTreeMap<_,_>>()})
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn layout_statistics_reject_missing_files_invalid_membership_and_false_bounds() -> Result<()> {
        let shape = json!({"files":2,"stripes":1});
        let statistics = json!([
            {"stripe":0,"file_index":0,"rows":10,"date_min":"1995-03-14","date_max":"1995-03-15",
             "mode_min":"AIR","mode_max":"RAIL","line_min":1,"line_max":2,"matching_rows":2},
            {"stripe":0,"file_index":1,"rows":5,"date_min":"1995-03-16","date_max":"1995-03-17",
             "mode_min":"AIR","mode_max":"AIR","line_min":1,"line_max":1,"matching_rows":0},
        ]);
        assert_eq!(
            layout_geometry(&shape, &statistics)?,
            json!([
                {"stripe":0,"file_index":0,"rows":10,"candidate":true,"matching_rows":2},
                {"stripe":0,"file_index":1,"rows":5,"candidate":false,"matching_rows":0},
            ])
        );
        for invalid in [
            json!({"files":0,"stripes":0}),
            json!({"files":1,"stripes":2}),
            json!({"files":true,"stripes":1}),
        ] {
            assert!(layout_query(&invalid).is_err());
            assert!(layout_geometry(&invalid, &statistics).is_err());
        }
        assert!(layout_geometry(&shape, &json!([statistics[0]])).is_err());
        for (field, value) in [
            ("stripe", json!(1)),
            ("file_index", json!(1)),
            ("rows", json!(0)),
            ("matching_rows", json!(11)),
            ("matching_rows", json!(-1)),
            ("date_min", json!("1995-03-16")),
            ("date_max", json!("1995-03-14")),
            ("mode_min", json!("RAIL")),
            ("mode_max", json!("AAA")),
            ("line_min", json!(2)),
            ("line_max", json!(0)),
        ] {
            let mut invalid = statistics.clone();
            invalid[0][field] = value;
            assert!(layout_geometry(&shape, &invalid).is_err(), "{field}");
        }
        Ok(())
    }

    #[test]
    fn planning_requires_independent_counts_and_bounded_candidate_geometry() -> Result<()> {
        let counts = json!([12000, 1200, 600, 360]);
        let source = json!({"rows":12000});
        check_counts(&counts, &source)?;
        for invalid in [
            json!([12000, 1200, 600, 200]),
            json!([12000, 1200, 1200, 360]),
            json!([13000, 1200, 600, 360]),
            json!([12000, 1200, 600]),
        ] {
            assert!(check_counts(&invalid, &source).is_err());
        }
        let shape = shapes(512, 20000, 1048576, 1024)?["q4"].clone();
        let files: Vec<_> = (0..60)
            .map(|i| {
                json!({"rows":200,"candidate":i % 10 == 0,
            "matching_rows":if i % 10 == 0 {60} else {0}})
            })
            .collect();
        let mut geometry = json!(files);
        assert_eq!(check_geometry(&geometry, &shape, &counts)?, (6, 6));
        geometry[0]["rows"] = json!(199);
        assert!(check_geometry(&geometry, &shape, &counts).is_err());
        geometry[0]["rows"] = json!(200);
        geometry[0]["candidate"] = json!(false);
        assert!(check_geometry(&geometry, &shape, &counts).is_err());
        geometry[0]["candidate"] = json!(true);
        geometry[0]["matching_rows"] = json!(59);
        assert!(check_geometry(&geometry, &shape, &counts).is_err());
        Ok(())
    }
}

use super::{
    Result, State, files, geometry, io,
    limits::Limits,
    production, require,
    rows::{Layout, Rows},
    rules::{self, integer},
    sort,
    spec::Spec,
};
use crate::selective_read::{
    array, contract,
    storage::{read_json, save},
    text, workload,
};
use serde_json::{Value, json};
use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    path::Path,
};

struct Population {
    counts: Vec<u64>,
    deleted_matches: u64,
    bytes: u64,
    source_reused: bool,
}
#[derive(Default)]
struct Physical {
    rows: u64,
    matches: u64,
    deleted: u64,
    candidates: Vec<String>,
    matching: Vec<String>,
    scans: Value,
}
fn coordinates(item: &Value) -> Result<BTreeMap<u64, [i64; 2]>> {
    if item["deletion_vector"].is_null() {
        return Ok(BTreeMap::new());
    }
    let dv = &item["deletion_vector"];
    let indexes = array(&dv["physical_ordinals"])?;
    let keys = array(&dv["logical_ids"])?;
    require(indexes.len() == keys.len(), "invalid deletion coordinates")?;
    indexes
        .iter()
        .zip(keys)
        .map(|(i, k)| Ok((integer(i)?, rules::key(k)?)))
        .collect()
}
fn removed(spec: &Spec, key: [i64; 2], extras: &BTreeSet<[i64; 2]>) -> Result<bool> {
    Ok(spec.table["deletion_vectors"] == true
        && if spec.source.is_none() {
            !rules::control_matches(&spec.case, key[0])? && key[0] % 1000 == 0
        } else {
            rules::deleted(key) || extras.contains(&key)
        })
}
impl State {
    pub(super) fn prepare(&mut self, request: &Value) -> Result<Value> {
        let fixtures = Path::new(text(&request["fixtures"])?).canonicalize()?;
        let output = Path::new(text(&request["reference"])?);
        let case = text(&request["case_id"])?;
        let context = if let Some(path) = request["workload"].as_str() {
            workload::resolve(Path::new(path), &fixtures, case, &self.harness)?
        } else {
            Value::Null
        };
        if request["reuse"] == true {
            let metadata = read_json(&output.join("reference.json"))?;
            super::check::build(&metadata)?;
            if let Some(sql) = request.get("sql") {
                let spec = Spec::load(&fixtures, case, false, &context)?;
                require(
                    sql == &spec.sql && metadata["canonical_sql"] == *sql,
                    "oracle SQL differs from catalog",
                )?;
            }
            return Ok(metadata);
        }
        let limits = Limits::enter(
            output.parent().ok_or("reference output has no parent")?,
            &context["workload"]["oracle_limits"],
        )?;
        let duplicate = request["duplicate_in_literal"] == true;
        let manifest_before = files::identity(&fixtures.join("manifest.json"))?;
        let spec = Spec::load(&fixtures, case, duplicate, &context)?;
        if let Some(sql) = request.get("sql") {
            require(sql == &spec.sql, "oracle SQL differs from catalog")?;
        }
        let verified = files::objects(
            &mut self.digests,
            &fixtures,
            &spec.manifest,
            &spec.table,
            spec.source.as_ref(),
        )?;
        let geometry = if spec.source.is_none() {
            geometry::control(&fixtures, &spec.table, &spec.case)?
        } else {
            Value::Null
        };
        let saved = spec.saved_keys()?;
        let extras = if !spec.production || spec.table["deletion_vectors"] == true {
            self.extra_keys(&fixtures, &spec)?
        } else {
            BTreeSet::new()
        };
        fs::create_dir(output)?;
        let reference = output.join("reference.parquet");
        let layout = Layout::new(&spec.projection)?;
        let population = self.expected(
            &fixtures,
            &spec,
            &extras,
            &saved,
            &reference,
            &layout,
            limits.quota.map(|n| n / 4),
        )?;
        let live = population
            .counts
            .last()
            .copied()
            .ok_or("missing predicate populations")?
            - population.deleted_matches;
        require(
            population.counts[0] == integer(&spec.table["rows"])?,
            "source/table row counts disagree",
        )?;
        let physical = self.physical(
            &fixtures,
            &spec,
            &extras,
            &layout,
            &reference,
            live,
            population.source_reused,
            limits.quota.map(|n| n / 4),
        )?;
        require(
            physical.rows == population.counts[0] && physical.deleted as usize == saved.len(),
            "fixture/reference populations disagree",
        )?;
        if spec.production {
            require(
                physical.matches == *population.counts.last().unwrap(),
                "fixture/reference qualifying rows disagree",
            )?;
        }
        require(
            physical
                .matching
                .iter()
                .all(|name| physical.candidates.contains(name)),
            "file statistics exclude a matching file",
        )?;
        if !spec.suffix.is_empty() {
            let summary = &spec.table["deletion_summary"];
            let affected = array(&spec.table["files"])?
                .iter()
                .filter(|f| !f["deletion_vector"].is_null())
                .count();
            require(
                summary["physical_rows"] == population.counts[0]
                    && summary["deleted_rows"] == saved.len()
                    && summary["live_rows"] == population.counts[0] - saved.len() as u64
                    && summary["dv_files"] == affected,
                "wrong deletion summary",
            )?;
            if !spec.production {
                require(
                    (summary["density"]
                        .as_f64()
                        .ok_or("invalid deletion density")?
                        - saved.len() as f64 / population.counts[0] as f64)
                        .abs()
                        < 1e-15
                        && (summary["file_coverage"]
                            .as_f64()
                            .ok_or("invalid DV coverage")?
                            - affected as f64 / array(&spec.table["files"])?.len() as f64)
                            .abs()
                            < 1e-15,
                    "wrong deletion summary",
                )?;
            } else {
                require(
                    population.deleted_matches > 0
                        && live > 0
                        && !physical.candidates.is_empty()
                        && physical.candidates.len() < array(&spec.table["files"])?.len()
                        && summary["dv_files"] == spec.table["file_count"],
                    "invalid DV coverage/deletions",
                )?;
            }
        }
        if !context.is_null() && spec.table["deletion_vectors"] == true {
            require(
                population.deleted_matches > 0 && live > 0,
                "large DV anchor needs qualifying deletions and survivors",
            )?;
        }
        for item in &verified {
            self.digests.verify(&fixtures, item)?;
        }
        let mut metadata = json!({"format":"selective-read-reference-v2","status":"complete","comparison_revision":2,
            "oracle_implementation":"rust-v1","python":self.python_version,"pyarrow":"25.0.1","sort_backend":"rust-external-merge-v1",
            "reference_storage":{"format":"parquet","compression":"zstd","row_group_rows":131072},"sort_memory_bytes":super::MEMORY,"sort_threads":1,
            "fixture_manifest_sha256":self.digests.digest(&fixtures.join("manifest.json"))?,
            "case_id":case,"snapshot_version":spec.table["snapshot_version"],"profile":if spec.production {json!(format!("production-{}",text(&spec.manifest["mode"])?))} else {spec.manifest["profile"].clone()},
            "query_variant":if duplicate {"duplicate-in"} else {"canonical"},"canonical_sql":spec.sql,"in_literals":spec.literals,"projection":spec.projection,"limit":spec.limit,
            "source_rows":population.counts[0],"qualifying_rows":live,"physical_qualifying_rows":population.counts.last(),"deleted_qualifying_rows":population.deleted_matches,
            "deleted_rows":saved.len(),"live_rows":population.counts[0]-saved.len() as u64,"output_rows":spec.limit.map_or(live,|n|n.min(live)),
            "predicate_step_population":"physical_rows_before_deletions","qualifying_selectivity":live as f64/population.counts[0] as f64,
            "active_files":array(&spec.table["files"])?.len(),"candidate_files":physical.candidates,"matching_files":physical.matching,
            "verified_objects":verified,"reference_sha256":self.digests.digest(&reference)?});
        let predicates = rules::conditions(&spec.predicate, &spec.literals)?;
        metadata["predicate_steps"]=json!(predicates.iter().zip(&population.counts[1..]).map(|(p,n)|json!({"predicate":p.describe(),"rows":n,"selectivity":*n as f64/population.counts[0] as f64})).collect::<Vec<_>>());
        if !geometry.is_null() {
            metadata["within_file_geometry"] = geometry;
        }
        if spec.manifest.get("wide_file_pair").is_some() {
            require(
                live > 0
                    && (spec.table["deletion_vectors"] != true || population.deleted_matches > 0),
                "wide pair needs a deleted match and a survivor",
            )?;
            let mut sizes = array(&spec.table["files"])?
                .iter()
                .map(|f| integer(&f["bytes"]))
                .collect::<Result<Vec<_>>>()?;
            sizes.sort_unstable();
            let mut excluded = array(&spec.table["files"])?
                .iter()
                .map(|f| text(&f["path"]))
                .collect::<Result<Vec<_>>>()?;
            excluded.retain(|f| !physical.candidates.iter().any(|c| c == f));
            excluded.sort_unstable();
            metadata["wide_file_geometry"] = json!({"file_bytes_sorted":sizes,"excluded_files":excluded,"dv_coverage":geometry::coverage(&spec.table,&physical.candidates,&physical.matching)?});
        }
        if spec.production {
            let evidence = array(&spec.table["file_evidence"])?;
            let mut groups = 0;
            let mut pages = 0;
            for file in evidence {
                for group in array(&file["matching_groups"])? {
                    groups += 1;
                    pages += integer(&group["matching_output_pages"])?;
                }
            }
            metadata["physical_rows"] = json!(physical.rows);
            metadata["predicate_stage_rows"] = json!(population.counts);
            metadata["within_file_geometry"] = json!({"matching_groups":groups,"matching_projected_pages":pages,"scope":"physical geometry before deletions, not reader decode counters"});
            metadata["derived_value_checks"] = json!({"payloads":"all qualifying output rows, independent SHA256","metrics":"all stored rows, independent Rust arithmetic"});
            metadata["physical_validation"] = physical.scans;
        }
        if !context.is_null() {
            for (key, value) in context["identity"]
                .as_object()
                .ok_or("invalid workload identity")?
            {
                metadata[key] = value.clone();
            }
            metadata["workload_manifest"] = context["path"].clone();
            metadata["oracle_limits"] = context["workload"]["oracle_limits"].clone();
            metadata["projected_logical_bytes"] = json!(population.bytes);
            metadata["projected_logical_bytes_definition"] = json!(
                "non-null fixed-width values and actual UTF-8 string bytes; excludes null bitmaps, offsets and IPC framing"
            );
            let current = workload::resolve(
                Path::new(text(&context["path"])?),
                &fixtures,
                case,
                &self.harness,
            )?;
            require(
                contract::same_comparison(&context["identity"], &current["identity"]),
                "workload changed during reference preparation",
            )?;
        }
        require(
            files::identity(&fixtures.join("manifest.json"))? == manifest_before,
            "fixture manifest changed during reference preparation",
        )?;
        // Write the completion marker only after all independent checks succeed.
        save(&output.join("reference.json"), &metadata)?;
        Ok(metadata)
    }
    #[allow(clippy::too_many_arguments)]
    fn expected(
        &mut self,
        fixtures: &Path,
        spec: &Spec,
        extras: &BTreeSet<[i64; 2]>,
        saved: &BTreeSet<[i64; 2]>,
        reference: &Path,
        layout: &Layout,
        quota: Option<u64>,
    ) -> Result<Population> {
        let predicates = rules::conditions(&spec.predicate, &spec.literals)?;
        let mut population = Population {
            counts: vec![0; predicates.len() + 1],
            deleted_matches: 0,
            bytes: 0,
            source_reused: false,
        };
        let mut writer = io::Reference::new(reference, layout, quota)?;
        let mut deleted = BTreeSet::new();
        let mut found = BTreeSet::new();
        if spec.production {
            let (scan, reused) = self.source_scan(fixtures, spec)?;
            population.counts = scan.counts.into();
            population.source_reused = reused;
            let input = Layout::new(&rules::wide())?;
            for row in scan.rows {
                if removed(spec, row.key, extras)? {
                    population.deleted_matches += 1;
                } else {
                    let row = rules::project(&row, &input, layout, false)?;
                    population.bytes += rules::logical_bytes(&row, layout);
                    writer.push(row)?;
                }
            }
        } else if let Some(source) = &spec.source {
            let input = Layout::new(&rules::original())?;
            let mut previous = None;
            let literal_predicates = rules::conditions("eq2", &[])?;
            for item in array(&source["files"])? {
                let path = files::path(fixtures, source, item)?;
                let before = files::identity(&path)?;
                let mut parquet = self.decoder.open(&path, &[])?;
                input.check(parquet.schema().as_ref())?;
                let mut count = 0;
                for row in Rows::new(parquet.by_ref(), &input) {
                    let row = row?;
                    count += 1;
                    require(
                        previous.is_none_or(|key| key < row.key),
                        "source keys are duplicated or out of generator order",
                    )?;
                    previous = Some(row.key);
                    if rules::prefix(&row, &input, &literal_predicates)? == literal_predicates.len()
                    {
                        let super::rows::Cell::Int(part) = row.values[1] else {
                            unreachable!()
                        };
                        found.insert(part);
                        if found.len() > 20 {
                            found.pop_last();
                        }
                    }
                    let passed = rules::prefix(&row, &input, &predicates)?;
                    for count in &mut population.counts[..=passed] {
                        *count += 1;
                    }
                    if removed(spec, row.key, extras)? {
                        deleted.insert(row.key);
                        population.deleted_matches += u64::from(passed == predicates.len());
                    } else if passed == predicates.len() {
                        let row = rules::project(&row, &input, layout, true)?;
                        population.bytes += rules::logical_bytes(&row, layout);
                        writer.push(row)?;
                    }
                }
                parquet.finish()?;
                require(
                    count == integer(&item["rows"])? && files::identity(&path)? == before,
                    "reference file changed during scan",
                )?;
            }
            require(
                found.into_iter().collect::<Vec<_>>() == spec.literals,
                "frozen IN literals disagree with full source",
            )?;
            require(
                population.counts[0] == integer(&source["rows"])?,
                "source row counts disagree",
            )?;
        } else {
            population.counts[0] = integer(&spec.table["rows"])?;
            for n in 0..i64::try_from(population.counts[0])? {
                if removed(spec, [n, 0], extras)? {
                    deleted.insert([n, 0]);
                }
                if rules::control_matches(&spec.case, n)? {
                    population.counts[1] += 1;
                    writer.push(rules::control_row(n))?;
                }
            }
            require(
                population.counts[1]
                    == if spec.case == "row-groups.select" {
                        65536
                    } else {
                        64
                    },
                "wrong qualifying control geometry",
            )?;
        }
        writer.finish()?;
        if !spec.production {
            require(
                &deleted == saved,
                "saved logical deletions disagree with independent source rule",
            )?;
        }
        Ok(population)
    }
    #[allow(clippy::too_many_arguments)]
    fn physical(
        &mut self,
        fixtures: &Path,
        spec: &Spec,
        extras: &BTreeSet<[i64; 2]>,
        output: &Layout,
        reference: &Path,
        expected: u64,
        source_reused: bool,
        quota: Option<u64>,
    ) -> Result<Physical> {
        let mut result = Physical {
            scans: State::scan_metadata(source_reused),
            ..Default::default()
        };
        let predicates = rules::conditions(&spec.predicate, &spec.literals)?;
        let input = Layout::new(&spec.stored())?;
        let wide = Layout::new(&rules::wide())?;
        let mut spool = sort::Spool::new(
            output,
            reference.parent().ok_or("reference has no parent")?,
            quota,
            super::MEMORY,
        )?;
        let files = array(&spec.table["files"])?;
        if spec.production {
            require(
                array(&spec.table["file_evidence"])?.len() == files.len(),
                "missing physical evidence",
            )?;
        }
        for (index, item) in files.iter().enumerate() {
            let name = text(&item["path"])?;
            if rules::candidate(&item["delta_stats"], &predicates)? {
                result.candidates.push(name.into());
            }
            let path = files::path(fixtures, &spec.table, item)?;
            let before = files::identity(&path)?;
            let coordinates = coordinates(item)?;
            let cached = if spec.production {
                self.cache
                    .files
                    .get(&(before.clone(), spec.metrics))
                    .cloned()
            } else {
                None
            };
            let mut physical = if let Some(cached) = &cached {
                cached.clone()
            } else {
                production::Physical {
                    rows: 0,
                    ordinals: Vec::new(),
                    matches: Vec::new(),
                }
            };
            let mut matched = false;
            let mut count = 0;
            if spec.production {
                let name = if cached.is_some() {
                    "reused_full_column_scans"
                } else {
                    "full_column_scans"
                };
                result.scans[name] = json!(integer(&result.scans[name])? + 1);
                require(
                    spec.table["deletion_vectors"] != true || !coordinates.is_empty(),
                    "production file has an empty DV",
                )?;
            }
            if cached.is_some() && spec.table["deletion_vectors"] != true {
                count = physical.rows;
            } else {
                let columns = if cached.is_some() {
                    vec!["l_orderkey".into(), "l_linenumber".into()]
                } else {
                    vec![]
                };
                let mut parquet = self.decoder.open(&path, &columns)?;
                let scan_layout = if cached.is_some() {
                    Layout::new(&columns)?
                } else {
                    Layout::new(&spec.stored())?
                };
                scan_layout.check(parquet.schema().as_ref())?;
                for batch in parquet.by_ref() {
                    let batch = scan_layout.normalize(batch?)?;
                    if spec.production {
                        let stages = if cached.is_none() {
                            production::metrics(&batch, spec.metrics)?;
                            production::stages(&batch)?
                        } else {
                            vec![]
                        };
                        for (i, key) in production::keys(&batch)?.enumerate() {
                            let deleted = removed(spec, key, extras)?;
                            require(
                                coordinates.contains_key(&(count + i as u64)) == deleted
                                    && (!deleted || coordinates[&(count + i as u64)] == key),
                                "wrong physical deletion ordinal/key",
                            )?;
                            result.deleted += u64::from(deleted);
                            if cached.is_none() && stages[i] == 3 {
                                physical.ordinals.push(count + i as u64);
                                physical.matches.push(rules::project(
                                    &input.read_row(&batch, i)?,
                                    &input,
                                    &wide,
                                    false,
                                )?);
                            }
                        }
                    } else {
                        for i in 0..batch.num_rows() {
                            let row = input.read_row(&batch, i)?;
                            if spec.source.is_none() {
                                require(row.key[0] as u64==result.rows+count+i as u64 && row.values[1]==super::rows::Cell::Text(if rules::control_matches(&spec.case,row.key[0])? {"match"} else {"other"}.into()),"control row order or match placement changed")?;
                            }
                            let deleted = removed(spec, row.key, extras)?;
                            require(
                                coordinates.contains_key(&(count + i as u64)) == deleted
                                    && (!deleted || coordinates[&(count + i as u64)] == row.key),
                                "physical deletion ordinal differs from rule",
                            )?;
                            result.deleted += u64::from(deleted);
                            if !deleted
                                && rules::prefix(&row, &input, &predicates)? == predicates.len()
                            {
                                matched = true;
                                spool.push(rules::project(&row, &input, output, false)?)?;
                            }
                        }
                    }
                    count += batch.num_rows() as u64;
                }
                parquet.finish()?;
            }
            require(
                count == integer(&item["rows"])? && files::identity(&path)? == before,
                "fixture changed during scan",
            )?;
            if spec.production {
                require(
                    json!(physical.ordinals)
                        == spec.table["file_evidence"][index]["matching_ordinals"],
                    "fixture rows/match geometry changed",
                )?;
                for row in &physical.matches {
                    if !removed(spec, row.key, extras)? {
                        matched = true;
                        spool.push(rules::project(row, &wide, output, false)?)?;
                    }
                }
                result.matches += physical.ordinals.len() as u64;
                physical.rows = count;
                self.cache.files.insert((before, spec.metrics), physical);
            }
            result.rows += count;
            if matched {
                result.matching.push(name.into());
            }
        }
        let sorted = spool.finish()?;
        let mut reference = self.decoder.open(reference, &[])?;
        output.check(reference.schema().as_ref())?;
        sort::compare(
            sorted,
            Rows::new(reference.by_ref(), output),
            expected,
            false,
        )?;
        reference.finish()?;
        Ok(result)
    }
}

#[cfg(test)]
mod tests;

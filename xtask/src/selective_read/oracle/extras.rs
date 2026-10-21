use super::{
    Result, State, files, io, production, require,
    rows::{Layout, Rows},
    rules,
    spec::Spec,
};
use crate::selective_read::{array, fixture_tables, text, workload};
use serde_json::Value;
use std::{collections::BTreeSet, path::Path};

impl State {
    pub(super) fn same_tables(
        &mut self,
        left_root: &Path,
        left: &Value,
        right_root: &Path,
        right: &Value,
    ) -> Result<u64> {
        let mut inputs = Vec::new();
        let mut paths = Vec::new();
        for (root, table) in [(left_root, left), (right_root, right)] {
            let group = files::inside(root, text(&table["path"])?)?;
            let mut group_paths = Vec::new();
            for item in array(&table["files"])? {
                let path = self.digests.verify(&group, item)?;
                inputs.push((path.clone(), files::identity(&path)?));
                group_paths.push(path);
            }
            paths.push(group_paths);
        }
        let mut other = io::Decoder::new(&self.python, &self.harness);
        let mut a = self.decoder.open_paths(&paths[0], &[])?;
        let mut b = other.open_paths(&paths[1], &[])?;
        let projection = a
            .schema()
            .fields()
            .iter()
            .map(|f| f.name().clone())
            .collect::<Vec<_>>();
        let layout = Layout::new(&projection)?;
        layout.check(b.schema().as_ref())?;
        let mut a_rows = Rows::new(a.by_ref(), &layout);
        let mut b_rows = Rows::new(b.by_ref(), &layout);
        let mut count = 0;
        loop {
            match (a_rows.next().transpose()?, b_rows.next().transpose()?) {
                (None, None) => break,
                (Some(a), Some(b)) => {
                    require(
                        a == b,
                        &format!("repacked values/order differ at ordinal {count}"),
                    )?;
                    count += 1;
                }
                _ => return Err("repacked row count differs".into()),
            }
        }
        a.finish()?;
        b.finish()?;
        for (path, before) in inputs {
            require(
                files::identity(&path)? == before,
                "repacked input changed during scan",
            )?;
        }
        Ok(count)
    }
    pub(super) fn extra_keys(
        &mut self,
        fixtures: &Path,
        spec: &Spec,
    ) -> Result<BTreeSet<[i64; 2]>> {
        if spec.production {
            return self.production_extras(fixtures, &spec.manifest);
        }
        let Some(pair) = spec.manifest.get("wide_file_pair") else {
            return Ok(BTreeSet::new());
        };
        workload::run(workload::Request::WideGeometry {
            manifest: spec.manifest.clone(),
            large: pair["kind"] == "large",
            smoke: spec.manifest["profile"] == "smoke",
        })?;
        let tables = array(&spec.manifest["tables"])?;
        let find = |id: &str| {
            tables
                .iter()
                .find(|t| t["id"] == id)
                .ok_or_else(|| format!("missing wide organization: {id}"))
        };
        let normal = find("wide.clustered")?;
        let repacked = find("wide.files4096")?;
        require(
            self.same_tables(fixtures, normal, fixtures, repacked)?
                == rules::integer(&normal["rows"])?
                && normal["rows"] == repacked["rows"],
            "wide repack lost rows",
        )?;
        let groups = if pair["kind"] == "large" {
            vec!["wide.clustered", "wide.files4096"]
        } else {
            vec!["wide.files4096"]
        };
        require(
            pair["shared_organizations"] == serde_json::json!(groups),
            "wrong deletion-union organizations",
        )?;
        let predicates = rules::conditions("eq2-in20", &spec.literals)?;
        let layout = Layout::new(&rules::wide())?;
        let mut extras = BTreeSet::new();
        let mut matches = BTreeSet::new();
        for group in groups {
            let table = find(group)?;
            files::objects(&mut self.digests, fixtures, &spec.manifest, table, None)?;
            for item in array(&table["files"])? {
                let path = files::path(fixtures, table, item)?;
                let before = files::identity(&path)?;
                let mut parquet = self.decoder.open(&path, &[])?;
                layout.check(parquet.schema().as_ref())?;
                let mut minimum = None;
                for row in Rows::new(parquet.by_ref(), &layout) {
                    let row = row?;
                    if rules::prefix(&row, &layout, &predicates)? == predicates.len() {
                        matches.insert(row.key);
                    } else {
                        minimum = Some(minimum.map_or(row.key, |old: [i64; 2]| old.min(row.key)));
                    }
                }
                parquet.finish()?;
                require(
                    files::identity(&path)? == before,
                    "DV input changed during scan",
                )?;
                extras.insert(minimum.ok_or("file has no nonmatching logical key")?);
            }
        }
        extras.insert(
            *matches
                .first()
                .ok_or("wide file pair has no matching row")?,
        );
        require(
            matches
                .iter()
                .any(|key| !extras.contains(key) && !rules::deleted(*key)),
            "wide file pair has no surviving matching row",
        )?;
        require(
            extras.iter().copied().collect::<Vec<_>>()
                == array(&pair["extra_logical_keys"])?
                    .iter()
                    .map(rules::key)
                    .collect::<Result<Vec<_>>>()?,
            "saved deletion union differs from independent file minima",
        )?;
        for table in tables {
            if table["deletion_vectors"] == true {
                require(
                    table["deletion_summary"]["extra_logical_keys"] == pair["extra_logical_keys"],
                    "DV organizations use different deletion sets",
                )?;
            }
        }
        Ok(extras)
    }
    fn production_extras(
        &mut self,
        fixtures: &Path,
        manifest: &Value,
    ) -> Result<BTreeSet<[i64; 2]>> {
        let declared = &manifest["production_dv"];
        let saved = array(&declared["extra_logical_keys"])?
            .iter()
            .map(rules::key)
            .collect::<Result<Vec<_>>>()?;
        let bases: Vec<_> = fixture_tables(manifest)?
            .iter()
            .filter(|t| t["deletion_vectors"] == false)
            .collect();
        let inventory = bases
            .iter()
            .map(|t| Ok(text(&t["id"])?.to_owned()))
            .collect::<Result<BTreeSet<_>>>()?;
        require(
            inventory.iter().cloned().collect::<Vec<_>>()
                == rules::strings(&declared["base_case_ids"])?,
            "shared DV union inventory changed",
        )?;
        let cases = workload::cases();
        let shapes = bases
            .iter()
            .map(|t| text(&cases[text(&t["id"])?]["shape"]))
            .collect::<Result<BTreeSet<_>>>()?;
        let layout = &declared["layout"];
        require(
            layout.is_null() || layout == "localized" || layout == "scattered",
            "unknown pairing layout",
        )?;
        let expected = cases
            .as_object()
            .ok_or("invalid workload cases")?
            .iter()
            .filter(|(_, v)| {
                v["deletion_vectors"] == false
                    && v["shape"].as_str().is_some_and(|s| shapes.contains(s))
                    && (layout.is_null() || &v["layout"] == layout)
            })
            .map(|(k, _)| k.clone())
            .collect::<BTreeSet<_>>();
        require(
            !shapes.is_empty() && inventory == expected,
            "shared DV union inventory does not match requested layout scope",
        )?;
        let mut paths = Vec::new();
        for table in &bases {
            for file in array(&table["files"])? {
                paths.push(files::path(fixtures, table, file)?);
            }
        }
        let identities = paths
            .iter()
            .map(|p| files::identity(p))
            .collect::<Result<Vec<_>>>()?;
        if let Some(extras) = self.extras.get(&identities) {
            require(
                extras.iter().copied().collect::<Vec<_>>() == saved,
                "shared production deletion union changed",
            )?;
            return Ok(extras.clone());
        }
        let projection =
            ["l_orderkey", "l_linenumber", "l_shipdate", "l_shipmode"].map(str::to_owned);
        let layout = Layout::new(&projection)?;
        let mut extras = BTreeSet::new();
        let mut minimum_match = None;
        for path in &paths {
            let mut parquet = self.decoder.open(path, &projection)?;
            let mut minimum = None;
            for batch in parquet.by_ref() {
                let batch = layout.normalize(batch?)?;
                let selected = production::stages(&batch)?;
                for (key, stage) in production::keys(&batch)?.zip(selected) {
                    let target = if stage == 3 {
                        &mut minimum_match
                    } else {
                        &mut minimum
                    };
                    *target = Some(target.map_or(key, |old: [i64; 2]| old.min(key)));
                }
            }
            parquet.finish()?;
            extras.insert(minimum.ok_or("DV base file has no nonmatching row")?);
        }
        extras.insert(minimum_match.ok_or("DV bases have no matching rows")?);
        require(
            extras.iter().copied().collect::<Vec<_>>() == saved,
            "shared production deletion union changed",
        )?;
        require(
            paths
                .iter()
                .map(|p| files::identity(p))
                .collect::<Result<Vec<_>>>()?
                == identities,
            "DV union inputs changed during scan",
        )?;
        self.extras.insert(identities, extras.clone());
        Ok(extras)
    }
}

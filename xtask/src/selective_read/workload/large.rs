use super::*;
use crate::selective_read::{campaign, storage::read_json};

const SCALES: [f64; 5] = [1.0, 10.0, 30.0, 100.0, 300.0];

pub(super) fn shapes() -> Value {
    let wide: Vec<_> = [
        "l_orderkey",
        "l_linenumber",
        "l_shipdate",
        "l_shipmode",
        "l_partkey",
    ]
    .into_iter()
    .map(str::to_owned)
    .chain(payloads())
    .collect();
    json!({"all-wide":[wide,"all",null],"date30-wide":[wide,"date30",null],"date7-wide":[wide,"date7",null],
        "eq1":[wide,"eq1",null],"eq2":[wide,"eq2",null],"eq2-in20":[wide,"eq2-in20",null],
        "eq2-in20-keys":[["l_orderkey","l_linenumber"],"eq2-in20",null]})
}

pub(super) fn definitions(family: &str, pilot_case: Option<&str>) -> Result<Value> {
    if family == "production" {
        return Ok(json!(
            object(&cases())?
                .iter()
                .map(|(case, row)| (case.clone(), json!(["production", case, row["shape"]])))
                .collect::<Map<_, _>>()
        ));
    }
    require(
        ["data", "files", "pilot"].contains(&family),
        "unknown workload family",
    )?;
    require(
        family == "pilot" || pilot_case.is_none(),
        "case selection requires the pilot family",
    )?;
    if family == "pilot" {
        let case = pilot_case.unwrap_or("large.wide.shuffled.date30-wide");
        let declared = definitions("data", None)?;
        require(
            declared.get(case).is_some_and(|v| v[0] == "data") && !case.ends_with(".dv"),
            "pilot requires a declared no-DV data case",
        )?;
        return Ok(json!({case:declared[case]}));
    }
    let mut cases = Map::new();
    if family == "files" {
        for (name, fixture) in [
            ("files.normal", "wide.clustered"),
            ("files4096", "wide.files4096"),
        ] {
            for suffix in ["", ".dv"] {
                cases.insert(
                    format!("large.{name}.eq2-in20{suffix}"),
                    json!(["files", format!("{fixture}{suffix}"), "eq2-in20"]),
                );
            }
        }
    } else {
        for layout in ["clustered", "shuffled"] {
            for name in object(&shapes())?.keys() {
                cases.insert(
                    format!("large.wide.{layout}.{name}"),
                    json!(["data", format!("wide.{layout}"), name]),
                );
            }
            cases.insert(
                format!("large.wide.{layout}.date30-wide.dv"),
                json!(["data", format!("wide.{layout}.dv"), "date30-wide"]),
            );
        }
        cases.insert(
            "scale-control.wide.shuffled.date30-wide".into(),
            json!(["control", "wide.shuffled", "date30-wide"]),
        );
        cases.insert(
            "scale-control.wide.clustered.eq2-in20".into(),
            json!(["control", "wide.clustered", "eq2-in20"]),
        );
    }
    Ok(json!(cases))
}

fn sessions(family: &str, expected: &Value) -> Value {
    if family == "files" {
        return json!({"reuse.large.files4096":"large.files4096.eq2-in20","reuse.large.files4096.dv":"large.files4096.eq2-in20.dv"});
    }
    json!(
        [
            ("reuse.large.date30", "large.wide.shuffled.date30-wide"),
            ("reuse.large.compound", "large.wide.clustered.eq2-in20")
        ]
        .into_iter()
        .filter(|(_, case)| expected.get(case).is_some())
        .map(|(session, case)| (session.to_owned(), json!(case)))
        .collect::<Map<_, _>>()
    )
}

fn sql(projection: &Value, predicate: &str, literals: &[Value]) -> Result<String> {
    let columns = array(projection)?
        .iter()
        .map(text)
        .collect::<Result<Vec<_>>>()?
        .join(", ");
    let mut sql = format!("SELECT {columns} FROM bench");
    let date = "l_shipdate = DATE '1995-03-15'";
    let compound = format!("{date} AND l_shipmode = 'AIR'");
    let filter = match predicate {
        "all" => return Ok(sql),
        "date30" => "l_shipdate >= DATE '1995-03-01' AND l_shipdate < DATE '1995-03-31'".into(),
        "date7" => "l_shipdate >= DATE '1995-03-15' AND l_shipdate < DATE '1995-03-22'".into(),
        "eq1" => date.into(),
        "eq2" => compound,
        "eq2-in20" => format!(
            "{compound} AND l_partkey IN ({})",
            literals
                .iter()
                .map(Value::to_string)
                .collect::<Vec<_>>()
                .join(", ")
        ),
        _ => return Err("unknown large predicate".into()),
    };
    sql.push_str(" WHERE ");
    sql.push_str(&filter);
    Ok(sql)
}

pub(super) fn query(case: &str, manifest: &Value) -> Result<Value> {
    let mut declared = definitions("data", None)?;
    declared
        .as_object_mut()
        .ok_or("invalid workload definitions")?
        .extend(object(&definitions("files", None)?)?.clone());
    let definition = field(&declared, case)?;
    let (role, fixture, name) = (
        text(&definition[0])?,
        text(&definition[1])?,
        text(&definition[2])?,
    );
    let table = array(&manifest["tables"])?
        .iter()
        .find(|t| t["id"] == fixture)
        .ok_or("missing large table")?;
    let source = array(&manifest["sources"])?
        .iter()
        .find(|s| {
            s["scale_factor"].as_f64() == table["scale_factor"].as_f64()
                && s["scale_factor"].is_number()
        })
        .ok_or("missing large source")?;
    let shape = shapes()[name].clone();
    for key in ["path", "file_count", "bytes"] {
        field(table, key)?;
    }
    field(source, "rows")?;
    object(&source["wide_queries"])?;
    let projection = array(&shape[0])?;
    let predicate = text(&shape[1])?;
    let literals = array(&source["in_literals"])?;
    let numbers = literals
        .iter()
        .map(|n| n.as_i64().ok_or("invalid frozen literals"))
        .collect::<std::result::Result<Vec<_>, _>>()?;
    require(
        !numbers.is_empty() && numbers.windows(2).all(|pair| pair[0] < pair[1]),
        "invalid frozen literals",
    )?;
    require(
        numbers.len() == 20 || manifest["profile"] == "smoke" && numbers.len() < 20,
        "wrong literal count",
    )?;
    let dv = fixture.ends_with(".dv");
    require(
        table["deletion_vectors"] == dv
            && table["snapshot_version"].as_u64() == Some(u64::from(dv)),
        "wrong large snapshot/DV state",
    )?;
    let sql = sql(&shape[0], predicate, literals)?;
    if let Some(expected) = source["wide_queries"].get(name) {
        require(*expected == sql, "source SQL differs from large query")?;
    }
    let query_case = format!(
        "{}.{name}{}",
        fixture.strip_suffix(".dv").unwrap_or(fixture),
        if dv { ".dv" } else { "" }
    );
    if dv {
        require(
            table["queries"][&query_case] == sql,
            "DV SQL differs from large query",
        )?;
    }
    let columns = match predicate {
        "all" => vec![],
        "eq2" => vec!["l_shipdate", "l_shipmode"],
        "eq2-in20" => vec!["l_shipdate", "l_shipmode", "l_partkey"],
        _ => vec!["l_shipdate"],
    };
    let files = array(&table["files"])?;
    for file in files {
        field(file, "bytes")?;
    }
    Ok(
        json!({"case_id":case,"role":role,"query_case_id":query_case,"fixture_id":fixture,"fixture_path":table["path"],
        "snapshot_version":table["snapshot_version"],"deletion_vectors":dv,"scale_factor":source["scale_factor"],"source_rows":source["rows"],
        "layout":fixture.split('.').nth(1),"in_literals":literals,"projection":projection,"predicate":predicate,"predicate_columns":columns,
        "projection_columns":projection.len(),"predicate_only_columns":columns.iter().filter(|c| !projection.contains(&json!(c))).collect::<Vec<_>>(),
        "limit":null,"canonical_sql":sql,
        "geometry":{"files":table["file_count"],"physical_bytes":table["bytes"],"file_bytes":files.iter().map(|f| &f["bytes"]).collect::<Vec<_>>(),
            "row_groups":files.iter().map(|f| Ok(array(&f["row_groups"])?.len())).collect::<Result<Vec<_>>>()?}}),
    )
}

fn scales(value: &Value, family: &str) -> Result<()> {
    let scales = object(&value["scales"])?;
    let roles = match family {
        "pilot" => vec!["data"],
        "files" => vec!["files"],
        _ => vec!["data", "control"],
    };
    require(
        scales.len() == roles.len() && roles.iter().all(|r| scales.contains_key(*r)),
        "invalid source-scale controls",
    )?;
    if value["scope"] == "smoke" {
        return require(
            scales.values().all(|v| v.as_f64() == Some(0.01)),
            "invalid smoke scale",
        );
    }
    let data = scales[roles[0]].as_f64().ok_or("invalid source scale")?;
    let index = SCALES
        .iter()
        .position(|scale| *scale == data)
        .ok_or("invalid source scale")?;
    match family {
        "pilot" => Ok(()),
        "files" => require(index > 0, "invalid file-pair scale"),
        _ => require(
            index > 0 && scales["control"].as_f64() == Some(SCALES[index - 1]),
            "invalid source-scale controls",
        ),
    }
}

pub(super) fn load(value: Value, harness: &Path) -> Result<Value> {
    require(
        value["format"] == "selective-read-large-workload-v1"
            && [json!(3), json!(4)].contains(&value["comparison_revision"]),
        "unknown large workload",
    )?;
    let family = value.get("family").map(text).transpose()?.unwrap_or("data");
    require(
        (value["scope"] == "smoke"
            || value["scope"]
                == if family == "pilot" {
                    "pilot"
                } else {
                    "candidate"
                })
            && value["publication_ready"] == false,
        "formal workload freeze is not implemented by this slice",
    )?;
    let expected = definitions(
        family,
        value
            .get("pilot_case")
            .filter(|v| !v.is_null())
            .map(text)
            .transpose()?,
    )?;
    require(
        value["readers"] == json!(campaign::roster(&json!({}))?)
            && value["sessions"] == sessions(family, &expected),
        "workload readers/sessions changed",
    )?;
    let rows = array(&value["cases"])?;
    let actual = rows
        .iter()
        .map(|r| Ok(text(&r["case_id"])?.to_owned()))
        .collect::<Result<BTreeSet<_>>>()?;
    let wanted: BTreeSet<_> = object(&expected)?.keys().cloned().collect();
    require(
        rows.len() == wanted.len() && actual == wanted,
        "incomplete workload family",
    )?;
    scales(&value, family)?;
    for row in rows {
        let definition = field(&expected, text(&row["case_id"])?)?;
        let shape = shapes()[text(&definition[2])?].clone();
        require(
            row["role"] == definition[0]
                && row["fixture_id"] == definition[1]
                && row["scale_factor"].as_f64() == value["scales"][text(&definition[0])?].as_f64()
                && row.get("projection") == Some(&shape[0])
                && row.get("predicate") == Some(&shape[1])
                && row.get("limit") == Some(&shape[2]),
            "large workload shape or scale changed",
        )?;
    }
    let limits = object(&value["oracle_limits"])?;
    require(
        limits.len() == 3
            && limits.values().all(|v| v.as_u64().is_some_and(|n| n > 0))
            && limits.get("memory_bytes") == Some(&json!(16 * 1024u64.pow(3)))
            && limits
                .get("disk_bytes")
                .and_then(Value::as_u64)
                .is_some_and(|n| n >= 9 * 1024 * 1024)
            && limits
                .get("elapsed_seconds")
                .and_then(Value::as_u64)
                .is_some_and(|n| n < 1 << 31),
        "invalid oracle ceilings",
    )?;
    let translations = object(&value["translations"])?;
    require(
        translations.len() == 2
            && ["polars", "daft"]
                .iter()
                .all(|r| translations.contains_key(*r)),
        "missing native translations",
    )?;
    for (reader, translation) in translations {
        require(
            translation["lock_sha256"]
                == digest(&harness.join("runners").join(reader).join("lock.json"))?,
            "translation lock changed",
        )?;
        let expressions = object(&translation["expressions"])?;
        require(
            expressions.keys().cloned().collect::<BTreeSet<_>>() == wanted,
            "incomplete native translations",
        )?;
        for (case, expression) in expressions {
            let row = rows
                .iter()
                .find(|row| row["case_id"] == *case)
                .ok_or("missing translated case")?;
            require(
                expression["native_expression"]
                    .as_object()
                    .is_some_and(|v| !v.is_empty())
                    && !text(&row["canonical_sql"])?.trim().is_empty()
                    && expression["canonical_sql"] == row["canonical_sql"],
                "native expression or SQL binding invalid",
            )?;
        }
    }
    Ok(value)
}

pub(super) fn binding(value: &Value, fixtures: &Path, case: &str) -> Result<Value> {
    let row = array(&value["cases"])?
        .iter()
        .find(|row| row["case_id"] == case)
        .ok_or("missing workload case")?;
    let path = fixtures.join("manifest.json");
    require(
        row["fixture_manifest_sha256"] == digest(&path)?,
        "workload fixture identity changed",
    )?;
    let manifest = read_json(&path)?;
    require(
        object(&query(case, &manifest)?)?
            .iter()
            .all(|(k, v)| row.get(k) == Some(v)),
        "workload SQL, literals, scale or geometry changed",
    )?;
    if value["family"] == "files" {
        super::wide::geometry(&manifest, true, value["scope"] == "smoke")?;
    }
    Ok(row.clone())
}

#[cfg(test)]
mod tests;

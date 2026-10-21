use super::*;
use parquet::{arrow::ArrowWriter, basic::Compression, file::properties::WriterProperties};
use std::fs::File;

pub(crate) fn write(path: &Path, layout: &Layout, rows: &[Row]) -> Result<()> {
    let mut writer = ArrowWriter::try_new(
        File::create(path)?,
        layout.schema.clone(),
        Some(
            WriterProperties::builder()
                .set_compression(Compression::ZSTD(Default::default()))
                .set_max_row_group_row_count(Some(32))
                .build(),
        ),
    )?;
    for rows in rows.chunks(8192) {
        writer.write(&layout.batch(rows)?)?;
    }
    writer.close()?;
    Ok(())
}
pub(crate) fn originals() -> Result<Vec<Row>> {
    (1..=152)
        .map(|n| {
            let day = match n {
                141 => "1995-03-21",
                142 => "1995-03-22",
                143 => "1994-01-01",
                144 => "1994-12-31",
                145 => "1995-01-01",
                146..=149 => "1994-07-01",
                150 => "1993-12-31",
                _ => "1995-03-15",
            };
            Ok(Row {
                key: [n, 1],
                values: vec![
                    Cell::Int(n),
                    Cell::Int(n),
                    Cell::Int(10),
                    Cell::Int(1),
                    Cell::Decimal(match n {
                        143 | 149 => 2399,
                        148 => 2400,
                        _ => 1700,
                    }),
                    Cell::Decimal(123456),
                    Cell::Decimal(match n {
                        143 => 5,
                        144 => 7,
                        146 => 4,
                        147 => 8,
                        _ => 6,
                    }),
                    Cell::Decimal(2),
                    Cell::Text("N".into()),
                    Cell::Text("O".into()),
                    rules::date(day)?,
                    rules::date("1995-03-16")?,
                    rules::date("1995-03-20")?,
                    Cell::Text("DELIVER IN PERSON".into()),
                    Cell::Text(
                        match n {
                            151 => "SHIP",
                            152 => "RAIL",
                            _ => "AIR",
                        }
                        .into(),
                    ),
                    Cell::Text("text with trailing spaces  ".into()),
                ],
            })
        })
        .collect()
}
pub(crate) fn cases(wide: bool) -> &'static [&'static str] {
    if wide {
        &[
            "eq1",
            "eq2",
            "eq2-in1",
            "eq2-in20",
            "eq2-in20-keys",
            "all-wide",
        ]
    } else {
        &[
            "all-keys",
            "all-full",
            "empty",
            "date7-full",
            "date7-keys",
            "date7-limit",
            "q6-scan",
            "eq2-in1",
            "eq2-in20",
        ]
    }
}
pub(crate) fn shape(query: &str, wide: bool) -> (Vec<String>, &str, Option<u64>) {
    let projection = if query.ends_with("-keys") {
        vec!["l_orderkey".into(), "l_linenumber".into()]
    } else if wide {
        rules::wide69()
    } else if query == "q6-scan" {
        vec![
            "l_orderkey".into(),
            "l_linenumber".into(),
            "l_extendedprice".into(),
            "l_discount".into(),
        ]
    } else {
        rules::original()
    };
    let predicate = match query {
        "all-keys" | "all-full" | "all-wide" => "all",
        "date7-full" | "date7-keys" | "date7-limit" => "date7",
        "q6-scan" => "q6",
        "eq2-in20-keys" => "eq2-in20",
        other => other,
    };
    (
        projection,
        predicate,
        if query == "date7-limit" {
            Some(100)
        } else {
            None
        },
    )
}
pub(crate) fn expected(query: &str) -> Vec<i64> {
    match query {
        "all-keys" | "all-full" | "all-wide" => (1..=152).collect(),
        "empty" => vec![],
        "date7-full" | "date7-keys" | "date7-limit" => (1..=141).chain([151, 152]).collect(),
        "q6-scan" => vec![143, 144, 149],
        "eq1" => (1..=140).chain([151, 152]).collect(),
        "eq2" => (1..=140).collect(),
        "eq2-in1" => vec![1],
        _ => (1..=20).collect(),
    }
}
fn json_cell(cell: &Cell, field: &arrow_schema::Field) -> Value {
    match cell {
        Cell::Null => Value::Null,
        Cell::Text(s) => json!(s),
        Cell::Decimal(n) => json!(format!("{}.{:02}", n / 100, n.abs() % 100)),
        Cell::Int(n) if field.data_type() == &arrow_schema::DataType::Date32 => json!(
            arrow_array::types::Date32Type::to_naive_date_opt(*n as i32)
                .unwrap()
                .to_string()
        ),
        Cell::Int(n) => json!(n),
    }
}
pub(crate) fn group(
    root: &Path,
    name: &str,
    layout: &Layout,
    rows: &[Row],
    delta: bool,
) -> Result<Value> {
    let directory = root.join(name);
    fs::create_dir_all(&directory)?;
    let mut files = Vec::new();
    for (index, rows) in rows.chunks(64).enumerate() {
        let path = directory.join(format!("part-{index:05}.parquet"));
        write(&path, layout, rows)?;
        let mut stats =
            json!({"numRecords":rows.len(),"minValues":{},"maxValues":{},"nullCount":{}});
        for (i, field) in layout.schema.fields().iter().enumerate() {
            let values = rows
                .iter()
                .map(|r| &r.values[i])
                .filter(|c| **c != Cell::Null)
                .collect::<Vec<_>>();
            stats["minValues"][field.name()] =
                json_cell(values.iter().min().copied().unwrap_or(&Cell::Null), field);
            stats["maxValues"][field.name()] =
                json_cell(values.iter().max().copied().unwrap_or(&Cell::Null), field);
            stats["nullCount"][field.name()] = json!(rows.len() - values.len());
        }
        files.push(json!({"path":path.file_name().unwrap().to_str().unwrap(),"rows":rows.len(),"bytes":fs::metadata(&path)?.len(),"sha256":digest(&path)?,"delta_stats":stats,"row_groups":[]}));
    }
    let mut group = json!({"path":name,"files":files,"rows":rows.len(),"scale_factor":0.01,"delta_log":null,"file_count":files.len(),"bytes":files.iter().map(|f|f["bytes"].as_u64().unwrap()).sum::<u64>()});
    if delta {
        group["id"] = json!(name);
        group["snapshot_version"] = json!(0);
        group["deletion_vectors"] = json!(false);
        rewrite_log(root, &mut group)?;
    }
    Ok(group)
}
pub(crate) fn rewrite_log(root: &Path, group: &mut Value) -> Result<()> {
    let path = root
        .join(group["path"].as_str().unwrap())
        .join("_delta_log/00000000000000000000.json");
    fs::create_dir_all(path.parent().unwrap())?;
    let mut actions =
        vec![json!({"protocol":{"minReaderVersion":1,"minWriterVersion":2}}).to_string()];
    for f in group["files"].as_array().unwrap() {
        actions.push(json!({"add":{"path":f["path"],"size":f["bytes"],"stats":f["delta_stats"].to_string()}}).to_string());
    }
    fs::write(&path, actions.join("\n") + "\n")?;
    group["delta_log"] = json!({"path":"_delta_log/00000000000000000000.json","bytes":fs::metadata(&path)?.len(),"sha256":digest(&path)?});
    Ok(())
}
pub(crate) fn dataset(root: &Path) -> Result<Value> {
    fs::create_dir_all(root)?;
    let original = originals()?;
    let input = Layout::new(&rules::original())?;
    let wide = Layout::new(&rules::wide())?;
    let widened = original
        .iter()
        .map(|r| rules::project(r, &input, &wide, true))
        .collect::<Result<Vec<_>>>()?;
    let mut source = group(root, "source", &input, &original, false)?;
    let literals = (1..=20).collect::<Vec<_>>();
    source["in_literals"] = json!(literals);
    for (name, is_wide) in [("queries", false), ("wide_queries", true)] {
        source[name] = json!({});
        for query in cases(is_wide) {
            let (columns, predicate, limit) = shape(query, is_wide);
            source[name][query] = json!(rules::sql(&columns, predicate, limit, &literals)?);
        }
    }
    let mut tables = Vec::new();
    for (family, layout, rows) in [("li", &input, original), ("wide", &wide, widened)] {
        for organization in ["clustered", "shuffled"] {
            let mut rows = rows.clone();
            if organization == "shuffled" {
                rows.reverse();
            } else {
                rows.sort_by(|a, b| {
                    (&a.values[10], &a.values[14], &a.values[1]).cmp(&(
                        &b.values[10],
                        &b.values[14],
                        &b.values[1],
                    ))
                });
            }
            tables.push(group(
                root,
                &format!("{family}.{organization}"),
                layout,
                &rows,
                true,
            )?);
        }
    }
    let manifest = json!({"status":"complete","protocol":"selective-read-v1","protocol_sha256":"0".repeat(64),"profile":"smoke","sources":[source],"tables":tables});
    save(&root.join("manifest.json"), &manifest)?;
    Ok(manifest)
}

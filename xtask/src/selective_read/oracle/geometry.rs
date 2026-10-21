use super::{
    Result, files, require,
    rules::{self, integer},
};
use crate::selective_read::{array, text};
use parquet::{basic::Compression, file::metadata::ParquetMetaDataReader};
use serde_json::{Value, json};
use std::{collections::BTreeSet, fs::File, path::Path};

pub(super) fn control(root: &Path, table: &Value, case: &str) -> Result<Value> {
    let row_groups = case == "row-groups.select";
    let (files, groups) = if row_groups { (16, 16) } else { (1, 2) };
    require(
        integer(&table["file_count"])? == files
            && array(&table["files"])?.len() as u64 == files
            && integer(&table["rows"])? == files * groups * 4096,
        "wrong control dimensions",
    )?;
    let (mut candidates, mut candidate_pages, mut pages) = (0, 0, 0);
    let mut columns = rules::control();
    columns.insert(1, "event_id".into());
    for item in array(&table["files"])? {
        let stats = &item["delta_stats"];
        require(
            stats["numRecords"] == item["rows"]
                && integer(&item["rows"])? == groups * 4096
                && stats["nullCount"]["event_id"] == 0
                && stats["minValues"]["event_id"] == "match"
                && stats["maxValues"]["event_id"] == "other",
            "control file can be pruned",
        )?;
        let file = File::open(files::path(root, table, item)?)?;
        let metadata = ParquetMetaDataReader::new().parse_and_finish(&file)?;
        require(
            metadata.num_row_groups() as u64 == groups
                && array(&item["row_groups"])?.len() as u64 == groups,
            "wrong control row groups",
        )?;
        for (g, group) in array(&item["row_groups"])?.iter().enumerate() {
            let actual = metadata.row_group(g);
            require(
                integer(&group["first_row"])? == g as u64 * 4096
                    && group["rows"] == 4096
                    && actual.num_rows() == 4096,
                "wrong control group boundary",
            )?;
            let low = if !row_groups || g == 7 {
                "match"
            } else {
                "other"
            };
            let high = if row_groups { low } else { "other" };
            let hex = |s: &str| {
                s.as_bytes()
                    .iter()
                    .map(|b| format!("{b:02x}"))
                    .collect::<String>()
            };
            candidates += u64::from(low == "match");
            let expected = json!({"nulls":0,"min_hex":hex(low),"max_hex":hex(high)});
            require(
                array(&group["columns"])?
                    .iter()
                    .map(|c| text(&c["column"]))
                    .collect::<Result<Vec<_>>>()?
                    == columns,
                "predicate/payload columns changed",
            )?;
            for (c, column) in array(&group["columns"])?.iter().enumerate() {
                let actual = actual.column(c);
                require(
                    actual.column_index_offset().is_some()
                        && actual.offset_index_offset().is_some()
                        && integer(&column["column_index"]["length"])? > 0
                        && integer(&column["offset_index"]["length"])? > 0,
                    "control index missing",
                )?;
                if !row_groups {
                    require(
                        actual.compression() == Compression::UNCOMPRESSED
                            && column["compression"] == "UNCOMPRESSED"
                            && actual.dictionary_page_offset().is_none(),
                        "page writer settings changed",
                    )?;
                    let pages = array(&column["pages"])?;
                    require(
                        pages.len() == 32
                            && pages
                                .iter()
                                .enumerate()
                                .all(|(i, p)| p["first_row"] == i * 128 && p["rows"] == 128),
                        "wrong 128-row page boundaries",
                    )?;
                }
                if c != 1 {
                    continue;
                }
                let stats = actual.statistics().ok_or("missing predicate statistics")?;
                require(
                    stats.min_bytes_opt() == Some(low.as_bytes())
                        && stats.max_bytes_opt() == Some(high.as_bytes())
                        && stats.null_count_opt() == Some(0)
                        && column["statistics"] == expected,
                    "unexpected predicate group pruning",
                )?;
                for page in array(&column["pages"])? {
                    let page_low = if row_groups {
                        low
                    } else if case == "pages.scattered" || page["first_row"] == 0 {
                        "match"
                    } else {
                        "other"
                    };
                    require(
                        page["bounds"] == json!({"min_hex":hex(page_low),"max_hex":hex(high)})
                            && page["nulls"] == 0
                            && page["all_null"] == false,
                        "unexpected predicate page pruning",
                    )?;
                    pages += 1;
                    candidate_pages += u64::from(page_low == "match");
                }
            }
        }
    }
    Ok(
        json!({"row_groups":files*groups,"candidate_row_groups":candidates,"predicate_pages":pages,"candidate_predicate_pages":candidate_pages,
        "predicate_columns":["event_id"],"output_columns":rules::control(),"decoded_row_groups":null,"decoded_pages":null,
        "counter_unavailable_reason":"geometry describes opportunities; common adapters do not expose comparable decoded group/page counters"}),
    )
}
pub(super) fn coverage(table: &Value, candidates: &[String], matching: &[String]) -> Result<Value> {
    let files = array(&table["files"])?;
    let all: BTreeSet<_> = files
        .iter()
        .map(|f| text(&f["path"]))
        .collect::<Result<_>>()?;
    let dvs: BTreeSet<_> = files
        .iter()
        .filter(|f| {
            f["deletion_vector"]["physical_ordinals"]
                .as_array()
                .is_some_and(|v| !v.is_empty())
        })
        .map(|f| text(&f["path"]))
        .collect::<Result<_>>()?;
    require(
        table["deletion_vectors"] != true || dvs == all,
        "wide pair requires nonempty real DVs on every file",
    )?;
    let candidates: BTreeSet<_> = candidates.iter().map(String::as_str).collect();
    Ok([("candidate",candidates.clone()),("excluded",all.difference(&candidates).copied().collect()),("matching",matching.iter().map(String::as_str).collect())].into_iter().map(|(name,files)| {
        let dv=files.intersection(&dvs).count();(name.into(),json!({"files":files.len(),"dv_files":dv,"coverage":if files.is_empty() {None} else {Some(dv as f64/files.len() as f64)}}))
    }).collect::<serde_json::Map<_,_>>().into())
}

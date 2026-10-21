use super::{
    Result, require,
    rules::{self, integer},
};
use crate::selective_read::{array, fixture_tables, storage::read_json, text};
use serde_json::{Value, json};
use std::{collections::BTreeSet, path::Path};

pub(super) struct Spec {
    pub manifest: Value,
    pub table: Value,
    pub source: Option<Value>,
    pub case: String,
    pub suffix: String,
    pub projection: Vec<String>,
    pub predicate: String,
    pub limit: Option<u64>,
    pub literals: Vec<i64>,
    pub sql: String,
    pub production: bool,
    pub metrics: usize,
}
pub(super) fn base_case(case: &str, large: bool) -> Result<(&str, &str)> {
    for suffix in [".dv", ".feature-only"] {
        if let Some(base) = case.strip_suffix(suffix) {
            let valid = if suffix == ".feature-only" {
                base == "row-groups.select"
            } else {
                [
                    "li.clustered.date7-full",
                    "li.shuffled.date7-full",
                    "wide.clustered.eq2-in20",
                    "li.clustered.date7-limit",
                    "row-groups.select",
                    "pages.localized",
                    "wide.files4096.eq2-in20",
                ]
                .contains(&base)
                    || large
                        && ["wide.clustered.date30-wide", "wide.shuffled.date30-wide"]
                            .contains(&base)
            };
            require(valid, "unknown DV case")?;
            return Ok((base, suffix));
        }
    }
    Ok((case, ""))
}
fn shape(query: &str, wide: bool, large: bool) -> Result<(Vec<String>, String, Option<u64>)> {
    let keys = vec!["l_orderkey".into(), "l_linenumber".into()];
    let result = if large {
        require(
            [
                "all-wide",
                "date30-wide",
                "date7-wide",
                "eq1",
                "eq2",
                "eq2-in20",
                "eq2-in20-keys",
            ]
            .contains(&query),
            "unknown large query",
        )?;
        (
            if query.ends_with("-keys") {
                keys
            } else {
                rules::wide69()
            },
            query.trim_end_matches("-wide").trim_end_matches("-keys"),
            None,
        )
    } else if wide {
        require(
            [
                "eq1",
                "eq2",
                "eq2-in1",
                "eq2-in20",
                "eq2-in20-keys",
                "all-wide",
            ]
            .contains(&query),
            "unknown wide query",
        )?;
        (
            if query.ends_with("-keys") {
                keys
            } else {
                rules::wide69()
            },
            query.trim_end_matches("-wide").trim_end_matches("-keys"),
            None,
        )
    } else {
        match query {
            "all-keys" => (keys, "all", None),
            "all-full" => (rules::original(), "all", None),
            "empty" => (rules::original(), "empty", None),
            "date7-full" => (rules::original(), "date7", None),
            "date7-keys" => (keys, "date7", None),
            "date7-limit" => (rules::original(), "date7", Some(100)),
            "q6-scan" => (
                [
                    "l_orderkey",
                    "l_linenumber",
                    "l_extendedprice",
                    "l_discount",
                ]
                .map(str::to_owned)
                .into(),
                "q6",
                None,
            ),
            "eq2-in1" | "eq2-in20" => (rules::original(), query, None),
            _ => return Err("unknown public query".into()),
        }
    };
    Ok((result.0, result.1.into(), result.2))
}
impl Spec {
    pub(super) fn load(
        fixtures: &Path,
        case_id: &str,
        duplicate: bool,
        context: &Value,
    ) -> Result<Self> {
        let manifest = read_json(&fixtures.join("manifest.json"))?;
        let production = matches!(
            context["identity"]["comparison_revision"].as_u64(),
            Some(5 | 6)
        );
        let row = &context["case"];
        let large = !context.is_null();
        require(
            !large || !duplicate,
            "large query variants need a separate workload identity",
        )?;
        if production {
            let table = fixture_tables(&manifest)?
                .iter()
                .find(|t| t["id"] == case_id)
                .ok_or("missing production fixture")?
                .clone();
            let source = array(&manifest["sources"])?
                .iter()
                .find(|s| s["scale_factor"] == 10)
                .ok_or("missing SF10 source")?
                .clone();
            let mut projection = rules::wide69();
            let metrics = match text(&row["shape"])? {
                "q2" => 336,
                "q4" => {
                    projection.extend(["l_suppkey".into(), "l_quantity".into()]);
                    10
                }
                _ => return Err("unknown production shape".into()),
            };
            let sql = rules::sql(&projection, "production", None, &[1])?;
            require(
                json!(projection) == row["projection"] && sql == row["canonical_sql"],
                "production query differs from oracle",
            )?;
            return Ok(Self {
                manifest,
                table,
                source: Some(source),
                case: case_id.into(),
                suffix: if case_id.ends_with(".dv") {
                    ".dv".into()
                } else {
                    String::new()
                },
                projection,
                predicate: "production".into(),
                limit: None,
                literals: vec![1],
                sql,
                production,
                metrics,
            });
        }
        require(
            manifest["status"] == "complete" && manifest["protocol"] == "selective-read-v1",
            "incomplete or unknown fixtures",
        )?;
        require(
            large || manifest["profile"] != "large",
            "large fixtures require a workload manifest",
        )?;
        let query = if large {
            text(&row["query_case_id"])?
        } else {
            case_id
        };
        let (case, suffix) = base_case(query, large)?;
        let control = ["row-groups.select", "pages.localized", "pages.scattered"].contains(&case);
        let fixture = match case {
            "row-groups.select" => "row-groups",
            "pages.localized" | "pages.scattered" => case,
            _ => case.rsplit_once('.').ok_or("invalid case")?.0,
        };
        let table = array(&manifest["tables"])?
            .iter()
            .find(|t| t["id"] == format!("{fixture}{suffix}"))
            .ok_or("missing fixture")?
            .clone();
        require(
            integer(&table["snapshot_version"])? == u64::from(!suffix.is_empty())
                && table["deletion_vectors"] == (suffix == ".dv"),
            "unexpected snapshot version or DV flag",
        )?;
        if !suffix.is_empty() {
            require(
                table["base_fixture_id"] == fixture
                    && table["variant"] == suffix[1..]
                    && table["dv_features"] == true,
                "incorrect variant identity",
            )?;
            let base = array(&manifest["tables"])?
                .iter()
                .find(|t| t["id"] == fixture)
                .ok_or("missing base fixture")?;
            require(
                table["schema"] == base["schema"] && table["rows"] == base["rows"],
                "DV pair changed schema or physical rows",
            )?;
            let mut files = table["files"].clone();
            for f in files.as_array_mut().ok_or("missing files")? {
                if f.as_object_mut()
                    .unwrap()
                    .remove("deletion_vector")
                    .is_some()
                {
                    f["delta_stats"]
                        .as_object_mut()
                        .ok_or("missing stats")?
                        .remove("tightBounds");
                }
            }
            require(
                files == base["files"],
                "DV pair changed Parquet objects, statistics, or geometry",
            )?;
        }
        let (source, projection, predicate, limit, literals, mut sql) = if control {
            require(!duplicate, "control predicate has no IN literals")?;
            let projection = rules::control();
            let sql = rules::sql(&projection, "control-match", None, &[])?;
            require(
                table["queries"][query] == sql,
                "control SQL differs from protocol",
            )?;
            (None, projection, "control-match".into(), None, vec![], sql)
        } else {
            let (fixture, query_name) = case.rsplit_once('.').ok_or("invalid case")?;
            require(
                [
                    "li.clustered",
                    "li.shuffled",
                    "wide.clustered",
                    "wide.shuffled",
                    "wide.files4096",
                    "files64",
                    "files4096",
                ]
                .contains(&fixture),
                "unsupported fixture",
            )?;
            require(
                !fixture.starts_with("files") || ["empty", "eq2-in20"].contains(&query_name),
                "unknown file organization query",
            )?;
            let source = array(&manifest["sources"])?
                .iter()
                .find(|s| s["scale_factor"] == table["scale_factor"])
                .ok_or("missing source")?
                .clone();
            let wide = fixture.starts_with("wide.");
            let (projection, predicate, limit) = shape(query_name, wide, large)?;
            let literals = array(&source["in_literals"])?
                .iter()
                .map(|v| v.as_i64().ok_or("invalid frozen IN literal".into()))
                .collect::<Result<Vec<_>>>()?;
            require(
                !literals.is_empty()
                    && literals.windows(2).all(|p| p[0] < p[1])
                    && (literals.len() == 20
                        || manifest["profile"] == "smoke" && literals.len() < 20),
                "invalid frozen IN literals",
            )?;
            let sql = rules::sql(&projection, &predicate, limit, &literals)?;
            require(
                sql == *if large {
                    &row["canonical_sql"]
                } else {
                    &source[if wide { "wide_queries" } else { "queries" }][query_name]
                },
                "manifest SQL differs from frozen case",
            )?;
            if !suffix.is_empty() {
                require(table["queries"][query] == sql, "variant SQL changed")?;
            }
            (Some(source), projection, predicate, limit, literals, sql)
        };
        if duplicate {
            require(
                ["eq2-in1", "eq2-in20"].contains(&predicate.as_str()),
                "duplicate literal requires an IN case",
            )?;
            let (before, _) = sql.rsplit_once(')').ok_or("invalid IN SQL")?;
            sql = format!("{before}, {})", literals[0]);
        }
        Ok(Self {
            manifest,
            table,
            source,
            case: case.into(),
            suffix: suffix.into(),
            projection,
            predicate,
            limit,
            literals,
            sql,
            production,
            metrics: 0,
        })
    }
    pub(super) fn saved_keys(&self) -> Result<BTreeSet<[i64; 2]>> {
        let mut keys = BTreeSet::new();
        for file in array(&self.table["files"])? {
            if let Some(dv) = file.get("deletion_vector") {
                for value in array(&dv["logical_ids"])? {
                    require(
                        keys.insert(rules::key(value)?),
                        "duplicate deleted logical ID",
                    )?;
                }
            }
        }
        Ok(keys)
    }
    pub(super) fn stored(&self) -> Vec<String> {
        if self.source.is_none() {
            let mut projection = rules::control();
            projection.insert(1, "event_id".into());
            projection
        } else if self.production {
            rules::wide()
                .into_iter()
                .chain((0..self.metrics).map(|j| format!("metric_{j:03}")))
                .collect()
        } else if self.table["id"]
            .as_str()
            .is_some_and(|s| s.starts_with("wide."))
        {
            rules::wide()
        } else {
            rules::original()
        }
    }
}

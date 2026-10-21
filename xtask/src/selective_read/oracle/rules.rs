use arrow_array::types::Date32Type;
use serde_json::Value;
#[cfg(test)]
use serde_json::json;
use sha2::{Digest, Sha256};

use super::{
    Result, require,
    rows::{Cell, Layout, Row},
};
use crate::selective_read::{array, text};

pub(super) const ORIGINAL: [&str; 16] = [
    "l_orderkey",
    "l_partkey",
    "l_suppkey",
    "l_linenumber",
    "l_quantity",
    "l_extendedprice",
    "l_discount",
    "l_tax",
    "l_returnflag",
    "l_linestatus",
    "l_shipdate",
    "l_commitdate",
    "l_receiptdate",
    "l_shipinstruct",
    "l_shipmode",
    "l_comment",
];
pub(super) fn original() -> Vec<String> {
    ORIGINAL.map(str::to_owned).into()
}
pub(super) fn wide() -> Vec<String> {
    original().into_iter().chain(payloads()).collect()
}
pub(super) fn payloads() -> impl Iterator<Item = String> {
    (0..64).map(|n| format!("payload_{n:02}"))
}
pub(super) fn wide69() -> Vec<String> {
    [
        "l_orderkey",
        "l_linenumber",
        "l_shipdate",
        "l_shipmode",
        "l_partkey",
    ]
    .map(str::to_owned)
    .into_iter()
    .chain(payloads())
    .collect()
}
pub(super) fn control() -> Vec<String> {
    ["row_id".into()]
        .into_iter()
        .chain((0..16).map(|n| format!("payload_{n:03}")))
        .collect()
}
pub(super) fn strings(value: &Value) -> Result<Vec<String>> {
    array(value)?
        .iter()
        .map(|v| Ok(text(v)?.to_owned()))
        .collect()
}
pub(super) fn integer(value: &Value) -> Result<u64> {
    value.as_u64().ok_or("expected unsigned integer".into())
}
pub(super) fn key(value: &Value) -> Result<[i64; 2]> {
    let parts = array(value)?;
    require((1..=2).contains(&parts.len()), "invalid logical key")?;
    Ok([
        parts[0].as_i64().ok_or("invalid key")?,
        if parts.len() == 2 {
            parts[1].as_i64().ok_or("invalid key")?
        } else {
            0
        },
    ])
}
pub(super) fn date(value: &str) -> Result<Cell> {
    Ok(Cell::Int(i64::from(Date32Type::from_naive_date(
        value.parse()?,
    ))))
}
fn hash64(value: &str) -> u64 {
    u64::from_le_bytes(Sha256::digest(value.as_bytes())[..8].try_into().unwrap())
}
pub(super) fn payload(key: [i64; 2], column: usize) -> Cell {
    let value = hash64(&format!("dar-wide-v1/{}/{}/{column:02}", key[0], key[1]));
    if value.is_multiple_of(17) {
        Cell::Null
    } else {
        Cell::Int((value & 0x7fff_ffff_ffff_ffff) as i64 - 0x4000_0000_0000_0000)
    }
}
pub(super) fn deleted(key: [i64; 2]) -> bool {
    hash64(&format!("{}/{}", key[0], key[1])).is_multiple_of(1000)
}
pub(super) fn control_matches(case: &str, row: i64) -> Result<bool> {
    Ok(match case {
        "row-groups.select" => row / 4096 % 16 == 7,
        "pages.localized" => row % 4096 < 32,
        "pages.scattered" => row % 128 == 0,
        _ => return Err("unknown control layout".into()),
    })
}
pub(super) fn control_row(row: i64) -> Row {
    Row {
        key: [row, 0],
        values: [Cell::Int(row)]
            .into_iter()
            .chain((0..16).map(|j| {
                if (row + j) % 17 == 0 {
                    Cell::Null
                } else {
                    Cell::Text(format!(
                        "payload-{j:03}-{row:08}-{}",
                        "abcdefghijklmnopqrstuvwxyz0123456789".repeat(12)
                    ))
                }
            }))
            .collect(),
    }
}

#[derive(Clone, Debug)]
pub(super) struct Condition {
    pub name: String,
    pub op: &'static str,
    pub values: Vec<Cell>,
}
impl Condition {
    fn new(name: &str, op: &'static str, values: Vec<Cell>) -> Self {
        Self {
            name: name.into(),
            op,
            values,
        }
    }
    fn matches(&self, actual: &Cell) -> bool {
        match self.op {
            "=" => actual == &self.values[0],
            "<" => actual < &self.values[0],
            "<=" => actual <= &self.values[0],
            ">=" => actual >= &self.values[0],
            "IN" => self.values.contains(actual),
            _ => unreachable!(),
        }
    }
    pub(super) fn describe(&self) -> String {
        let render = |v: &Cell| match v {
            Cell::Int(n) if self.name.ends_with("date") => Date32Type::to_naive_date_opt(*n as i32)
                .expect("constant oracle date")
                .to_string(),
            Cell::Int(n) => n.to_string(),
            Cell::Decimal(n) => format!("{}.{:02}", n / 100, n.abs() % 100),
            Cell::Text(s) => s.clone(),
            Cell::Null => "None".into(),
        };
        let value = if self.op == "IN" {
            format!(
                "[{}]",
                self.values
                    .iter()
                    .map(render)
                    .collect::<Vec<_>>()
                    .join(", ")
            )
        } else {
            render(&self.values[0])
        };
        format!("{} {} {value}", self.name, self.op)
    }
}
pub(super) fn conditions(predicate: &str, literals: &[i64]) -> Result<Vec<Condition>> {
    let one = |n, o, v| Condition::new(n, o, vec![v]);
    Ok(match predicate {
        "all" => vec![],
        "control-match" => vec![one("event_id", "=", Cell::Text("match".into()))],
        "empty" => vec![one("l_shipdate", "<", date("1990-01-01")?)],
        "date7" | "date30" => vec![
            one(
                "l_shipdate",
                ">=",
                date(if predicate == "date7" {
                    "1995-03-15"
                } else {
                    "1995-03-01"
                })?,
            ),
            one(
                "l_shipdate",
                "<",
                date(if predicate == "date7" {
                    "1995-03-22"
                } else {
                    "1995-03-31"
                })?,
            ),
        ],
        "q6" => vec![
            one("l_shipdate", ">=", date("1994-01-01")?),
            one("l_shipdate", "<", date("1995-01-01")?),
            one("l_discount", ">=", Cell::Decimal(5)),
            one("l_discount", "<=", Cell::Decimal(7)),
            one("l_quantity", "<", Cell::Decimal(2400)),
        ],
        "eq1" | "eq2" | "eq2-in1" | "eq2-in20" | "production" => {
            let mut result = vec![one("l_shipdate", "=", date("1995-03-15")?)];
            if predicate != "eq1" {
                result.push(one("l_shipmode", "=", Cell::Text("AIR".into())));
            }
            if predicate == "production" {
                result.push(Condition::new("l_linenumber", "IN", vec![Cell::Int(1)]));
            } else if predicate.starts_with("eq2-in") {
                require(!literals.is_empty(), "missing IN literals")?;
                result.push(Condition::new(
                    "l_partkey",
                    "IN",
                    literals
                        .iter()
                        .take(if predicate == "eq2-in1" {
                            1
                        } else {
                            literals.len()
                        })
                        .map(|n| Cell::Int(*n))
                        .collect(),
                ));
            }
            result
        }
        _ => return Err("unknown oracle predicate".into()),
    })
}
pub(super) fn prefix(row: &Row, layout: &Layout, predicates: &[Condition]) -> Result<usize> {
    for (i, condition) in predicates.iter().enumerate() {
        if !condition.matches(&row.values[layout.schema.index_of(&condition.name)?]) {
            return Ok(i);
        }
    }
    Ok(predicates.len())
}
fn bound(value: &Value, name: &str) -> Result<Cell> {
    if name.ends_with("date") {
        return date(text(value)?);
    }
    if ["l_quantity", "l_extendedprice", "l_discount", "l_tax"].contains(&name) {
        // Decimal fixture bounds have scale two; never compare through binary floats.
        let number = value
            .as_str()
            .map(str::to_owned)
            .unwrap_or_else(|| value.to_string());
        let (whole, fraction) = number.split_once('.').unwrap_or((&number, ""));
        require(
            fraction.len() <= 2 && fraction.bytes().all(|c| c.is_ascii_digit()),
            "invalid decimal fixture bound",
        )?;
        let negative = whole.starts_with('-');
        let scaled = whole.parse::<i128>()? * 100;
        let fraction = format!("{fraction:0<2}").parse::<i128>()?;
        return Ok(Cell::Decimal(
            scaled + if negative { -fraction } else { fraction },
        ));
    }
    if let Some(n) = value.as_i64() {
        Ok(Cell::Int(n))
    } else {
        Ok(Cell::Text(text(value)?.into()))
    }
}
pub(super) fn candidate(stats: &Value, predicates: &[Condition]) -> Result<bool> {
    for condition in predicates {
        let name = &condition.name;
        if stats["nullCount"][name] == stats["numRecords"] {
            return Ok(false);
        }
        let low = bound(&stats["minValues"][name], name)?;
        let high = bound(&stats["maxValues"][name], name)?;
        let value = &condition.values[0];
        let possible = match condition.op {
            "=" => low <= *value && *value <= high,
            "<" => low < *value,
            "<=" => low <= *value,
            ">=" => high >= *value,
            "IN" => condition.values.iter().any(|v| low <= *v && *v <= high),
            _ => unreachable!(),
        };
        if !possible {
            return Ok(false);
        }
    }
    Ok(true)
}
pub(super) fn project(row: &Row, source: &Layout, output: &Layout, derive: bool) -> Result<Row> {
    let values = output
        .schema
        .fields()
        .iter()
        .map(|f| {
            if derive && f.name().starts_with("payload_") {
                Ok(payload(row.key, f.name()[8..].parse()?))
            } else {
                Ok(row.values[source.schema.index_of(f.name())?].clone())
            }
        })
        .collect::<Result<_>>()?;
    output.row(values)
}
pub(super) fn logical_bytes(row: &Row, layout: &Layout) -> u64 {
    row.values
        .iter()
        .zip(layout.schema.fields())
        .map(|(v, f)| match v {
            Cell::Null => 0,
            Cell::Text(v) => v.len() as u64,
            Cell::Decimal(_) => 16,
            Cell::Int(_) => {
                if f.data_type() == &arrow_schema::DataType::Int64 {
                    8
                } else {
                    4
                }
            }
        })
        .sum()
}
pub(super) fn sql(
    projection: &[String],
    predicate: &str,
    limit: Option<u64>,
    literals: &[i64],
) -> Result<String> {
    let eq1 = "l_shipdate = DATE '1995-03-15'";
    let eq2 = format!("{eq1} AND l_shipmode = 'AIR'");
    let filter=match predicate {
        "all"=>String::new(), "empty"=>"l_shipdate < DATE '1990-01-01'".into(),
        "date7"=>"l_shipdate >= DATE '1995-03-15' AND l_shipdate < DATE '1995-03-22'".into(),
        "date30"=>"l_shipdate >= DATE '1995-03-01' AND l_shipdate < DATE '1995-03-31'".into(),
        "eq1"=>eq1.into(),"eq2"=>eq2.clone(),
        "eq2-in1"|"eq2-in20"=>{require(!literals.is_empty(),"missing IN literals")?;format!("{eq2} AND l_partkey IN ({})",literals.iter().take(if predicate=="eq2-in1" {1} else {literals.len()}).map(ToString::to_string).collect::<Vec<_>>().join(", "))},
        "production"=>format!("{eq2} AND l_linenumber IN (1)"),
        "control-match"=>"event_id = 'match'".into(),
        "q6"=>"l_shipdate >= DATE '1994-01-01' AND l_shipdate < DATE '1995-01-01' AND l_discount BETWEEN CAST('0.05' AS DECIMAL(15,2)) AND CAST('0.07' AS DECIMAL(15,2)) AND l_quantity < CAST('24.00' AS DECIMAL(15,2))".into(),
        _=>return Err("unknown oracle SQL predicate".into()),
    };
    Ok(format!(
        "SELECT {} FROM bench{}{}",
        projection.join(", "),
        if filter.is_empty() {
            String::new()
        } else {
            format!(" WHERE {filter}")
        },
        limit.map(|v| format!(" LIMIT {v}")).unwrap_or_default()
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn scalar_rules_keep_decimal_boundaries_null_payloads_and_control_layouts() -> Result<()> {
        assert_eq!(bound(&json!("-0.05"), "l_discount")?, Cell::Decimal(-5));
        assert_eq!(bound(&json!(0.07), "l_discount")?, Cell::Decimal(7));
        assert!(bound(&json!(0.001), "l_discount").is_err());
        let conditions = conditions("q6", &[])?;
        let stats = json!({"numRecords":1,"nullCount":{"l_shipdate":0,"l_discount":0,"l_quantity":0},
            "minValues":{"l_shipdate":"1994-01-01","l_discount":0.05,"l_quantity":23.99},
            "maxValues":{"l_shipdate":"1994-12-31","l_discount":0.07,"l_quantity":23.99}});
        assert!(candidate(&stats, &conditions)?);
        let mut excluded = stats.clone();
        excluded["minValues"]["l_quantity"] = json!(24);
        assert!(!candidate(&excluded, &conditions)?);
        assert_eq!(
            (0..8192)
                .filter(|n| control_matches("pages.localized", *n).unwrap())
                .count(),
            64
        );
        assert_eq!(
            (0..8192)
                .filter(|n| control_matches("pages.scattered", *n).unwrap())
                .count(),
            64
        );
        assert_eq!(control_row(0).values[1], Cell::Null);
        assert_eq!(payload([1, 1], 0), Cell::Int(2558623671389681668));
        Ok(())
    }
}

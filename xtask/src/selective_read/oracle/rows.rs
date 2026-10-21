use std::{collections::VecDeque, sync::Arc};

use arrow_array::{
    Array, ArrayRef, Date32Array, Decimal128Array, Int32Array, Int64Array, RecordBatch, StringArray,
};
use arrow_schema::{DataType, Field, Schema, SchemaRef};

use super::{Result, require};

pub(super) const BATCH_ROWS: usize = 8192;

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(super) enum Cell {
    Null,
    Int(i64),
    Decimal(i128),
    Text(String),
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(super) struct Row {
    pub key: [i64; 2],
    pub values: Vec<Cell>,
}

impl Row {
    pub(super) fn bytes(&self) -> usize {
        std::mem::size_of::<Self>()
            + self.values.capacity() * std::mem::size_of::<Cell>()
            + self
                .values
                .iter()
                .map(|c| {
                    if let Cell::Text(s) = c {
                        s.capacity()
                    } else {
                        0
                    }
                })
                .sum::<usize>()
    }
}

pub(super) struct Layout {
    pub schema: SchemaRef,
    keys: Vec<usize>,
}

impl Layout {
    pub(super) fn new(projection: &[String]) -> Result<Self> {
        let fields = projection
            .iter()
            .map(|name| {
                let kind = match name.as_str() {
                    "l_orderkey" | "l_partkey" | "l_suppkey" => DataType::Int64,
                    "l_linenumber" | "row_id" => DataType::Int32,
                    "l_shipdate" | "l_commitdate" | "l_receiptdate" => DataType::Date32,
                    "l_quantity" | "l_extendedprice" | "l_discount" | "l_tax" => {
                        DataType::Decimal128(15, 2)
                    }
                    "l_returnflag" | "l_linestatus" | "l_shipinstruct" | "l_shipmode"
                    | "l_comment" | "event_id" => DataType::Utf8,
                    _ if (0..64).any(|n| name == &format!("payload_{n:02}")) => DataType::Int64,
                    _ if (0..16).any(|n| name == &format!("payload_{n:03}")) => DataType::Utf8,
                    _ if (0..336).any(|n| name == &format!("metric_{n:03}")) => DataType::Int64,
                    _ => return Err(format!("unknown oracle field: {name}").into()),
                };
                Ok(Field::new(
                    name,
                    kind,
                    name.starts_with("payload_") || name.starts_with("metric_"),
                ))
            })
            .collect::<Result<Vec<_>>>()?;
        let schema = Arc::new(Schema::new(fields));
        let keys = if projection.iter().any(|s| s == "row_id") {
            vec![schema.index_of("row_id")?]
        } else {
            vec![
                schema.index_of("l_orderkey")?,
                schema.index_of("l_linenumber")?,
            ]
        };
        Ok(Self { schema, keys })
    }

    pub(super) fn check(&self, schema: &Schema) -> Result<()> {
        require(
            schema.fields().len() == self.schema.fields().len(),
            "output field count differs from projection",
        )?;
        for (actual, expected) in schema.fields().iter().zip(self.schema.fields()) {
            require(
                actual.name() == expected.name(),
                "output field names/order differ from projection",
            )?;
            let mut kind = actual.data_type();
            while let DataType::Dictionary(_, value) = kind {
                kind = value;
            }
            let kind = match kind {
                DataType::LargeUtf8 | DataType::Utf8View => &DataType::Utf8,
                _ => kind,
            };
            require(
                kind == expected.data_type(),
                &format!("wrong logical type for {}: {kind}", expected.name()),
            )?;
        }
        Ok(())
    }

    pub(super) fn normalize(&self, batch: RecordBatch) -> Result<RecordBatch> {
        self.check(batch.schema().as_ref())?;
        let columns = batch
            .columns()
            .iter()
            .zip(self.schema.fields())
            .map(|(array, field)| {
                require(
                    field.is_nullable() || array.null_count() == 0,
                    &format!("unexpected null: {}", field.name()),
                )?;
                Ok(arrow_cast::cast(array, field.data_type())?)
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(RecordBatch::try_new(self.schema.clone(), columns)?)
    }
    pub(super) fn read_row(&self, batch: &RecordBatch, i: usize) -> Result<Row> {
        let values = batch
            .columns()
            .iter()
            .map(|a| -> Result<Cell> {
                if a.is_null(i) {
                    return Ok(Cell::Null);
                }
                Ok(match a.data_type() {
                    DataType::Int64 => Cell::Int(
                        a.as_any()
                            .downcast_ref::<Int64Array>()
                            .ok_or("invalid Int64 array")?
                            .value(i),
                    ),
                    DataType::Int32 => Cell::Int(i64::from(
                        a.as_any()
                            .downcast_ref::<Int32Array>()
                            .ok_or("invalid Int32 array")?
                            .value(i),
                    )),
                    DataType::Date32 => Cell::Int(i64::from(
                        a.as_any()
                            .downcast_ref::<Date32Array>()
                            .ok_or("invalid Date32 array")?
                            .value(i),
                    )),
                    DataType::Decimal128(_, _) => Cell::Decimal(
                        a.as_any()
                            .downcast_ref::<Decimal128Array>()
                            .ok_or("invalid Decimal128 array")?
                            .value(i),
                    ),
                    DataType::Utf8 => Cell::Text(
                        a.as_any()
                            .downcast_ref::<StringArray>()
                            .ok_or("invalid Utf8 array")?
                            .value(i)
                            .to_owned(),
                    ),
                    _ => return Err("unsupported normalized oracle type".into()),
                })
            })
            .collect::<Result<_>>()?;
        self.row(values)
    }
    pub(super) fn rows(&self, batch: RecordBatch) -> Result<Vec<Row>> {
        let batch = self.normalize(batch)?;
        (0..batch.num_rows())
            .map(|i| self.read_row(&batch, i))
            .collect()
    }

    pub(super) fn row(&self, values: Vec<Cell>) -> Result<Row> {
        require(
            values.len() == self.schema.fields().len(),
            "wrong oracle row width",
        )?;
        let mut key = [0; 2];
        for (slot, index) in self.keys.iter().enumerate() {
            let Cell::Int(value) = values[*index] else {
                return Err("invalid logical row key".into());
            };
            key[slot] = value;
        }
        Ok(Row { key, values })
    }

    pub(super) fn batch(&self, rows: &[Row]) -> Result<RecordBatch> {
        let columns = self
            .schema
            .fields()
            .iter()
            .enumerate()
            .map(|(i, field)| -> Result<ArrayRef> {
                let ints = || {
                    rows.iter().map(move |row| {
                        if let Cell::Int(v) = row.values[i] {
                            Some(v)
                        } else {
                            None
                        }
                    })
                };
                Ok(match field.data_type() {
                    DataType::Int64 => Arc::new(Int64Array::from_iter(ints())),
                    DataType::Int32 => Arc::new(Int32Array::from(
                        ints()
                            .map(|v| v.map(i32::try_from).transpose())
                            .collect::<std::result::Result<Vec<_>, _>>()?,
                    )),
                    DataType::Date32 => Arc::new(Date32Array::from(
                        ints()
                            .map(|v| v.map(i32::try_from).transpose())
                            .collect::<std::result::Result<Vec<_>, _>>()?,
                    )),
                    DataType::Decimal128(p, s) => Arc::new(
                        Decimal128Array::from_iter(rows.iter().map(|row| {
                            if let Cell::Decimal(v) = row.values[i] {
                                Some(v)
                            } else {
                                None
                            }
                        }))
                        .with_precision_and_scale(*p, *s)?,
                    ),
                    DataType::Utf8 => Arc::new(StringArray::from_iter(rows.iter().map(|row| {
                        if let Cell::Text(v) = &row.values[i] {
                            Some(v.as_str())
                        } else {
                            None
                        }
                    }))),
                    _ => unreachable!("oracle schema"),
                })
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(RecordBatch::try_new(self.schema.clone(), columns)?)
    }
}

pub(super) struct Rows<'a, I> {
    batches: I,
    layout: &'a Layout,
    pending: VecDeque<Row>,
}
impl<'a, I: Iterator<Item = Result<RecordBatch>>> Rows<'a, I> {
    pub(super) fn new(batches: I, layout: &'a Layout) -> Self {
        Self {
            batches,
            layout,
            pending: VecDeque::new(),
        }
    }
}
impl<I: Iterator<Item = Result<RecordBatch>>> Iterator for Rows<'_, I> {
    type Item = Result<Row>;
    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if let Some(row) = self.pending.pop_front() {
                return Some(Ok(row));
            }
            match self.batches.next()? {
                Ok(batch) => match self.layout.rows(batch) {
                    Ok(rows) => self.pending = rows.into(),
                    Err(error) => return Some(Err(error)),
                },
                Err(error) => return Some(Err(error)),
            }
        }
    }
}

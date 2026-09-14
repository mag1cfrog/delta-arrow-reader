// Modified from Sail v0.7.1 for the Delta reader experiment. See experiments/spark-sql/UPSTREAM.md in the host repository.
pub use arrow_buffer::i256;
use half::f16;

use crate::spec;
use crate::spec::TimestampType;

/// See [`spec::DataType`] for details on data types.
#[derive(Debug, Clone, PartialEq)]
pub enum Literal {
    Null,
    Boolean {
        value: Option<bool>,
    },
    Int8 {
        value: Option<i8>,
    },
    Int16 {
        value: Option<i16>,
    },
    Int32 {
        value: Option<i32>,
    },
    Int64 {
        value: Option<i64>,
    },
    UInt8 {
        value: Option<u8>,
    },
    UInt16 {
        value: Option<u16>,
    },
    UInt32 {
        value: Option<u32>,
    },
    UInt64 {
        value: Option<u64>,
    },
    Float16 {
        value: Option<f16>,
    },
    Float32 {
        value: Option<f32>,
    },
    Float64 {
        value: Option<f64>,
    },
    TimestampSecond {
        seconds: Option<i64>,
        timestamp_type: TimestampType,
    },
    TimestampMillisecond {
        milliseconds: Option<i64>,
        timestamp_type: TimestampType,
    },
    TimestampMicrosecond {
        microseconds: Option<i64>,
        timestamp_type: TimestampType,
    },
    TimestampNanosecond {
        nanoseconds: Option<i64>,
        timestamp_type: TimestampType,
    },
    Date32 {
        days: Option<i32>,
    },
    Date64 {
        milliseconds: Option<i64>,
    },
    Time32Second {
        seconds: Option<i32>,
    },
    Time32Millisecond {
        milliseconds: Option<i32>,
    },
    Time64Microsecond {
        microseconds: Option<i64>,
    },
    Time64Nanosecond {
        nanoseconds: Option<i64>,
    },
    DurationSecond {
        seconds: Option<i64>,
    },
    DurationMillisecond {
        milliseconds: Option<i64>,
    },
    DurationMicrosecond {
        microseconds: Option<i64>,
    },
    DurationNanosecond {
        nanoseconds: Option<i64>,
    },
    IntervalYearMonth {
        months: Option<i32>,
    },
    IntervalDayTime {
        value: Option<IntervalDayTime>,
    },
    IntervalMonthDayNano {
        value: Option<IntervalMonthDayNano>,
    },
    Binary {
        value: Option<Vec<u8>>,
    },
    FixedSizeBinary {
        size: i32,
        value: Option<Vec<u8>>,
    },
    LargeBinary {
        value: Option<Vec<u8>>,
    },
    BinaryView {
        value: Option<Vec<u8>>,
    },
    Utf8 {
        value: Option<String>,
    },
    LargeUtf8 {
        value: Option<String>,
    },
    Utf8View {
        value: Option<String>,
    },
    List {
        data_type: spec::DataType,
        nullable: bool,
        values: Option<Vec<Literal>>,
    },
    FixedSizeList {
        length: i32,
        data_type: spec::DataType,
        nullable: bool,
        values: Option<Vec<Literal>>,
    },
    LargeList {
        data_type: spec::DataType,
        nullable: bool,
        values: Option<Vec<Literal>>,
    },
    Struct {
        data_type: spec::DataType,
        values: Option<Vec<Literal>>,
    },
    Union {
        union_fields: spec::UnionFields,
        union_mode: spec::UnionMode,
        value: Option<(i8, Box<Literal>)>,
    },
    Dictionary {
        key_type: spec::DataType,
        value_type: spec::DataType,
        value: Option<Box<Literal>>,
    },
    Decimal128 {
        precision: u8,
        scale: i8,
        value: Option<i128>,
    },
    Decimal256 {
        precision: u8,
        scale: i8,
        value: Option<i256>,
    },
    Map {
        key_type: spec::DataType,
        value_type: spec::DataType,
        value_type_nullable: bool,
        keys: Option<Vec<Literal>>,
        values: Option<Vec<Literal>>,
    },
}

#[derive(Debug, Clone, PartialEq)]
pub struct IntervalDayTime {
    pub days: i32,
    pub milliseconds: i32,
}

#[derive(Debug, Clone, PartialEq)]
pub struct IntervalMonthDayNano {
    pub months: i32,
    pub days: i32,
    pub nanoseconds: i64,
}

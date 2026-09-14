// Modified from Sail v0.7.1 for the Delta reader experiment. See experiments/spark-sql/UPSTREAM.md in the host repository.

use crate::spec::data_type::Schema;
use crate::spec::expression::{Expr, ObjectName, SortOrder};
use crate::spec::literal::Literal;
use crate::spec::{Identifier, Window};

/// Unresolved query plan for the read-only SQL frontend.
#[derive(Debug, Clone, PartialEq)]
pub struct QueryPlan {
    pub node: QueryNode,
    pub plan_id: Option<i64>,
}

impl QueryPlan {
    pub fn new(node: QueryNode) -> Self {
        Self {
            node,
            plan_id: None,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum QueryNode {
    Read {
        read_type: ReadType,
        is_streaming: bool,
    },
    Project {
        input: Option<Box<QueryPlan>>,
        expressions: Vec<Expr>,
    },
    Filter {
        input: Box<QueryPlan>,
        condition: Expr,
    },
    Join(Join),
    SetOperation(SetOperation),
    Sort {
        input: Box<QueryPlan>,
        order: Vec<SortOrder>,
        is_global: bool,
    },
    Limit {
        input: Box<QueryPlan>,
        skip: Option<Expr>,
        limit: Option<Expr>,
    },
    Aggregate(Aggregate),
    LocalRelation {
        data: Option<Vec<u8>>,
        schema: Option<Schema>,
    },
    Sample(Sample),
    TableSample {
        input: Box<QueryPlan>,
        sample: TableSample,
    },
    Deduplicate(Deduplicate),
    Range(Range),
    SubqueryAlias {
        input: Box<QueryPlan>,
        alias: Identifier,
        qualifier: Vec<Identifier>,
    },
    Repartition {
        input: Box<QueryPlan>,
        num_partitions: usize,
        shuffle: bool,
    },
    ToDf {
        input: Box<QueryPlan>,
        column_names: Vec<Identifier>,
    },
    WithColumnsRenamed {
        input: Box<QueryPlan>,
        rename_columns_map: Vec<(Identifier, Identifier)>,
    },
    Drop {
        input: Box<QueryPlan>,
        columns: Vec<Expr>,
        column_names: Vec<Identifier>,
    },
    Tail {
        input: Box<QueryPlan>,
        limit: Expr,
    },
    WithColumns {
        input: Box<QueryPlan>,
        aliases: Vec<Expr>,
    },
    Hint {
        input: Box<QueryPlan>,
        name: String,
        parameters: Vec<Expr>,
    },
    Pivot(Pivot),
    Unpivot(Unpivot),
    ToSchema {
        input: Box<QueryPlan>,
        schema: Schema,
    },
    RepartitionByExpression {
        input: Box<QueryPlan>,
        partition_expressions: Vec<Expr>,
        num_partitions: Option<usize>,
    },
    /// Rejected Python entrypoint; input retains the early-rejection check.
    MapPartitions {
        input: Box<QueryPlan>,
    },
    CollectMetrics {
        input: Box<QueryPlan>,
        name: String,
        metrics: Vec<Expr>,
    },
    Parse(Parse),
    GroupMap {
        input: Box<QueryPlan>,
    },
    CoGroupMap {
        input: Box<QueryPlan>,
        other: Box<QueryPlan>,
    },
    WithWatermark {
        input: Box<QueryPlan>,
    },
    ApplyInPandasWithState {
        input: Box<QueryPlan>,
    },
    CachedLocalRelation {
        hash: String,
    },
    CachedRemoteRelation {
        relation_id: String,
    },
    CommonInlineUserDefinedTableFunction {
        arguments: Vec<Expr>,
    },
    // NA operations
    FillNa {
        input: Box<QueryPlan>,
        columns: Vec<Identifier>,
        values: Vec<Expr>,
    },
    DropNa {
        input: Box<QueryPlan>,
        columns: Vec<Identifier>,
        min_non_nulls: Option<usize>,
    },
    Replace {
        input: Box<QueryPlan>,
        columns: Vec<Identifier>,
        replacements: Vec<Replacement>,
    },
    // stat operations
    StatSummary {
        input: Box<QueryPlan>,
        statistics: Vec<String>,
    },
    StatDescribe {
        input: Box<QueryPlan>,
        columns: Vec<Identifier>,
    },
    StatCrosstab {
        input: Box<QueryPlan>,
        left_column: Identifier,
        right_column: Identifier,
    },
    StatCov {
        input: Box<QueryPlan>,
        left_column: Identifier,
        right_column: Identifier,
    },
    StatCorr {
        input: Box<QueryPlan>,
        left_column: Identifier,
        right_column: Identifier,
        method: String,
    },
    StatApproxQuantile {
        input: Box<QueryPlan>,
        columns: Vec<Identifier>,
        probabilities: Vec<f64>,
        relative_error: f64,
    },
    StatFreqItems {
        input: Box<QueryPlan>,
        columns: Vec<Identifier>,
        support: Option<f64>,
    },
    StatSampleBy {
        input: Box<QueryPlan>,
        column: Expr,
        fractions: Vec<Fraction>,
        seed: Option<i64>,
    },
    // extensions
    Empty {
        produce_one_row: bool,
    },
    WithParameters {
        input: Box<QueryPlan>,
        positional_arguments: Vec<Expr>,
        named_arguments: Vec<(String, Expr)>,
    },
    Values(Vec<Vec<Expr>>),
    TableAlias {
        input: Box<QueryPlan>,
        name: Identifier,
        columns: Vec<Identifier>,
    },
    WithCtes {
        input: Box<QueryPlan>,
        recursive: bool,
        ctes: Vec<(Identifier, QueryPlan)>,
    },
    NamedWindows {
        input: Box<QueryPlan>,
        windows: Vec<(Identifier, Window)>,
    },
    /// A relation that wraps a root plan with referenced subquery plans.
    WithRelations {
        root: Box<QueryPlan>,
        references: Vec<QueryPlan>,
    },
    LateralView {
        input: Option<Box<QueryPlan>>,
        function: ObjectName,
        arguments: Vec<Expr>,
        named_arguments: Vec<(Identifier, Expr)>,
        table_alias: Option<ObjectName>,
        column_aliases: Option<Vec<Identifier>>,
        outer: bool,
    },
    LateralJoin {
        left: Box<QueryPlan>,
        right: Box<QueryPlan>,
        join_condition: Option<Expr>,
        join_type: JoinType,
    },
}

#[derive(Debug, Clone, PartialEq)]
pub enum ReadType {
    NamedTable(Box<ReadNamedTable>),
    Udtf(Box<ReadUdtf>),
    DataSource(Box<ReadDataSource>),
    DynamicTable(Box<ReadDynamicTable>),
}

#[derive(Debug, Clone, PartialEq)]
pub struct ReadNamedTable {
    pub name: ObjectName,
    pub temporal: Option<TableTemporal>,
    pub sample: Option<TableSample>,
    pub options: Vec<(String, String)>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ReadDynamicTable {
    pub name: Expr,
    pub sample: Option<TableSample>,
    pub options: Vec<(String, String)>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum TableTemporal {
    Version { value: Expr },
    Timestamp { value: Expr },
}

#[derive(Debug, Clone, PartialEq)]
pub struct TableSample {
    pub method: TableSampleMethod,
    pub seed: Option<i64>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum TableSampleMethod {
    Percent {
        value: Expr,
    },
    Rows {
        value: Expr,
    },
    Bucket {
        numerator: usize,
        denominator: usize,
    },
}

#[derive(Debug, Clone, PartialEq)]
pub struct ReadUdtf {
    pub name: ObjectName,
    pub arguments: Vec<Expr>,
    pub named_arguments: Vec<(Identifier, Expr)>,
    pub options: Vec<(String, String)>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ReadDataSource {
    pub format: Option<String>,
    pub schema: Option<Schema>,
    pub options: Vec<(String, String)>,
    pub paths: Vec<String>,
    pub predicates: Vec<Expr>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Join {
    pub left: Box<QueryPlan>,
    pub right: Box<QueryPlan>,
    pub join_type: JoinType,
    pub join_criteria: Option<JoinCriteria>,
    pub join_data_type: Option<JoinDataType>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct SetOperation {
    pub left: Box<QueryPlan>,
    pub right: Box<QueryPlan>,
    pub set_op_type: SetOpType,
    pub is_all: bool,
    pub by_name: bool,
    pub allow_missing_columns: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Aggregate {
    pub input: Box<QueryPlan>,
    pub grouping: Vec<Expr>,
    pub aggregate: Vec<Expr>,
    pub having: Option<Expr>,
    /// Whether the grouping expressions should be added to the projection.
    pub with_grouping_expressions: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Sample {
    pub input: Box<QueryPlan>,
    pub lower_bound: f64,
    pub upper_bound: f64,
    pub with_replacement: bool,
    pub seed: Option<i64>,
    pub deterministic_order: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Deduplicate {
    pub input: Box<QueryPlan>,
    pub column_names: Vec<Identifier>,
    pub all_columns_as_keys: bool,
    pub within_watermark: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Range {
    pub start: Option<i64>,
    pub end: i64,
    pub step: i64,
    pub num_partitions: Option<usize>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Pivot {
    pub input: Box<QueryPlan>,
    /// The group-by columns for the pivot operation, set for the DataFrame API (possibly
    /// empty). When `None` (for SQL statements), all the remaining columns are included.
    pub grouping: Option<Vec<Expr>>,
    pub aggregate: Vec<Expr>,
    pub columns: Vec<Expr>,
    pub values: Vec<PivotValue>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct PivotValue {
    /// The value expressions for a single pivot output column. Each is a foldable expression
    /// (a literal, typed literal such as `DATE'...'`, or a cast) that the resolver evaluates to a
    /// scalar. A single-element list is the common case; multiple elements form a struct pivot.
    pub values: Vec<Expr>,
    pub alias: Option<Identifier>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Unpivot {
    pub input: Box<QueryPlan>,
    /// When `ids` is [None] (for SQL statements), all remaining columns are included.
    /// When `ids` is [Some] (for the DataFrame API), only the specified columns are included.
    pub ids: Option<Vec<Expr>>,
    pub values: Option<Vec<UnpivotValue>>,
    pub variable_column_name: Identifier,
    pub value_column_names: Vec<Identifier>,
    pub include_nulls: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub struct UnpivotValue {
    pub columns: Vec<Expr>,
    pub alias: Option<Identifier>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Parse {
    pub input: Box<QueryPlan>,
    pub format: ParseFormat,
    pub schema: Option<Schema>,
    pub options: Vec<(String, String)>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum JoinType {
    Inner,
    FullOuter,
    LeftOuter,
    RightOuter,
    LeftSemi,
    RightSemi,
    LeftAnti,
    RightAnti,
    Cross,
}

#[derive(Debug, Clone, PartialEq)]
pub enum JoinCriteria {
    Natural,
    On(Expr),
    Using(Vec<Identifier>),
}

#[derive(Debug, Clone, PartialEq)]
pub struct JoinDataType {
    pub is_left_struct: bool,
    pub is_right_struct: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub enum SetOpType {
    Intersect,
    Union,
    Except,
}

#[derive(Debug, Clone, PartialEq)]
pub enum ParseFormat {
    Unspecified,
    Csv,
    Json,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Fraction {
    pub stratum: Literal,
    pub fraction: f64,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Replacement {
    pub old_value: Literal,
    pub new_value: Literal,
}

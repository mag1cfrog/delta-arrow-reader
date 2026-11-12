use std::mem;
use std::sync::Arc;

use datafusion::arrow::datatypes::DataType;
use datafusion::common::tree_node::{Transformed, TreeNodeRewriter};
use datafusion::common::{Column, RecursionUnnestOption, Result, UnnestOptions};
use datafusion::functions::core::expr_ext::FieldAccessor;
use datafusion::logical_expr::builder::unnest_with_options;
use datafusion::logical_expr::{Expr, ExprSchemable, LogicalPlan, Projection, ScalarUDF};
use datafusion_common::{ExprSchema, plan_err};
use datafusion_expr::expr::ScalarFunction;
use datafusion_expr::{ident, lit, when};
use datafusion_functions_nested::expr_fn as nested_fn;
use either::Either;
use sail_common_datafusion::utils::items::ItemTaker;
use sail_function::scalar::array::spark_array_item_with_position::ArrayItemWithPosition;
use sail_function::scalar::array_struct_field::ArrayStructField;
use sail_function::scalar::explode::{Explode, ExplodeKind};
use sail_function::scalar::map::map_entries::SparkMapEntries;
use sail_function::scalar::multi_expr::MultiExpr;

use crate::resolver::state::PlanResolverState;
use crate::resolver::tree::{
    PlanRewriter, empty_logical_plan, skip_aggregate_arguments_needing_validation,
};

enum ExplodeDataType {
    List,
    Map,
}

impl ExplodeDataType {
    fn try_from_expr(expr: &Expr, schema: &dyn ExprSchema) -> Result<Self> {
        match expr.get_type(schema)? {
            DataType::List(_) | DataType::LargeList(_) | DataType::FixedSizeList(_, _) => {
                Ok(ExplodeDataType::List)
            }
            DataType::Map(_, _) => Ok(ExplodeDataType::Map),
            _ => plan_err!("only list or map can be exploded"),
        }
    }
}

pub(crate) struct ExplodeRewriter<'s> {
    plan: LogicalPlan,
    state: &'s mut PlanResolverState,
}

impl<'s> PlanRewriter<'s> for ExplodeRewriter<'s> {
    fn new_from_plan(plan: LogicalPlan, state: &'s mut PlanResolverState) -> Self {
        Self { plan, state }
    }

    fn into_plan(self) -> LogicalPlan {
        self.plan
    }
}

impl TreeNodeRewriter for ExplodeRewriter<'_> {
    type Node = Expr;

    fn f_down(&mut self, node: Expr) -> Result<Transformed<Expr>> {
        skip_aggregate_arguments_needing_validation(node)
    }

    fn f_up(&mut self, node: Expr) -> Result<Transformed<Expr>> {
        let (func, args) = match node {
            Expr::ScalarFunction(ScalarFunction { func, args }) => (func, args),
            _ => return Ok(Transformed::no(node)),
        };
        let inner = func.inner();
        let explode = match inner.downcast_ref::<Explode>() {
            Some(explode) => explode,
            None => {
                return Ok(Transformed::no(func.call(args)));
            }
        };
        let (with_position, preserve_nulls, is_inline) = match explode.kind() {
            ExplodeKind::Explode => (false, false, false),
            ExplodeKind::ExplodeOuter => (false, true, false),
            ExplodeKind::PosExplode => (true, false, false),
            ExplodeKind::PosExplodeOuter => (true, true, false),
            ExplodeKind::Inline => (false, false, true),
            ExplodeKind::InlineOuter => (false, true, true),
        };
        let arg = args.one()?;
        let arg_type = arg.get_type(self.plan.schema())?;
        let return_type = func.return_type(&[arg_type])?;
        let data_type = ExplodeDataType::try_from_expr(&arg, self.plan.schema())?;
        let arg = match data_type {
            ExplodeDataType::List => arg,
            ExplodeDataType::Map => ScalarUDF::from(SparkMapEntries::new()).call(vec![arg]),
        };
        let mut projections = self
            .plan
            .schema()
            .columns()
            .into_iter()
            .map(Expr::Column)
            .collect::<Vec<_>>();
        let mut plan = mem::replace(&mut self.plan, empty_logical_plan());
        let arg = if preserve_nulls && !is_inline && arg.is_volatile() {
            // The outer CASE must check and return the same collection value.
            let field = self.state.register_hidden_field_name("");
            let mut materialized = projections.clone();
            materialized.push(arg.alias(&field));
            plan = LogicalPlan::Projection(Projection::try_new(materialized, Arc::new(plan))?);
            ident(field)
        } else {
            arg
        };
        let arg = match preserve_nulls {
            true => when(nested_fn::array_empty(arg.clone()).is_false(), arg).end()?,
            false => arg,
        };
        let arg = match with_position {
            true => ScalarUDF::from(ArrayItemWithPosition::new()).call(vec![arg]),
            false => arg,
        };

        let output_name = self.state.register_field_name("");
        let mut inline_projections = vec![];
        let out = match (data_type, with_position, is_inline) {
            (ExplodeDataType::List, false, false) => vec![ident(&output_name).alias("col")],
            (ExplodeDataType::List, true, false) => {
                vec![
                    ident(&output_name).field("pos").alias("pos"),
                    ident(&output_name).field("col").alias("col"),
                ]
            }
            (ExplodeDataType::List, _, true) => match return_type {
                DataType::Struct(fields) => Ok(fields
                    .into_iter()
                    .map(|field| {
                        let field_name = field.name().to_string();
                        let field_column = self.state.register_field_name("");
                        inline_projections.push((
                            ScalarUDF::from(ArrayStructField::new())
                                .call(vec![arg.clone(), lit(field_name.clone())])
                                .alias(&field_column),
                            Column::from_name(&field_column),
                        ));
                        ident(&field_column).alias(field_name)
                    })
                    .collect::<Vec<_>>()),
                wrong_type => plan_err!(
                    "inline/inline_outer expects List<Struct> as argument, got {wrong_type:?}"
                ),
            }?,
            (ExplodeDataType::Map, with_position, _) => {
                let DataType::Struct(fields) = return_type else {
                    return plan_err!("map entries must be structs");
                };
                let [key, value] = fields.as_ref() else {
                    return plan_err!("map entries must have key and value fields");
                };
                let mut entry = ident(&output_name);
                let mut out = vec![];
                if with_position {
                    out.push(entry.clone().field("pos").alias("pos"));
                    entry = entry.field("col");
                }
                out.push(entry.clone().field(key.name().as_str()).alias("key"));
                out.push(entry.field(value.name().as_str()).alias("value"));
                out
            }
        };

        let (columns_to_unnest, recursions) = if inline_projections.is_empty() {
            let input_name = self.state.register_field_name("");
            projections.push(arg.alias(&input_name));
            let input_column = Column::from_name(&input_name);
            // A distinct output name keeps field access on the unnested value
            // from being pushed below UNNEST, where the column is still a list.
            let recursion = RecursionUnnestOption {
                input_column: input_column.clone(),
                output_column: Column::from_name(&output_name),
                depth: 1,
            };
            (vec![input_column], vec![recursion])
        } else {
            let columns = inline_projections
                .iter()
                .map(|(_, column)| column.clone())
                .collect::<Vec<_>>();
            projections.extend(inline_projections.into_iter().map(|(expr, _)| expr));
            (columns, vec![])
        };

        self.plan = unnest_with_options(
            LogicalPlan::Projection(Projection::try_new(projections, Arc::new(plan))?),
            columns_to_unnest,
            UnnestOptions {
                preserve_nulls,
                recursions,
            },
        )?;

        let out = match out.one_or_more()? {
            Either::Left(node) => node,
            Either::Right(nodes) => ScalarUDF::from(MultiExpr::new()).call(nodes),
        };
        Ok(Transformed::yes(out))
    }
}

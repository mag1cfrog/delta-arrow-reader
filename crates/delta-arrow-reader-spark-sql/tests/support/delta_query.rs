//! Use the same Delta query through native SQL and the public Spark API.

use datafusion::{physical_plan::ExecutionPlan, prelude::SessionContext};
use delta_arrow_reader_spark_sql::{SparkSqlOptions, plan_query};
use std::sync::Arc;

#[derive(Clone, Copy, Debug)]
pub(super) enum QueryPath {
    Native,
    Spark { ansi_mode: bool },
}

impl QueryPath {
    pub const ALL: [Self; 3] = [
        Self::Native,
        Self::Spark { ansi_mode: false },
        Self::Spark { ansi_mode: true },
    ];

    pub async fn plan(
        self,
        context: &SessionContext,
        sql: &str,
    ) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
        match self {
            Self::Native => context.sql(sql).await?.create_physical_plan().await,
            Self::Spark { ansi_mode } => {
                plan_query(
                    context,
                    sql,
                    &SparkSqlOptions {
                        ansi_mode,
                        ..Default::default()
                    },
                )
                .await?
                .create_physical_plan(context)
                .await
            }
        }
    }
}

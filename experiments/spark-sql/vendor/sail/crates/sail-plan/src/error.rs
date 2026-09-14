// Modified from Sail v0.7.1 for the Delta reader experiment. See experiments/spark-sql/UPSTREAM.md in the host repository.
use datafusion::arrow::error::ArrowError;
use datafusion::common::DataFusionError;
use sail_sql_analyzer::error::SqlError;
use thiserror::Error;

/// Result type for plan operations.
pub type PlanResult<T> = Result<T, PlanError>;

pub trait IntoPlanResult<T> {
    fn into_plan_result(self) -> PlanResult<T>;
}

impl<T> IntoPlanResult<T> for T {
    fn into_plan_result(self) -> PlanResult<T> {
        Ok(self)
    }
}

impl<T> IntoPlanResult<T> for PlanResult<T> {
    fn into_plan_result(self) -> PlanResult<T> {
        self
    }
}

#[derive(Debug, Error)]
pub enum PlanError {
    #[error("error in DataFusion: {0}")]
    DataFusionError(#[from] DataFusionError),
    #[error("error in Arrow: {0}")]
    ArrowError(#[from] ArrowError),
    #[error("invalid argument: {0}")]
    InvalidArgument(String),
    #[error("not implemented: {0}")]
    NotImplemented(String),
    #[error("not supported: {0}")]
    NotSupported(String),
    #[error("internal error: {0}")]
    InternalError(String),
    #[error("analysis error: {0}")]
    AnalysisError(String),
}

impl PlanError {
    pub fn todo(message: impl Into<String>) -> Self {
        PlanError::NotImplemented(message.into())
    }

    pub fn unsupported(message: impl Into<String>) -> Self {
        PlanError::NotSupported(message.into())
    }

    pub fn invalid(message: impl Into<String>) -> Self {
        PlanError::InvalidArgument(message.into())
    }

    pub fn internal(message: impl Into<String>) -> Self {
        PlanError::InternalError(message.into())
    }

    pub fn analysis(message: impl Into<String>) -> Self {
        PlanError::AnalysisError(message.into())
    }
}

impl From<SqlError> for PlanError {
    fn from(value: SqlError) -> Self {
        match value {
            SqlError::SqlParserError(message) => PlanError::InvalidArgument(message),
            SqlError::InvalidArgument(message) => PlanError::InvalidArgument(message),
            SqlError::NotImplemented(message) => PlanError::NotImplemented(message),
            SqlError::NotSupported(message) => PlanError::NotSupported(message),
            SqlError::AnalysisError(message) => PlanError::AnalysisError(message),
        }
    }
}

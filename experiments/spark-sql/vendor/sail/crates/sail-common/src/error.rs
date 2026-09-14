// Modified from Sail v0.7.1 for the Delta reader experiment. See experiments/spark-sql/UPSTREAM.md in the host repository.
use thiserror::Error;

pub type CommonResult<T> = Result<T, CommonError>;

#[derive(Debug, Error)]
pub enum CommonError {
    #[error("not supported: {0}")]
    NotSupported(String),
}

impl CommonError {
    pub fn unsupported(message: impl Into<String>) -> Self {
        CommonError::NotSupported(message.into())
    }
}

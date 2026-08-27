//! Delta table metadata and Delta Kernel integration.

pub(crate) mod kernel;
pub(crate) mod protocol;
pub(crate) mod snapshot;
mod uri;

pub use protocol::DeltaProtocolInfo;

//! Delta table metadata and Delta Kernel integration.

pub(crate) mod kernel;
mod location;
pub(crate) mod protocol;
pub(crate) mod snapshot;

pub use protocol::DeltaProtocol;

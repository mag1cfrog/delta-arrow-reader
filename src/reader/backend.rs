//! Data-file reader backend implementations.

#[cfg(feature = "native-async")]
#[allow(dead_code)]
pub(crate) mod native_async;
#[cfg(feature = "official-kernel")]
#[allow(dead_code)]
pub(crate) mod official_kernel;

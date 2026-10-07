//! Guides for getting started and understanding how the reader works.
//!
//! New users should begin with [`getting_started`]. The [`concepts`] section
//! explains the read path, and [`mod@reference`] documents its controls and metrics.

/// Install the crate and run a first query.
pub mod getting_started {
    #[doc = include_str!("../docs/public/installation.md")]
    pub mod installation {}

    #[doc = include_str!("../docs/public/streaming-reader.md")]
    pub mod streaming_reader {}

    #[cfg(feature = "datafusion")]
    #[doc = include_str!("../docs/public/datafusion.md")]
    pub mod datafusion {}
}

/// Understand the reader's architecture and scan lifecycle.
pub mod concepts {
    #[doc = include_str!("../docs/public/architecture.md")]
    pub mod architecture {}

    #[doc = include_str!("../docs/public/delta-metadata-lifecycle.md")]
    pub mod delta_metadata_lifecycle {}

    #[doc = include_str!("../docs/public/scan-planning.md")]
    pub mod scan_planning {}

    #[doc = include_str!("../docs/public/read-scheduling.md")]
    pub mod read_scheduling {}
}

/// Look up scan controls and metrics.
pub mod reference {
    #[doc = include_str!("../docs/public/reference/execution-options.md")]
    pub mod execution_options {}

    #[doc = include_str!("../docs/public/reference/metrics.md")]
    pub mod metrics {}
}

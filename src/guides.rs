//! Guides for getting started and understanding how the reader works.
//!
//! New users should begin with [`getting_started`]. The [`concepts`] section
//! explains the read path, and [`mod@reference`] documents its controls and metrics.

/// Install the crate and run a first query.
pub mod getting_started {
    #[doc = include_str!("../docs/content/installation.md")]
    pub mod installation {}

    #[doc = include_str!("../docs/content/streaming-reader.md")]
    pub mod streaming_reader {}

    #[cfg(feature = "datafusion")]
    #[doc = include_str!("../docs/content/datafusion.md")]
    pub mod datafusion {}
}

/// Understand the reader's architecture and scan lifecycle.
pub mod concepts {
    #[doc = include_str!("../docs/content/architecture.md")]
    pub mod architecture {}

    #[doc = include_str!("../docs/content/delta-metadata-lifecycle.md")]
    pub mod delta_metadata_lifecycle {}

    #[doc = include_str!("../docs/content/scan-planning.md")]
    pub mod scan_planning {}

    #[doc = include_str!("../docs/content/read-scheduling.md")]
    pub mod read_scheduling {}
}

/// Look up scan controls and metrics.
pub mod reference {
    #[doc = include_str!("../docs/content/reference/execution-options.md")]
    pub mod execution_options {}

    #[doc = include_str!("../docs/content/reference/metrics.md")]
    pub mod metrics {}
}

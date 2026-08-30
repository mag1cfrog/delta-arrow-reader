# Changelog

## Unreleased

## [0.6.0](https://github.com/mag1cfrog/delta-arrow-reader/compare/v0.5.3...v0.6.0) - 2026-08-30

### Added

- refresh loaded Delta tables and DataFusion providers without mutating active scans

### Changed

- [**breaking**] remove table-wide Parquet metadata warmup after production evaluation
- [**breaking**] configure table metadata warmup through one builder API

### Fixed

- let adaptive Parquet planning choose exact ranges after estimation

### Performance

- benchmark incremental Delta metadata refresh strategies

## [0.5.3](https://github.com/mag1cfrog/delta-arrow-reader/compare/v0.5.2...v0.5.3) - 2026-08-29

### Added

- add opt-in Parquet metadata preparation for faster repeated queries

### Fixed

- prevent tiny Parquet reads from distorting adaptive range planning

## [0.5.2](https://github.com/mag1cfrog/delta-arrow-reader/compare/v0.5.1...v0.5.2) - 2026-08-29

### Added

- add debug diagnostics for automatic Parquet range-plan decisions

## [0.5.1](https://github.com/mag1cfrog/delta-arrow-reader/compare/v0.5.0...v0.5.1) - 2026-08-29

### Fixed

- preserve adaptive Parquet range learning across DataFusion scans

## [0.5.0](https://github.com/mag1cfrog/delta-arrow-reader/compare/v0.4.2...v0.5.0) - 2026-08-29

### Fixed

- keep scan metric snapshots extensible
- stabilize Parquet range benchmark tests on Windows

### Performance

- validate adaptive Parquet range reads across transport conditions
- adapt Parquet range reads to observed transport costs
- preserve native Parquet multi-range reads ([#65](https://github.com/mag1cfrog/delta-arrow-reader/pull/65))

## [0.4.2](https://github.com/mag1cfrog/delta-arrow-reader/compare/v0.4.1...v0.4.2) - 2026-08-29

### Performance

- quantify Parquet page-index range-read savings
- reduce Parquet I/O by reading only pages selected by row filters
- quantify Parquet row-filter savings from predicate-only decoding
- speed up Parquet row filtering by limiting filter reads to predicate columns

## [0.4.1](https://github.com/mag1cfrog/delta-arrow-reader/compare/v0.4.0...v0.4.1) - 2026-08-28

### Documentation

- clarify Delta reader tradeoffs in the README
- explain Delta metadata caching and improve reader guides ([#50](https://github.com/mag1cfrog/delta-arrow-reader/pull/50))
- clarify lazy and eager metadata loading ([#49](https://github.com/mag1cfrog/delta-arrow-reader/pull/49))

### Performance

- reduce memory retained by eager Delta metadata caching

## [0.4.0](https://github.com/mag1cfrog/delta-arrow-reader/compare/v0.3.0...v0.4.0) - 2026-08-28

### Added

- expose eager Delta metadata cache lifecycle tracing
- add opt-in Delta metadata preloading for log-free query planning
- [**breaking**] simplify table loading with explicit async methods

### Changed

- separate Parquet schema alignment from direct file reading
- [**breaking**] make reader APIs easier to understand ([#24](https://github.com/mag1cfrog/delta-arrow-reader/pull/24))
- [**breaking**] simplify Parquet reader backend selection
- organize reader modules ([#21](https://github.com/mag1cfrog/delta-arrow-reader/pull/21))
- [**breaking**] require Rust 1.94 and make release cache read-only ([#19](https://github.com/mag1cfrog/delta-arrow-reader/pull/19))

### Documentation

- quantify eager metadata caching performance and memory tradeoffs
- explain eager Delta scan metadata initialization
- compare Delta reader performance and tradeoffs
- make the README easier for new users
- organize benchmark and documentation files
- publish shared user guides on docs.rs
- add a standalone Delta Arrow Reader site
- publish reproducible reader benchmark results

### Maintenance

- organize release changelog sections

## [0.3.0](https://github.com/mag1cfrog/delta-arrow-reader/compare/v0.2.0...v0.3.0) - 2026-08-12

### Added

- add configurable file repartitioning ([#16](https://github.com/mag1cfrog/delta-arrow-reader/pull/16))
- speed up large Parquet scans with intra-file parallelism

### Other

- explain intra-file repartitioning flow
- enforce strict Rust quality checks ([#14](https://github.com/mag1cfrog/delta-arrow-reader/pull/14))

## 0.2.0 - 2026-08-11

- Allow DataFusion consumers to disable string and binary view arrays while keeping them enabled by default.

## 0.1.3 - 2026-08-11

- Decode DataFusion string and binary columns directly into Arrow view arrays and dictionary-encode string and binary partition columns.

## 0.1.2 - 2026-08-11

- Add staged snapshot loading for protocol checks before Arrow schema conversion.
- Expose protocol and shared metrics identity needed by downstream integrations.
- Preserve planning and object-store diagnostic context across DataFusion tasks.

## 0.1.1 - 2026-08-10

- Preserve the extracted reader's independent scan, partition, and prefetch bounds.

## 0.1.0 - 2026-08-10

- Add the read-only direct Delta Lake to Arrow streaming API.
- Add NativeAsync and OfficialKernel data-file reader backends.
- Add projections, predicates, deletion vectors, bounded scheduling, and scan metrics.
- Add the optional DataFusion provider, registration, filtering, execution, and metrics API.

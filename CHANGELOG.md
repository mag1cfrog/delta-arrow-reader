# Changelog

## Unreleased

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

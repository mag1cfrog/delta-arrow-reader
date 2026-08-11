# Changelog

## Unreleased

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

from collections.abc import Mapping
from os import PathLike
from types import TracebackType
from typing import Literal

import pyarrow

__version__: str

_Filter = (
    tuple[str, Literal["is", "is not"], None]
    | tuple[str, Literal["==", "!=", "<", "<=", ">", ">="], bool | int | float]
)

class DeltaReaderError(Exception):
    phase: str
    code: str

class ScanExecutionOptions:
    """Immutable execution settings for a table or an individual scan."""
    def __init__(
        self, *, parquet_backend: Literal["direct", "delta_kernel"] = "direct",
        max_concurrent_file_reads_per_scan: int | None = None,
        max_concurrent_file_reads_per_partition: int = 3,
        output_buffer_batches_per_partition: int = 1,
        prefetch_files_per_partition: int = 2,
        parquet_metadata_size_hint_bytes: int | None = 65536,
        parquet_full_file_read_threshold_bytes: int | None = None,
    ) -> None: ...
    @property
    def parquet_backend(self) -> Literal["direct", "delta_kernel"]: ...
    @property
    def max_concurrent_file_reads_per_scan(self) -> int | None: ...
    @property
    def max_concurrent_file_reads_per_partition(self) -> int: ...
    @property
    def output_buffer_batches_per_partition(self) -> int: ...
    @property
    def prefetch_files_per_partition(self) -> int: ...
    @property
    def parquet_metadata_size_hint_bytes(self) -> int | None: ...
    @property
    def parquet_full_file_read_threshold_bytes(self) -> int | None: ...

class RecordBatchStream:
    """Single-use Arrow exporter created by DeltaTable.scan(), with no public constructor."""
    def __arrow_c_stream__(self, requested_schema: object | None = None) -> object: ...
    def close(self) -> None: ...
    def __enter__(self) -> RecordBatchStream: ...
    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None: ...

class DeltaTable:
    def __init__(
        self,
        location: str | PathLike[str],
        *,
        version: int | None = None,
        storage_options: Mapping[str, str] | None = None,
        warmup: Literal["none", "query_planning"] = "none",
        execution_options: ScanExecutionOptions | None = None,
    ) -> None: ...
    def refresh(self) -> DeltaTable: ...
    @property
    def version(self) -> int: ...
    @property
    def schema(self) -> pyarrow.Schema: ...
    def __arrow_c_schema__(self) -> object: ...
    def scan(
        self, *, columns: list[str] | tuple[str, ...] | None = None,
        filters: list[_Filter] | list[list[_Filter]] | None = None,
        limit: int | None = None,
        target_partitions: int | None = None,
        execution_options: ScanExecutionOptions | None = None,
    ) -> RecordBatchStream: ...
    def to_reader(
        self, *, columns: list[str] | tuple[str, ...] | None = None,
        filters: list[_Filter] | list[list[_Filter]] | None = None,
        limit: int | None = None,
        target_partitions: int | None = None,
        execution_options: ScanExecutionOptions | None = None,
    ) -> pyarrow.RecordBatchReader: ...

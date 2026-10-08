from collections.abc import Mapping
from os import PathLike
from types import TracebackType
from typing import Literal

import pyarrow

__version__: str

class DeltaReaderError(Exception):
    phase: str
    code: str

class ScanExecutionOptions:
    """Immutable execution settings for a table or an individual scan."""
    def __init__(
        self, *, parquet_backend: Literal["direct", "delta_kernel"] = "direct",
    ) -> None: ...
    @property
    def parquet_backend(self) -> Literal["direct", "delta_kernel"]: ...

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
        limit: int | None = None,
        target_partitions: int | None = None,
        execution_options: ScanExecutionOptions | None = None,
    ) -> RecordBatchStream: ...
    def to_reader(
        self, *, columns: list[str] | tuple[str, ...] | None = None,
        limit: int | None = None,
        target_partitions: int | None = None,
        execution_options: ScanExecutionOptions | None = None,
    ) -> pyarrow.RecordBatchReader: ...

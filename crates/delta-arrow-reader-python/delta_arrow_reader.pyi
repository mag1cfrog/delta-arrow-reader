from collections.abc import Mapping
from os import PathLike
from types import TracebackType
from typing import Literal

import pyarrow

__version__: str

class DeltaReaderError(Exception):
    phase: str
    code: str

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
    ) -> RecordBatchStream: ...
    def to_reader(
        self, *, columns: list[str] | tuple[str, ...] | None = None,
        limit: int | None = None,
        target_partitions: int | None = None,
    ) -> pyarrow.RecordBatchReader: ...

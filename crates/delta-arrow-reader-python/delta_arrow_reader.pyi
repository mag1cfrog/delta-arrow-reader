from collections.abc import Mapping
from os import PathLike

import pyarrow

__version__: str

class DeltaReaderError(Exception):
    phase: str
    code: str

class DeltaTable:
    def __init__(
        self,
        location: str | PathLike[str],
        *,
        version: int | None = None,
        storage_options: Mapping[str, str] | None = None,
    ) -> None: ...
    @property
    def version(self) -> int: ...
    @property
    def schema(self) -> pyarrow.Schema: ...
    def __arrow_c_schema__(self) -> object: ...
    def to_reader(self) -> pyarrow.RecordBatchReader: ...

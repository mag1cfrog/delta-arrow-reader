from collections.abc import Mapping
from os import PathLike

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

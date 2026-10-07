from os import PathLike

__version__: str

class DeltaReaderError(Exception):
    phase: str
    code: str

class DeltaTable:
    def __init__(self, location: str | PathLike[str]) -> None: ...
    @property
    def version(self) -> int: ...

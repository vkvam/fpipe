from typing import Type

from fpipe.exceptions import FileDataException
from fpipe.file import File
from fpipe.meta.abstract import FileData, T


def meta_prioritized(t: Type[FileData[T]], *sources: File) -> T:
    error = FileDataException(t)
    for s in sources:
        try:
            return s[t]
        except FileDataException:
            pass
    raise error

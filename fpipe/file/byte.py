import io
from typing import Union, Optional, Iterable

from fpipe.file.file import FileStream
from fpipe.meta.abstract import FileMeta


class ByteFile(FileStream):
    def __init__(self, b: bytes, meta: Optional[Union[FileMeta, Iterable[FileMeta]]] = None):
        super().__init__(io.BytesIO(b), meta=meta)

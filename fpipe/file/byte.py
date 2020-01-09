import io
from typing import Union, Optional, Iterable

from fpipe.file.file import FileStream
from fpipe.meta.abstract import FileMeta


class ByteFile(FileStream):
    def __init__(
        self,
        s: Union[bytes, bytearray],
        meta: Optional[
            Union[FileMeta, Iterable[FileMeta]]
        ] = None,
    ):
        super().__init__(io.BytesIO(s), meta=meta)

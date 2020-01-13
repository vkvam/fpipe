import io
from typing import Union, Optional, Iterable

from fpipe.file.file import File
from fpipe.meta.abstract import FileData


class ByteFile(File):
    def __init__(
        self,
        s: Union[bytes, bytearray],
        meta: Optional[
            Union[FileData, Iterable[FileData]]
        ] = None,
    ):
        super().__init__(stream=io.BytesIO(s), meta=meta)

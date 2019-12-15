import datetime
from typing import Generic, TypeVar
from fpipe.file.filemeta import MetaStr
from fpipe.fileinfo import FileInfoException

T = TypeVar('T')


class MetaGetter(Generic[T]):
    def __init__(self, getter, lock):
        self.getter = getter
        self.lock = lock

    @property
    def value(self) -> T:
        if self.lock.locked():
            raise FileInfoException("S3 object meta is not available until after object has been written")
        return self.getter()


# Futures
class S3Size(MetaGetter[int]):
    pass


class S3Modified(MetaGetter[datetime.datetime]):
    pass


class S3Mime(MetaGetter[str]):
    pass


# From S3File
class S3Version(MetaStr):
    pass


class S3Key(MetaStr):
    pass

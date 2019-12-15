import datetime
from typing import Generic, TypeVar
from fpipe.generators.fileinfo import FileInfoException
from fpipe.meta.abstract import FileMeta, FileMetaValue

T = TypeVar('T')


class MetaGetter(FileMeta[T]):
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
class S3Version(FileMetaValue[str]):
    pass


class S3Key(FileMetaValue[str]):
    pass

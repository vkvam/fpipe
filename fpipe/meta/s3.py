import datetime
from typing import TypeVar, Iterable

from fpipe.exceptions import FileInfoException
from fpipe.meta.abstract import FileMeta, FileMetaValue
from fpipe.utils.s3_reader import S3FileReader

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


class S3FileInfo:
    def __init__(self, reader: S3FileReader):
        self.reader = reader
        self.__s3_obj = None
        self.bucket = reader.bucket
        self.path = reader.key

    def _get_metadata(self, key: str):
        self.__s3_obj = self.__s3_obj or self.reader.s3_client.get_object(
            Bucket=self.reader.bucket,
            Key=self.reader.key,
            **({'VersionId': self.reader.version} if self.reader.version else {})
        )
        if self.__s3_obj:
            return self.__s3_obj[key]
        else:
            raise Exception("?")  # TODO: Improve

    def meta_gen(self) -> Iterable[FileMeta]:
        if self.reader.version:
            yield S3Version(self.reader.version)
        yield S3Key(self.path)

        yield S3Size(lambda: self._get_metadata('ContentLength'), self.reader.meta_lock)
        yield S3Modified(lambda: self._get_metadata('LastModified'), self.reader.meta_lock)
        yield S3Mime(lambda: self._get_metadata('ContentType'), self.reader.meta_lock)
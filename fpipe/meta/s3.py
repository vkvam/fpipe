from typing import Iterable, Type
from fpipe.exceptions import FileMetaException
from fpipe.meta import Version
from fpipe.meta.modified import Modified
from fpipe.meta.mime import Mime
from fpipe.meta.path import Path
from fpipe.meta.size import Size
from fpipe.meta.abstract import FileMeta
from fpipe.utils.s3_reader import S3FileReader


class S3MetadataProducer:
    def __init__(self, reader: S3FileReader):
        self.reader = reader
        self.__s3_obj = None
        self.bucket = reader.bucket
        self.path = reader.key

    def generate(self) -> Iterable[FileMeta]:
        if self.reader.version:
            yield Version(self.reader.version)

        yield Path(self.path)
        yield Size(future=self.__future("ContentLength", Size))
        yield Modified(future=self.__future("LastModified", Modified))
        yield Mime(future=self.__future("ContentType", Mime))

    def __get_metadata(self, lock, key: str, value_class):
        if lock.locked():
            raise FileMetaException(value_class)
        self.__s3_obj = self.__s3_obj or self.reader.s3_client.get_object(
            Bucket=self.reader.bucket,
            Key=self.reader.key,
            **(
                {"VersionId": self.reader.version}
                if self.reader.version
                else {}
            ),
        )
        if self.__s3_obj:
            return self.__s3_obj[key]
        else:
            raise FileMetaException(value_class)  # TODO: improve

    def __future(self, key_name, value_class: Type):
        lock = self.reader.meta_lock
        return lambda: self.__get_metadata(lock, key_name, value_class)

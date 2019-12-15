from threading import Thread, Lock
from typing import Iterable, Union, Optional

from fpipe.file.file import Path
from .meta import S3Version, S3Key, S3Size, S3Modified, S3Mime
from fpipe.utils.mime import guess_mime
from fpipe.utils.s3 import list_objects
from fpipe.file import File, FileStream, SeekableFileStream, FileMeta, FileStreamGenerator
from fpipe.utils.s3_reader import S3FileReader
from fpipe.utils.s3_write import S3FileWriter


class S3FileInfo(FileMeta):
    def __init__(self, reader: S3FileReader):
        super().__init__()
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

        return self.__s3_obj[key]

    def meta_gen(self) -> Iterable[FileMeta]:
        yield S3Version(self.reader.version)
        yield S3Key(self.path)

        yield S3Size(lambda: self._get_metadata('ContentLength'), self.reader.meta_lock)
        yield S3Modified(lambda: self._get_metadata('LastModified'), self.reader.meta_lock)
        yield S3Mime(lambda: self._get_metadata('ContentType'), self.reader.meta_lock)


class S3File(File):
    def __init__(self, bucket: str, key: S3Key, version: Optional[S3Version] = None):
        super().__init__()
        self.bucket = bucket
        self.key = key
        self.version = version


class S3PrefixFile(File):
    def __init__(self, bucket: str, prefix: str):
        super().__init__()
        self.bucket = bucket
        self.prefix = prefix


class S3SeekableFileStream(SeekableFileStream):
    def __init__(self, file):
        info = S3FileInfo(file)
        super().__init__(file, meta=list(info.meta_gen()))


class S3FileGenerator(FileStreamGenerator):
    def __init__(self,
                 files: Iterable[Union[S3File, S3PrefixFile, FileStream]],
                 client,
                 resource,
                 bucket: Optional[str] = None):
        super().__init__(files)
        self.bucket = bucket
        self.client = client
        self.resource = resource

        # self.stats = Stats(self.__class__.__name__, 1)

    def __iter__(self) -> Iterable[S3SeekableFileStream]:
        for source in self.files:
            try:
                client, resource = self.client, self.resource

                if isinstance(source, S3File):
                    bucket, key, version = source.bucket, source.key, source.version
                    reader = S3FileReader(client, resource, bucket, key.value,
                                          version=version.value if version else version)
                    yield S3SeekableFileStream(reader)
                elif isinstance(source, S3PrefixFile):
                    bucket, prefix = source.bucket, source.prefix
                    for o in list_objects(client, bucket, prefix):
                        reader = S3FileReader(client, resource, bucket, o['Key'])
                        yield S3SeekableFileStream(reader)
                elif isinstance(source, FileStream):
                    bucket = self.bucket
                    key = source.meta(Path).value
                    mime, encoding = guess_mime(key)

                    reader = S3FileReader(client, resource, bucket, key, lock=Lock(), meta_lock=Lock())

                    def transfer_thread():
                        with S3FileWriter(client, bucket, key, mime=mime) as writer:
                            while True:
                                b = source.file.read(writer.buffer.chunk_size)
                                # self.stats.w(b)
                                writer.write(b)
                                if not b:
                                    break
                        reader.version = writer.mpu_res.get('VersionId')
                        reader.release()

                    transfer_thread = Thread(target=transfer_thread, daemon=True, name=self.__class__.__name__)
                    transfer_thread.start()
                    yield S3SeekableFileStream(reader)
                    transfer_thread.join()

            except Exception:
                raise

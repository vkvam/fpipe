from threading import Thread, Lock
from typing import Generator, Iterable, Union, Optional, cast

from fpipe.fileinfo import FileInfoException
from fpipe.utils.mime import guess_mime
from fpipe.utils.s3 import list_objects
from .abstract import File, Stream, SeekableStream, FileMeta, FileStreamGenerator
from .utils.s3_reader import S3FileReader
from .utils.s3_write import S3FileWriter


class S3FileInfoCalculated(FileMeta):
    def __init__(self, reader: S3FileReader):
        super().__init__()
        self.reader = reader
        self.__s3_obj = None
        self.bucket = reader.bucket
        self.path = reader.key


    def _get_metadata(self, key: str):
        if self.reader.lock and self.reader.lock.locked():
            raise FileInfoException("S3 object meta is not available until after object has been written")

        self.__s3_obj = self.__s3_obj or self.reader.s3_client.get_object(
            Bucket=self.reader.bucket,
            Key=self.reader.key,
            **({'VersionId': self.version} if self.version else {})
        )

        return self.__s3_obj[key]

    @property
    def version(self):
        return self.reader.version

    @property
    def key(self):
        return self.path

    @property
    def size(self):
        return self._get_metadata('ContentLength')

    @property
    def modified(self):
        return self._get_metadata('LastModified')

    @property
    def mime(self):
        return self._get_metadata('ContentType')


class S3File(File):
    def __init__(self, bucket: str, key: str, version: Optional[str] = None):
        super().__init__()
        self.bucket = bucket
        self.key = key
        self.version = version


class S3Prefix(File):
    def __init__(self, bucket, prefix):
        super().__init__()
        self.bucket = bucket
        self.prefix = prefix


class S3SeekableStream(SeekableStream):
    def __init__(self, file):
        super().__init__(file, S3FileInfoCalculated(file))

    @property
    def meta(self) -> S3FileInfoCalculated:
        return cast(S3FileInfoCalculated, super().meta)


class S3FileGenerator(FileStreamGenerator):
    def __init__(self,
                 files: Iterable[Union[S3File, S3Prefix, Stream]],
                 client,
                 resource,
                 bucket: Optional[str] = None):
        super().__init__(files)
        self.bucket = bucket
        self.client = client
        self.resource = resource

        # self.stats = Stats(self.__class__.__name__, 1)

    def __iter__(self) -> Iterable[S3SeekableStream]:
        for source in self.files:
            try:
                client, resource = self.client, self.resource

                if isinstance(source, S3File):
                    bucket, key, version = source.bucket, source.key, source.version
                    reader = S3FileReader(client, resource, bucket, key, version=version)
                    yield S3SeekableStream(reader)
                elif isinstance(source, S3Prefix):
                    bucket, prefix = source.bucket, source.prefix
                    for o in list_objects(client, bucket, prefix):
                        reader = S3FileReader(client, resource, bucket, o['Key'])
                        yield S3SeekableStream(reader)
                elif isinstance(source, Stream):
                    bucket = self.bucket
                    key = source.meta.path
                    mime, encoding = guess_mime(key)

                    lock = Lock()
                    reader = S3FileReader(client, resource, bucket, key, lock=lock)

                    def transfer_thread():
                        with S3FileWriter(client, bucket, key, mime=mime) as writer:
                            while True:
                                b = source.file.read(writer.buffer.chunk_size)
                                # self.stats.w(b)
                                writer.write(b)
                                if not b:
                                    break
                        reader.version = writer.mpu_res.get('VersionId')
                        lock.release()

                    transfer_thread = Thread(target=transfer_thread, daemon=True, name=self.__class__.__name__)
                    transfer_thread.start()

                    yield S3SeekableStream(reader)
                    transfer_thread.join()

            except Exception:
                raise

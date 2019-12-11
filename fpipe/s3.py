import mimetypes
from threading import Thread, Lock
from typing import Generator, Iterable, Union, Optional, cast

from fpipe.fileinfo import FileInfoException
from .abstract import File, FileGenerator, Stream, SeekableStream, FileMeta
from .utils.s3_reader import S3FileReader
from .utils.s3_write import S3FileWriter


class S3FileInfoCalculated(FileMeta):
    def __init__(self, reader: S3FileReader):
        super().__init__()
        self.reader = reader
        self.meta = None
        self.boto_meta_map = {
            'size': lambda x: x['ContentLength'],
            'modified': lambda x: x['LastModified']
        }

    def _get_metadata(self, key: str):
        if self.reader.lock and self.reader.lock.locked():
            raise FileInfoException("S3 object meta is not available until after object has been written")
        self.meta = self.meta or self.reader.s3_client.get_object(Bucket=self.reader.bucket, Key=self.reader.key)
        return self.boto_meta_map[key](self.meta)

    @property
    def size(self):
        return self._get_metadata('size')

    @property
    def modified(self):
        return self._get_metadata('modified')


class S3File(File):
    def __init__(self, path: str):
        super().__init__(S3FileInfoCalculated(None))  # TODO: Fix
        self.path = path


class S3SeekableStream(SeekableStream):
    def __init__(self, file):
        super().__init__(file, S3FileInfoCalculated(file))

    @property
    def meta(self) -> S3FileInfoCalculated:
        return cast(S3FileInfoCalculated, super().meta)


class S3FileGenerator(FileGenerator):
    def __init__(self, files: Iterable[Union[S3File, Stream]], client, resource, bucket: Optional[str] = None):
        super().__init__(files)
        self.bucket = bucket
        self.client = client
        self.resource = resource

        # self.stats = Stats(self.__class__.__name__, 1)

    def get_files(self) -> Generator[S3SeekableStream, None, None]:
        for source in self.files:
            try:
                client, resource = self.client, self.resource

                if isinstance(source, S3File):
                    bucket, key = self.bucket, source.path
                    reader = S3FileReader(client, resource, bucket, key)
                    yield S3SeekableStream(reader)

                elif isinstance(source, Stream):

                    bucket = self.bucket
                    key = source.meta.path
                    # TODO: Use default byte tipe if no mime is found
                    mime, encoding = mimetypes.guess_type(key, False)

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
                        lock.release()

                    transfer_thread = Thread(target=transfer_thread, daemon=True, name=self.__class__.__name__)
                    transfer_thread.start()

                    yield S3SeekableStream(reader)
                    transfer_thread.join()

            except Exception:
                raise

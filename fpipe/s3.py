import mimetypes
from threading import Thread, Lock
from typing import Generator, Iterable, Union, Optional

from .abstract import File, FileGenerator, Stream, SeekableStream, FileInfoCalculator, FileInfo
from .utils import Stats
from .utils.s3_reader import S3FileReader
from .utils.s3_write import S3FileWriter


class S3FileInfoCalculator(FileInfoCalculator):
    def __init__(self, path):
        super().__init__()
        self.path = path

    def write(self, b: bytes):
        pass

    def get(self) -> FileInfo:
        return FileInfo(path=self.path)


class S3File(File):
    def __init__(self, path: str):
        super().__init__(S3FileInfoCalculator(path))
        self.path = path


class S3SeekableStream(SeekableStream):
    pass


class S3FileGenerator(FileGenerator):
    def __init__(self, files: Iterable[Union[S3File, Stream]], client, resource, bucket: Optional[str] = None):
        super().__init__(files)
        self.bucket = bucket
        self.client = client
        self.resource = resource

        self.stats = Stats(self.__class__.__name__, 1)

    def get_files(self) -> Generator[S3SeekableStream, None, None]:
        for source in self.files:
            try:
                client, resource = self.client, self.resource

                if isinstance(source, S3File):
                    bucket, key = self.bucket, source.path
                    reader = S3FileReader(client, resource, bucket, key)
                    yield S3SeekableStream(reader, S3FileInfoCalculator(key))

                elif isinstance(source, Stream):

                    bucket = self.bucket
                    key = source.file_info_generator.get().path
                    mime, encoding = mimetypes.guess_type(key, False)

                    lock = Lock()
                    reader = S3FileReader(client, resource, bucket, key, lock=lock)

                    def transfer_thread():
                        with S3FileWriter(client, bucket, key, mime=mime) as writer:
                            while True:
                                b = source.file.read(writer.buffer.chunk_size)
                                self.stats.w(b)
                                writer.write(b)
                                if not b:
                                    break
                        lock.release()

                    transfer_thread = Thread(target=transfer_thread)
                    transfer_thread.start()

                    yield S3SeekableStream(reader, S3FileInfoCalculator(key))

                    transfer_thread.join()

            except Exception as e:
                raise e

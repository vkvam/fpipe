from threading import Lock, Thread
from typing import Iterable, Union, Optional

from fpipe.exceptions import FileException
from fpipe.file import FileStream
from fpipe.file.s3 import S3File, S3PrefixFile, S3SeekableFileStream
from fpipe.gen.abstract import FileStreamGenerator
from fpipe.meta import Path
from fpipe.utils.mime import guess_mime
from fpipe.utils.s3 import list_objects
from fpipe.utils.s3_reader import S3FileReader
from fpipe.utils.s3_writer import S3FileWriter


class S3FileGenerator(FileStreamGenerator):
    f_type = Union[S3File, S3PrefixFile, FileStream]

    def __init__(self,
                 client,
                 resource,
                 bucket: Optional[str] = None,
                 seekable=False):
        super().__init__()
        self.bucket = bucket
        self.client = client
        self.resource = resource
        self.seekable = seekable

    def __iter__(self) -> Iterable[FileStream]:
        for source in self.files:
            try:
                client, resource = self.client, self.resource

                if isinstance(source, S3File):
                    bucket, key, version = source.bucket, source.key, source.version

                    with S3FileReader(client,
                                      resource,
                                      bucket,
                                      key.value,
                                      version=version.value if version else None,
                                      seekable=self.seekable) as reader:
                        yield S3SeekableFileStream(reader, parent=source)
                elif isinstance(source, S3PrefixFile):
                    bucket, prefix = source.bucket, source.prefix
                    for o in list_objects(client, bucket, prefix):
                        with S3FileReader(client, resource, bucket, o['Key'], seekable=self.seekable) as reader:
                            yield S3SeekableFileStream(reader, parent=source)
                elif isinstance(source, FileStream):
                    if not self.bucket:
                        raise FileException("FileStream source needs bucket defined")
                    bucket = self.bucket
                    path: Path = source.meta(Path)
                    mime, encoding = guess_mime(path.value)
                    read_lock = Lock()
                    with S3FileReader(client,
                                      resource,
                                      bucket,
                                      path.value,
                                      lock=read_lock,
                                      meta_lock=Lock(),
                                      seekable=self.seekable) as reader:

                        def write_to_s3():
                            with S3FileWriter(client, bucket, path.value, mime) as writer:
                                while True:
                                    b = source.file.read(writer.buffer.chunk_size)
                                    # self.stats.w(b)
                                    writer.write(b)
                                    if not b:
                                        break
                            reader.version = writer.mpu_res.get('VersionId')
                            # Release reader when we are done writing
                            read_lock.release()

                        transfer_thread = Thread(target=write_to_s3, daemon=True, name=self.__class__.__name__)
                        transfer_thread.start()
                        yield S3SeekableFileStream(reader, parent=source)
                        transfer_thread.join()
                else:
                    raise FileException(f"FileStream source {source.__class__.__name} not valid")

            except Exception:
                raise

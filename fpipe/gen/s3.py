from threading import Lock, Thread
from typing import Union, Optional, Generator, Callable
from fpipe.exceptions import FileException, FileMetaException
from fpipe.file import FileStream, File, SeekableFileStream
from fpipe.file.s3 import S3File, S3PrefixFile, S3SeekableFileStream
from fpipe.gen.generator import FileGenerator, FileGeneratorResponse
from fpipe.meta import Path, Version
from fpipe.utils.mime import guess_mime
from fpipe.utils.s3_reader import S3FileReader
from fpipe.utils.s3_writer import S3FileWriter


class S3(FileGenerator[FileStream, SeekableFileStream]):
    f_type = Union[S3File, S3PrefixFile, FileStream]

    def __init__(
            self,
            client,
            resource,
            bucket: Optional[str] = None,
            seekable=False,
            pathname_resolver: Callable[[File], str] = None,
    ):
        super().__init__()
        self.bucket = bucket
        self.client = client
        self.resource = resource
        self.seekable = seekable
        self.pathname_resolver = pathname_resolver

    @staticmethod
    def write_to_s3(
            client,
            bucket,
            path,
            reader,
            read_lock,
            source: FileStream,
            mime: str,
            encoding: str,
    ):
        try:
            with S3FileWriter(client, bucket, path.value, mime) as writer:
                while True:
                    b = source.file.read(writer.buffer.chunk_size)
                    # self.stats.w(b)
                    writer.write(b)
                    if not b:
                        break
            if writer.mpu_res:
                reader.version = writer.mpu_res.get("VersionId")
            else:
                raise FileException
            # TODO: Exception from thread should be thrown on main thread
            # before pipe reaches reader and throws
            # FileException("Could not locate S3 object....")
        finally:
            # Release reader when we are done writing, or writing failed
            read_lock.release()

    def process(
            self, source: File
    ) -> Optional[Generator[FileGeneratorResponse, None, None]]:
        client, resource = self.client, self.resource

        if isinstance(source, S3File):
            bucket, key = source.bucket, source.meta(Path)
            try:
                version: Optional[Version] = source.meta(Version)
            except FileMetaException:  # Version not provided to source
                version = None

            with S3FileReader(
                    client,
                    resource,
                    bucket,
                    key.value,
                    version=version.value if version else None,
                    seekable=self.seekable,
            ) as reader:
                yield FileGeneratorResponse(
                    S3SeekableFileStream(reader, parent=source)
                )
        elif isinstance(source, S3PrefixFile):
            bucket, prefix = source.bucket, source.prefix
            for o in self.list_objects(client, bucket, prefix):
                with S3FileReader(
                        client, resource, bucket, o["Key"],
                        seekable=self.seekable
                ) as reader:
                    yield FileGeneratorResponse(
                        S3SeekableFileStream(reader, parent=source)
                    )
        elif isinstance(source, FileStream):
            if not self.bucket:
                raise FileException("FileStream source needs bucket defined")
            bucket = self.bucket

            path = (
                Path(self.pathname_resolver(source))
                if self.pathname_resolver
                else source.meta(Path)
            )

            mime, encoding = guess_mime(path.value)
            read_lock = Lock()
            with S3FileReader(
                    client,
                    resource,
                    bucket,
                    path.value,
                    lock=read_lock,
                    meta_lock=Lock(),
                    seekable=self.seekable,
            ) as reader:

                thread_args = (
                    client,
                    bucket,
                    path,
                    reader,
                    read_lock,
                    source,
                    mime,
                    encoding,
                )
                yield FileGeneratorResponse(
                    S3SeekableFileStream(reader, parent=source),
                    Thread(
                        target=self.write_to_s3,
                        args=thread_args,
                        daemon=True,
                        name=self.__class__.__name__,
                    ),
                )
        else:
            raise FileException(
                f"FileStream source {source.__class__.__name__} not valid"
            )

    @staticmethod
    def list_objects(
            client, bucket: str, prefix: str = None, use_generator: bool = True
    ):
        """
        Get all objects as a generator.
        :param client: boto3 s3 client
        :param bucket: Bucket name
        :param prefix: Object prefix
        :param use_generator: Use generator
        :return: generator or list
        """

        args = {"Bucket": bucket}
        if prefix is not None:
            args["Prefix"] = prefix

        paginator = client.get_paginator("list_objects_v2")
        page_iterator = paginator.paginate(**args)

        def get_generator():
            for page in page_iterator:
                for item in page["Contents"]:
                    yield item

        return get_generator() if use_generator else list(get_generator())

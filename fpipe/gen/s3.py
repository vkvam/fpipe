from threading import Lock, Thread
from typing import Optional, Generator, Union, Iterable, BinaryIO
from fpipe.exceptions import FileException, FileMetaException
from fpipe.file import File
from fpipe.gen.generator import FileGenerator, FileGeneratorResponse, \
    MetaResolver
from fpipe.meta import Path, Version, Bucket, Prefix
from fpipe.meta.s3 import S3MetadataProducer
from fpipe.utils.mime import guess_mime
from fpipe.utils.s3_reader import S3FileReader
from fpipe.utils.s3_writer import S3FileWriter


class S3(FileGenerator):
    """Generates Files with metadata: Size, Modified, Mime and Path.

    source or process_meta must provide metadata Bucket and Path or Prefix,
    """
    def __init__(
            self,
            client,
            resource,
            seekable=False,
            process_meta: Optional[
                Union[Iterable[MetaResolver], MetaResolver]] = None
    ):
        """

        :param client:
        :param resource:
        :param seekable:
        :param process_meta: MetaResolver to provider addition FileMeta
        if source File does not provide everything needed
        """
        super().__init__(process_meta)
        self.client = client
        self.resource = resource
        self.seekable = seekable

    @staticmethod
    def build_file_stream(reader: S3FileReader, parent: Optional[File] = None):
        info = S3MetadataProducer(reader)
        return File(
            file=reader,
            meta=list(info.generate()),
            parent=parent
        )

    @staticmethod
    def write_to_s3(
            client,
            bucket,
            path,
            reader,
            read_lock,
            source: BinaryIO,
            mime: str,
            encoding: str,
    ):
        try:
            with S3FileWriter(client, bucket, path, mime) as writer:
                while True:
                    b = source.read(writer.buffer.chunk_size)
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
            self,
            source: File,
            process_meta: File
    ) -> Optional[Generator[FileGeneratorResponse, None, None]]:
        client, resource = self.client, self.resource

        bucket = File.meta_prioritized(
            Bucket,
            process_meta,
            source
        ).value
        try:
            key = File.meta_prioritized(
                Path,
                process_meta,
                source
            ).value
            prefix = None
        except FileMetaException as e:
            try:
                key = None
                prefix = File.meta_prioritized(
                    Prefix,
                    process_meta,
                    source).value
            except FileMetaException as e2:
                raise e2 from e

        if key:
            if source.file:
                bucket = File.meta_prioritized(
                    Bucket,
                    process_meta,
                    source
                ).value

                mime, encoding = guess_mime(key)
                read_lock = Lock()
                with S3FileReader(
                        client,
                        resource,
                        bucket,
                        key,
                        lock=read_lock,
                        meta_lock=Lock(),
                        seekable=self.seekable,
                ) as reader:

                    thread_args = (
                        client,
                        bucket,
                        key,
                        reader,
                        read_lock,
                        source.file,
                        mime,
                        encoding,
                    )
                    yield FileGeneratorResponse(
                        self.build_file_stream(reader, source),
                        Thread(
                            target=self.write_to_s3,
                            args=thread_args,
                            daemon=True,
                            name=self.__class__.__name__,
                        ),
                    )
            else:
                try:
                    version = File.meta_prioritized(
                        Version,
                        process_meta,
                        source
                    )
                except FileMetaException:  # Version not provided to source
                    version = None

                with S3FileReader(
                        client,
                        resource,
                        bucket,
                        key,
                        version=version.value if version else None,
                        seekable=self.seekable,
                ) as reader:
                    yield FileGeneratorResponse(
                        self.build_file_stream(reader, source)
                    )
        elif prefix:
            for o in self.list_objects(client, bucket, prefix):
                with S3FileReader(
                        client, resource, bucket, o["Key"],
                        seekable=self.seekable
                ) as reader:
                    yield FileGeneratorResponse(
                        self.build_file_stream(reader, source)
                    )
        else:
            raise FileException(
                f"File source {source.__class__.__name__} not valid"
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

from typing import Optional

from fpipe.file import File, SeekableFileStream
from fpipe.meta.s3 import S3Key, S3Version, S3FileInfo
from fpipe.utils.s3_reader import S3FileReader


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
    def __init__(self, file: S3FileReader, parent=None):
        info = S3FileInfo(file)
        super().__init__(file, meta=list(info.meta_gen()), parent=parent)

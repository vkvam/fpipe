from typing import Optional, List

from fpipe.file.file import File, SeekableFileStream
from fpipe.meta.abstract import FileMeta
from fpipe.meta.path import Path
from fpipe.meta.s3 import S3MetadataProducer
from fpipe.meta import Version
from fpipe.utils.s3_reader import S3FileReader


class S3File(File):
    def __init__(self, bucket: str, key: str, version: Optional[str] = None):
        meta: List[FileMeta] = [Path(key)]
        if version:
            meta.append(Version(version))
        super().__init__(meta=meta)
        self.bucket = bucket


class S3PrefixFile(File):
    def __init__(self, bucket: str, prefix: str):
        super().__init__()
        self.bucket = bucket
        self.prefix = prefix


class S3SeekableFileStream(SeekableFileStream):
    def __init__(self, file: S3FileReader, parent=None):
        info = S3MetadataProducer(file)
        super().__init__(file, meta=list(info.generate()), parent=parent)

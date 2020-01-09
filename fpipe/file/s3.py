from typing import Optional

from fpipe.file.file import File
from fpipe.meta.bucket import Bucket
from fpipe.meta.path import Path
from fpipe.meta.version import Version
from fpipe.meta.prefix import Prefix


class S3File(File):
    def __init__(self, bucket: str, key: str, version: Optional[str] = None):
        super().__init__(
            meta=(
                Bucket(bucket),
                Path(key)
            )
        )
        if version:
            self.meta_map.set(Version(version))


class S3PrefixFile(File):
    def __init__(self, bucket: str, prefix: str):
        super().__init__(meta=(Bucket(bucket), Prefix(prefix)))

import tarfile
from typing import Iterable

from fpipe.file import File, FileStream
from fpipe.generators.abstract import FileStreamGenerator
from fpipe.meta.abstract import FileMetaValue
from fpipe.meta.path import Path
from fpipe.meta.size import Size


# TODO: Merge with S3Modified
class ModifiedTar(FileMetaValue[int]):
    pass


# TODO: Add needed
# Check A TarInfo object has the following public data attributes:
# https://docs.python.org/3/library/tarfile.html


class TarFile(File):
    def __init__(self):
        super().__init__()


class TarFileGenerator(FileStreamGenerator[FileStream]):
    def __init__(self, files: Iterable[FileStream]):
        super().__init__(files)

    def __iter__(self) -> Iterable[FileStream]:
        for source in self.files:
            try:
                with tarfile.open(fileobj=source.file, mode='r|*') as tar_content_stream:
                    for tar_info in tar_content_stream:
                        tar_stream = tar_content_stream.extractfile(tar_info)

                        if tar_stream:
                            yield FileStream(tar_stream, meta=(
                                Size(tar_info.size),
                                ModifiedTar(tar_info.mtime),
                                Path(tar_info.name)
                            ))
            except (FileNotFoundError, tarfile.TarError):
                raise

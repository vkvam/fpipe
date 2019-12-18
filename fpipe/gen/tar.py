import tarfile
from typing import Iterable

from fpipe.file.file import FileStream
from fpipe.gen.abstract import FileStreamGenerator
from fpipe.meta.path import Path
from fpipe.meta.size import Size
from fpipe.meta.tar import ModifiedTar


class TarFileGenerator(FileStreamGenerator[FileStream]):
    def __init__(self):
        super().__init__()

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

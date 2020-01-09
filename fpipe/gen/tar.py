import datetime
import tarfile
from typing import cast, BinaryIO

from fpipe.file.file import FileStream, File
from fpipe.gen.generator import FileGenerator, FileGeneratorResponse
from fpipe.meta import Modified
from fpipe.meta.path import Path
from fpipe.meta.size import Size


class Tar(FileGenerator[FileStream, FileStream]):
    """Generates FileStreams with metadata: Size, Modified and Path

    source must be a tar bytestream
    """

    def process(self, source: FileStream, meta_container: File):
        with tarfile.open(
                fileobj=source.file, mode="r|*"
        ) as tar_content_stream:
            for tar_info in tar_content_stream:
                tar_stream = tar_content_stream.extractfile(tar_info)

                if tar_stream:
                    yield FileGeneratorResponse(
                        FileStream(
                            cast(BinaryIO, tar_stream),
                            meta=(
                                Size(tar_info.size),
                                Modified(
                                    datetime.datetime.fromtimestamp(
                                        tar_info.mtime
                                    )
                                ),
                                Path(tar_info.name),
                            ),
                            parent=source
                        )
                    )

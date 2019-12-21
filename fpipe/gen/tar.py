import datetime
import tarfile
from typing import Optional, Generator


from fpipe.file.file import FileStream
from fpipe.gen.callable import CallableGen, CallableResponse
from fpipe.meta import Modified
from fpipe.meta.path import Path
from fpipe.meta.size import Size


class Tar(CallableGen[FileStream]):
    def executor(self, source: FileStream) -> Optional[Generator[CallableResponse, None, None]]:
        try:
            with tarfile.open(fileobj=source.file, mode='r|*') as tar_content_stream:
                for tar_info in tar_content_stream:
                    tar_stream = tar_content_stream.extractfile(tar_info)

                    if tar_stream:
                        yield CallableResponse(
                            FileStream(tar_stream, meta=(
                                Size(tar_info.size),
                                Modified(datetime.datetime.fromtimestamp(tar_info.mtime)),
                                Path(tar_info.name)
                            ))
                        )
        except (FileNotFoundError, tarfile.TarError):
            raise

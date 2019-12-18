from typing import Iterable

from fpipe.gen.abstract import FileStreamGenerator, FileStream
from fpipe.meta.path import Path
from fpipe.utils.ftp import FTPClient


class FTPFileGenerator(FileStreamGenerator):
    def __init__(self):
        super().__init__()

    def __iter__(self) -> Iterable[FileStream]:
        for source in self.files:

            ftp_client = FTPClient(host=source.host,
                                   username=source.username,
                                   password=source.password,
                                   blocksize=source.block_size,
                                   port=source.port)
            try:
                ftp_client.write_to_file_threaded(source.meta(Path).value)
                yield FileStream(ftp_client.bytes_io, parent=source)
            except Exception as e:
                raise e
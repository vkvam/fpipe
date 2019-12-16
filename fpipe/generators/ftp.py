from typing import Iterable

from fpipe.generators.abstract import FileStreamGenerator, File, FileStream
from fpipe.meta.path import Path
from fpipe.utils.ftp import FTPClient


class FTPFile(File):
    def __init__(self, path: str, host: str, username: str, password: str, port: int, block_size: int = 2 ** 23):
        super().__init__(meta=[Path(path)])
        self.path = path
        self.host = host
        self.username = username
        self.password = password
        self.port = port
        self.block_size = block_size


class FTPFileGenerator(FileStreamGenerator):
    def __init__(self, files: Iterable[File]):
        super().__init__(files)

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

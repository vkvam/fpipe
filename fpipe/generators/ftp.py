from typing import Iterable
from fpipe.utils import FTPClient
from fpipe.file import FileStream, File, FileStreamGenerator, FileMeta


class FTPFile(File):
    def __init__(self, path: str, host: str, username: str, password: str, port: int, block_size: int = 2 ** 23):
        super().__init__(FileMeta(path))
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
                ftp_client.write_to_file_threaded(source.path)
                yield FileStream(ftp_client.bytes_io, parent=source)
            except Exception as e:
                raise e

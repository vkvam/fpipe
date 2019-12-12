import os
from typing import Generator, Iterable
from .abstract import FileGenerator, Stream, FileMetaCalculated, FileMeta, File, FileStreamGenerator
from .utils import FTPClient

FNULL = open(os.devnull, 'w')


class FTPFileInfoCalculated(FileMetaCalculated):
    def __init__(self, path):
        super().__init__()
        self.path = path

    def write(self, b: bytes):
        pass

    def get(self) -> FileMeta:
        return FileMeta(path=self.path)


class FTPFile(File):
    def __init__(self, path: str, host: str, username: str, password: str, port: int, block_size: int = 2 ** 23):
        super().__init__(FTPFileInfoCalculated(path))
        self.path = path
        self.host = host
        self.username = username
        self.password = password
        self.port = port
        self.block_size = block_size


class FTPStream(Stream):
    pass


class FTPFileGenerator(FileStreamGenerator):
    def __init__(self, files: Iterable[File]):
        super().__init__(files)

    def __iter__(self) -> Iterable[FTPStream]:
        for source in self.files:

            ftp_client = FTPClient(host=source.host,
                                   username=source.username,
                                   password=source.password,
                                   blocksize=source.block_size,
                                   port=source.port)
            try:
                ftp_client.write_to_file_threaded(source.path)
                yield Stream(ftp_client.bytes_io, parent=source)
            except Exception as e:
                raise e

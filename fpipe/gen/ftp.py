from fpipe.gen.callable import MethodGen
from fpipe.file import FTPFile, File
from fpipe.gen.abstract import FileStream
from fpipe.gen.callable import CallableResponse
from fpipe.meta.path import Path
from fpipe.utils.ftp import FTPClient


class FTP(MethodGen[File, FileStream]):
    def executor(self, source: File):
        if isinstance(source, FTPFile):
            ftp_client = FTPClient(
                host=source.host,
                username=source.username,
                password=source.password,
                block_size=source.block_size,
                port=source.port,
            )
            thread, bytes_io = ftp_client.write_to_file_threaded(
                source.meta(Path).value
            )
            yield CallableResponse(FileStream(bytes_io, parent=source), thread)
        else:
            raise NotImplementedError

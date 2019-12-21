from fpipe.gen.callable import MethodGen
from fpipe.file import FTPFile
from fpipe.gen.abstract import FileStream
from fpipe.gen.callable import CallableResponse
from fpipe.meta.path import Path
from fpipe.utils.ftp import FTPClient


class FTP(MethodGen[FileStream]):
    def executor(self, source: FTPFile):
        ftp_client = FTPClient(
            host=source.host,
            username=source.username,
            password=source.password,
            blocksize=source.block_size,
            port=source.port,
        )
        thread, bytes_io = ftp_client.write_to_file_threaded(
            source.meta(Path).value
        )
        yield CallableResponse(FileStream(bytes_io, parent=source), thread)

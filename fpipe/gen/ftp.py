from fpipe.file import File
from fpipe.gen.generator import FileGenerator, FileGeneratorResponse
from fpipe.meta.blocksize import BlockSize
from fpipe.meta.host import Host
from fpipe.meta.password import Password
from fpipe.meta.path import Path
from fpipe.meta.port import Port
from fpipe.meta.username import Username
from fpipe.utils.ftp import FTPClient


class FTP(FileGenerator):
    def process(self,
                source: File,
                generated_meta_container: File):
        ftp_client = FTPClient(
            host=source[Host],
            username=source[Username],
            password=source[Password],
            block_size=source[BlockSize],
            port=source[Port]
        )
        thread, bytes_io = ftp_client.write_to_file_threaded(
            source[Path]
        )
        yield FileGeneratorResponse(
            File(stream=bytes_io, parent=source),
            thread
        )

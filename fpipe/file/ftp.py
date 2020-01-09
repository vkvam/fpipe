from fpipe.file import File
from fpipe.meta import Path
from fpipe.meta.blocksize import BlockSize
from fpipe.meta.host import Host
from fpipe.meta.password import Password
from fpipe.meta.port import Port
from fpipe.meta.username import Username
from fpipe.utils.const import DEFAULT_FTP_BLOCK_SIZE


class FTPFile(File):
    def __init__(
        self,
        path: str,
        host: str,
        username: str,
        password: str,
        port: int,
        block_size: int = DEFAULT_FTP_BLOCK_SIZE
    ):
        super().__init__(meta=(
            Path(path),
            Host(host),
            Username(username),
            Password(password),
            Port(port),
            BlockSize(block_size)

        ))

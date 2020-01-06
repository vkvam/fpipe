from fpipe.file import File
from fpipe.meta import Path
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
        super().__init__(meta=[Path(path)])
        self.path = path
        self.host = host
        self.username = username
        self.password = password
        self.port = port
        self.block_size = block_size

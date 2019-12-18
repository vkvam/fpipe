from fpipe.file import File
from fpipe.meta import Path


class FTPFile(File):
    def __init__(self, path: str, host: str, username: str, password: str, port: int, block_size: int = 2 ** 23):
        super().__init__(meta=[Path(path)])
        self.path = path
        self.host = host
        self.username = username
        self.password = password
        self.port = port
        self.block_size = block_size
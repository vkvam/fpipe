import ftplib
import socket
import threading
from typing import BinaryIO, Optional

from fpipe.utils.bytesloop import BytesLoop


class FTPClient(object):
    def __init__(
            self,
            host: str,
            username: str,
            password: str,
            blocksize: Optional[int] = None,
            timeout: Optional[int] = None,
            port: int = 21
    ):
        self.host: str = host
        self.port: int = port
        self.user: str = username
        self.passwd: str = password
        self.timeout: int = timeout or 60
        self.blocksize: Optional[int] = blocksize
        self.md5: Optional[str] = None

    def _get_session(self):
        ftp = ftplib.FTP()
        ftp.connect(host=self.host, port=self.port, timeout=self.timeout)
        ftp.login(user=self.user, passwd=self.passwd)
        return ftp

    def write_to_file_threaded(self, path):
        with BytesLoop(self.blocksize) as bytes_io:
            thread = threading.Thread(
                target=self.write_to_file, args=(path, bytes_io), daemon=True
            )
            return thread, bytes_io

    def write_to_file(self, path: str, bytes_io: BinaryIO):
        ftp = self._get_session()
        exception = None
        kwargs = dict(cmd="RETR " + path,
                      callback=bytes_io.write,
                      blocksize=self.blocksize)
        try:
            ftp.retrbinary(
                **{k: v for k, v in kwargs.items() if v is not None}
            )
            # For some reason retrbinary does not send EOF
            bytes_io.write(b"")
        except socket.timeout as e:
            exception = e
        finally:
            ftp.close()

        return self.md5, exception

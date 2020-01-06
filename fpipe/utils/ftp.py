import ftplib
from pyftpdlib import log as pyftpdlib_log

import logging
import threading

from typing import BinaryIO, Optional

from fpipe.utils.bytesloop import BytesLoop


class FTPClient(object):
    def __init__(
            self,
            host: str,
            username: str,
            password: str,
            block_size: Optional[int] = None,
            timeout: Optional[int] = None,
            port: int = 21,
            ftplib_log_level: int = 0,
            pyftpdlib_log_level: int = logging.WARNING
    ):
        """

        :param host: dns name or ip address
        :param port: ftp port, usually 21
        :param username: ftp account username
        :param password: ftp account password
        :param block_size: size of each block requested from ftp server
        :param timeout: idle timeout of ftp server
        :param ftplib_log_level: log level for ftplib library
        :param pyftpdlib_log_level: log level for pyftpdlib
        """
        self.host: str = host
        self.port: int = port
        self.user: str = username
        self.passwd: str = password
        self.timeout: int = timeout or 60
        self.blocksize: Optional[int] = block_size
        self.md5: Optional[str] = None

        self.ftp = ftplib.FTP(user=self.user, passwd=self.passwd)

        self.ftp.set_debuglevel(ftplib_log_level)
        pyftpdlib_log.config_logging(level=pyftpdlib_log_level)

    def _get_session(self):
        self.ftp.connect(host=self.host, port=self.port, timeout=self.timeout)
        self.ftp.login()
        return self.ftp

    def write_to_file_threaded(self, path):
        with BytesLoop(self.blocksize) as bytes_io:
            thread = threading.Thread(
                target=self.write_to_file, args=(path, bytes_io), daemon=True
            )
            return thread, bytes_io

    def write_to_file(self, path: str, bytes_io: BinaryIO):
        with self._get_session() as ftp:
            exception = None
            kwargs = dict(cmd="RETR " + path,
                          callback=bytes_io.write,
                          blocksize=self.blocksize)

            ftp.retrbinary(
                **{k: v for k, v in kwargs.items() if v is not None}
            )
            bytes_io.write(b"")

            return self.md5, exception

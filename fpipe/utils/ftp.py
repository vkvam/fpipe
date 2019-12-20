import ftplib
import socket
import threading
from typing import IO

from fpipe.utils.bytesloop import BytesLoop


class FTPClient(object):
    def __init__(self, host, username, password, blocksize=None, timeout=None, port=21):
        self.host = host
        self.port = port
        self.user = username
        self.passwd = password

        self.timeout = timeout or 60

        self.blocksize = blocksize
        self.md5 = None

    def _get_session(self):
        ftp = ftplib.FTP()
        ftp.connect(host=self.host, port=self.port, timeout=self.timeout)
        ftp.login(user=self.user, passwd=self.passwd)
        return ftp

    def write_to_file_threaded(self, path):
        with BytesLoop(self.blocksize) as bytes_io:
            thread = threading.Thread(target=self.write_to_file, args=(path, bytes_io), daemon=True)
            return thread, bytes_io

    def write_to_file(self, path: str, bytes_io: IO[bytes]):
        ftp = self._get_session()
        exception = None

        try:
            ftp.retrbinary('RETR ' + path, bytes_io.write, blocksize=self.blocksize)
            # For some reason retrbinary does not send EOF
            bytes_io.write(b'')
        except socket.timeout as e:
            exception = e
        finally:
            ftp.close()

        return self.md5, exception

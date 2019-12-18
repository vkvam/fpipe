import ftplib
import socket
import threading

from fpipe.utils.bytesloop import BytesLoop


class FTPClient(object):
    def __init__(self, host, username, password, blocksize=None, timeout=None, port=21):
        self.bytes_io = BytesLoop(blocksize)
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
        thread = threading.Thread(target=self.write_to_file, args=(path,), daemon=True)
        thread.start()
        return thread

    def write_to_file(self, path: str):
        ftp = self._get_session()
        exception = None

        try:
            ftp.retrbinary('RETR ' + path, self.bytes_io.write, blocksize=self.blocksize)
            # For some reason retrbinary does not send EOF
            self.bytes_io.write(b'')
        except socket.timeout as e:
            exception = e
        finally:
            ftp.close()

        return self.md5, exception

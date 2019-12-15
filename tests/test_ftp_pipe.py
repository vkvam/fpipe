import hashlib
import os

from unittest import TestCase

from fpipe.calculators import MD5CheckSum
from fpipe.file.file import Path
from fpipe.generators.fileinfo import FileInfoGenerator
from fpipe.generators import FTPFileGenerator, FTPFile
from fpipe.generators import ProcessFileGenerator
from test_utils.ftp_server import TestFTPServer


class TestFTP(TestCase):
    @staticmethod
    def __checksum(data: bytes):
        sig = hashlib.md5()
        sig.update(data)
        return sig.hexdigest()

    def test_chaining_ftp_server(self):
        port = 2121
        ftp_server_thread = TestFTPServer(port=port)
        ftp_server_thread.start()

        files_in = {
            f"temp_{size}.testfile": size
            for size in (
                2 ** x for x in range(20, 23)
            )
        }

        # Flag that is checked to ensure certain parts of the code has been run
        run_balance = False

        try:
            for path, size in files_in.items():
                with open(path, 'wb') as f:
                    f.write(b'x'*size)

            gen = FTPFileGenerator(
                FTPFile(
                    path,
                    host='localhost',
                    username='user',
                    password='12345',
                    block_size=2 ** 14,
                    port=port
                ) for path in files_in.keys()
            )

            # Encrypt and decrypt
            gen = ProcessFileGenerator(
                gen,
                "gpg --batch --symmetric --passphrase 'X'"
            )

            gen = ProcessFileGenerator(
                gen,
                "gpg --batch --decrypt --passphrase 'X'"
            )

            # Calculate checksum of output
            gen = FileInfoGenerator(gen, [MD5CheckSum])

            for file_out in gen:
                content_out = file_out.file.read()
                run_balance = False

                with open(file_out.meta(Path).value, 'rb') as f:
                    source_content = f.read()
                    md5 = hashlib.md5()
                    md5.update(source_content)

                    self.assertEqual(content_out, source_content)
                    self.assertEqual(file_out.meta(MD5CheckSum).value, md5.hexdigest())
                    run_balance = True
                self.assertTrue(run_balance)

            ftp_server_thread.stop()
        finally:
            for path in files_in.keys():
                try:
                    os.remove(path)
                except:
                    pass

        self.assertTrue(run_balance)

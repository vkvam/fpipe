import hashlib
import os

from unittest import TestCase

from fpipe.gen import MetaGen, ProcessGen, FTPGen
from fpipe.file import FTPFile
from fpipe.meta import MD5Calculated, Path
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
                2 ** x for x in range(20, 24)
            )
        }

        # Flag that is checked to ensure certain parts of the code has been run
        run_balance = False

        try:
            for path, size in files_in.items():
                with open(path, 'wb') as f:
                    f.write(b'x' * size)
            gen = FTPGen().chain(
                FTPFile(
                    path,
                    host='localhost',
                    username='user',
                    password='12345',
                    block_size=2 ** 14,
                    port=port
                ) for path in files_in.keys()
            )

            # Checksum of input
            gen = MetaGen(MD5Calculated).chain(
                gen
            )

            # Encrypt and decrypt
            gen = ProcessGen("gpg --batch --symmetric --passphrase 'X'").chain(
                gen
            )

            # # Checksum of encrypted
            gen = MetaGen(MD5Calculated).chain(
                gen
            )

            gen = ProcessGen("gpg --batch --decrypt --passphrase 'X'").chain(
                gen
            )

            # # Checksum of decrypted
            gen = MetaGen(MD5Calculated).chain(
                gen
            )

            for file_out in gen:
                content_out = file_out.file.read()
                run_balance = False

                with open(file_out.meta(Path).value, 'rb') as f:
                    source_content = f.read()
                    md5 = hashlib.md5()
                    md5.update(source_content)

                    # Source and output are equal
                    self.assertEqual(content_out, source_content)

                    # Source file
                    self.assertEqual(file_out.parent.parent.parent.parent.meta(MD5Calculated).value, md5.hexdigest())
                    # Encrypted file
                    self.assertNotEqual(file_out.parent.parent.meta(MD5Calculated).value, md5.hexdigest())
                    # Decrypted file
                    self.assertEqual(file_out.meta(MD5Calculated).value, md5.hexdigest())

                    run_balance = True
                self.assertTrue(run_balance)

        finally:
            for path in files_in.keys():
                try:
                    os.remove(path)
                except:
                    pass
            ftp_server_thread.stop()

        self.assertTrue(run_balance)

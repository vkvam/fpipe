import hashlib
import os

from unittest import TestCase

from fpipe.gen import Meta, Program, FTP
from fpipe.file import FTPFile
from fpipe.meta import MD5, Path
from fpipe.meta.stream import Stream
from fpipe.utils.const import PIPE_BUFFER_SIZE
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
            gen = FTP().chain(
                FTPFile(
                    path,
                    host='localhost',
                    username='user',
                    password='12345',
                    block_size=PIPE_BUFFER_SIZE,
                    port=port
                ) for path in files_in.keys()
            )

            # Checksum of input
            gen = Meta(MD5).chain(
                gen
            )

            # Encrypt and decrypt
            gen = Program("gpg --batch --symmetric --passphrase 'X'").chain(
                gen
            )

            # # Checksum of encrypted
            gen = Meta(MD5).chain(
                gen
            )

            gen = Program("gpg --batch --decrypt --passphrase 'X'").chain(
                gen
            )

            # # Checksum of decrypted
            gen = Meta(MD5).chain(
                gen
            )

            for file_out in gen:
                content_out = file_out[Stream].read()
                run_balance = False

                with open(file_out[Path], 'rb') as f:
                    source_content = f.read()
                    md5 = hashlib.md5()
                    md5.update(source_content)

                    # Source and output are equal
                    self.assertEqual(content_out, source_content)

                    # Source file
                    self.assertEqual(file_out[MD5, 2], md5.hexdigest())
                    # Encrypted file
                    self.assertNotEqual(file_out[MD5, 1], md5.hexdigest())
                    # Decrypted file
                    self.assertEqual(file_out[MD5], md5.hexdigest())

                    run_balance = True
                self.assertTrue(run_balance)
        except Exception:
            raise
        finally:
            for path in files_in.keys():
                try:
                    os.remove(path)
                except:
                    pass
            ftp_server_thread.stop()

        self.assertTrue(run_balance)

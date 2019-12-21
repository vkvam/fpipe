import hashlib

from unittest import TestCase

from fpipe.gen import Program, Meta
from fpipe.meta import MD5, Path,  Size
from fpipe.exceptions import FileMetaException
from test_utils.test_file import ReversibleTestFile, TestStream


class TestMeta(TestCase):
    @staticmethod
    def __checksum(data: bytes):
        sig = hashlib.md5()
        sig.update(data)
        return sig.hexdigest()

    def test_chaining_test_stream(self):
        stream_sizes = [2 ** i for i in range(18, 22)]

        # Get expected results from FileMetaGenerators
        md5_of_files = [
            self.__checksum(ReversibleTestFile(s).read()) for s in stream_sizes
        ]

        md5_of_reversed_files = [
            self.__checksum(bytes(reversed(ReversibleTestFile(s).read()))) for s in stream_sizes
        ]

        # Get checksum for initial files
        gen = Meta(MD5, Size).chain(
            TestStream(s, f'{s}', reversible=True) for s in stream_sizes
        )

        # Reverse stdout
        gen = Program("rev").chain(gen)
        gen = Program("tr -d '\n'").chain(gen)

        # Get checksum for reversed files
        for f in Meta(MD5, Size).chain(gen):
            f.file.read(1)
            # Assert that we are not able to retrieve calculated data before files have been completely read
            with self.assertRaises(FileMetaException):
                x = f.meta(MD5).value

            with self.assertRaises(FileMetaException):
                x = f.meta(Size).value
            f.file.read()

            # Assert that checksum created in two different ways are equal
            self.assertEqual(f.parent.parent.meta(MD5).value, md5_of_files.pop(0))
            self.assertEqual(f.meta(MD5).value, md5_of_reversed_files.pop(0))
            self.assertEqual(f.meta(Path).value, str(f.meta(Size).value))
        # Assert that we've checked all files
        self.assertEqual(len(md5_of_files) + len(md5_of_reversed_files), 0)

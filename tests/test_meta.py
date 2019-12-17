import hashlib

from unittest import TestCase

from fpipe.generators.process import ProcessFileGenerator
from fpipe.meta.checksum import MD5Calculated
from fpipe.meta.path import Path
from fpipe.meta.size import SizeCalculated
from fpipe.generators.fileinfo import FileInfoGenerator, FileInfoException
from test_utils.test_file import ReversibleTestFile, TestFileGenerator, TestStream


class TestMeta(TestCase):
    @staticmethod
    def __checksum(data: bytes):
        sig = hashlib.md5()
        sig.update(data)
        return sig.hexdigest()

    def test_chaining_test_stream(self):
        stream_sizes = [2 ** i for i in range(14, 18)]

        # Get expected results from FileInfoGenerators
        md5_of_files = [
            self.__checksum(ReversibleTestFile(s).read()) for s in stream_sizes
        ]

        md5_of_reversed_files = [
            self.__checksum(bytes(reversed(ReversibleTestFile(s).read()))) for s in stream_sizes
        ]

        gen = TestFileGenerator(
            TestStream(s, f'{s}', reversible=True) for s in stream_sizes
        )

        # Get checksum for initial files
        gen = FileInfoGenerator(gen, [MD5Calculated, SizeCalculated])

        # Reverse stdout
        gen = ProcessFileGenerator(gen, "rev|tr -d '\n'")

        # Get checksum for reversed files
        gen = FileInfoGenerator(gen, [MD5Calculated, SizeCalculated])
        for f in gen:
            f.file.read(1)
            # Assert that we are not able to retrieve calculated data before files have been completely read
            with self.assertRaises(FileInfoException):
                x = f.meta(MD5Calculated).value

            with self.assertRaises(FileInfoException):
                x = f.meta(SizeCalculated).value
            f.file.read()

            # Assert that checksum created in two different ways are equal
            self.assertEqual(f.parent.parent.meta(MD5Calculated).value, md5_of_files.pop(0))
            self.assertEqual(f.meta(MD5Calculated).value, md5_of_reversed_files.pop(0))
            self.assertEqual(f.meta(Path).value, str(f.meta(SizeCalculated).value))
        # Assert that we've checked all files
        self.assertEqual(len(md5_of_files) + len(md5_of_reversed_files), 0)

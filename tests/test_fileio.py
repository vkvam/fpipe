from unittest import TestCase

from fpipe.calculators import SizeCalculated, MD5CheckSum
from fpipe.fileinfo import FileInfoGenerator, FileInfoException
from fpipe.generators import ProcessFileGenerator
from test_utils.test_file import TestStream, TestFileGenerator


class TestFileIO(TestCase):
    def test_chaining_test_stream(self):
        size = 2 ** 28

        gen = TestStream(
            size,
            'xyz'
        )

        gen = ProcessFileGenerator(
            TestFileGenerator(
                [
                    gen
                ]
            ),
            "cat /dev/stdin"
        )

        for file in FileInfoGenerator(gen, [SizeCalculated, MD5CheckSum]):

            with self.assertRaises(FileInfoException):
                x = file.meta(SizeCalculated).value

            with self.assertRaises(FileInfoException):
                x = file.meta(MD5CheckSum).value

            while file.file.read(2 ** 20):
                ...

            self.assertEqual(file.meta(SizeCalculated).value, size)
            self.assertNotEqual(file.meta(MD5CheckSum).value, '')

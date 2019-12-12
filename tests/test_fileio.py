from unittest import TestCase

from fpipe.fileinfo import FileInfoGenerator, CalculatedFileMeta, FileInfoException
from fpipe.process import ProcessFileGenerator
from test_utils.test_file import TestStream, TestFileGenerator


class TestFileIO(TestCase):
    def test_chaining_test_stream(self):
        size = 2 ** 27

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

        for file in FileInfoGenerator(gen, CalculatedFileMeta):

            with self.assertRaises(FileInfoException):
                x = file.meta.checksum_md5

            while file.file.read(2 ** 20):
                ...

            self.assertEqual(file.meta.size, size)

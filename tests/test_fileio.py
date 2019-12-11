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
            ).get_files(),
            "cat /dev/stdin"
        )

        gen = FileInfoGenerator(gen.get_files(), CalculatedFileMeta)
        for file in gen.get_files():

            with self.assertRaises(FileInfoException):
                x = file.meta.checksum_md5

            while file.file.read(2 ** 20):
                ...

            self.assertEqual(file.meta.size, size)

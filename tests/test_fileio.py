import os
from subprocess import Popen
from unittest import TestCase

from typing import Iterable, Generator

from fpipe.abstract import FileGenerator, Stream
from fpipe.fileinfo import FileInfoGenerator, ChecksumCalculator, FileInfoException
from fpipe.local import LocalFile, LocalFileGenerator, LocalFileInfoCalculator
from fpipe.process import ProcessFileGenerator


class TestFile:
    def __init__(self, size):
        self.size = size
        self.count = 0

    def read(self, n=None):
        remaining = max(self.size - self.count, 0)
        if n:
            remaining = min(remaining, n)
        self.count += remaining
        return b'x' * remaining


class TestStream(Stream):
    def __init__(self, size, path):
        super().__init__(TestFile(size), LocalFileInfoCalculator(path))


class TestFileGenerator(FileGenerator):
    def __init__(self, files: Iterable[TestStream]):
        super().__init__(files)

    def get_files(self) -> Generator[TestStream, None, None]:
        for f in self.files:
            yield f


class TestFileIO(TestCase):
    def test_chaining_test_stream(self):
        size = 2 ** 27

        test_stream = TestStream(
            size,
            'xyz'
        )

        pass_through = ProcessFileGenerator(
            TestFileGenerator(
                [
                    test_stream
                ]
            ).get_files(),
            "cat /dev/stdin"
        )

        file_info = FileInfoGenerator(pass_through.get_files(), ChecksumCalculator)
        for f in file_info.get_files():

            with self.assertRaises(FileInfoException):
                f.file_info_generator.get()

            while f.file.read(2 ** 20):
                ...

            info = f.file_info_generator.get()
            self.assertEqual(info.size, size)

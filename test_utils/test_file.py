import string
from typing import Iterable

from fpipe.file import FileStream
from fpipe.generators.abstract import FileStreamGenerator
from fpipe.meta.path import Path


class TestFile:
    def __init__(self, size):
        self.size = size
        self.count = 0

    def reset(self):
        self.count = 0
        return True

    def read(self, n=None):
        remaining = max(self.size - self.count, 0)
        if n:
            remaining = min(remaining, n)
        self.count += remaining
        return b'x' * remaining


class ReversibleTestFile(TestFile):
    def __init__(self, size):
        super().__init__(size)
        self.letters = bytearray(string.ascii_letters, encoding='utf-8')
        self.letter_count = len(self.letters)

    def seek(self, n):
        self.count = n

    def read(self, n=None):
        remaining = max(self.size - self.count, 0)
        if n:
            remaining = min(remaining, n)
        last_count = self.count
        self.count += remaining
        return bytearray(self.letters[i % self.letter_count] for i in range(last_count, last_count + remaining))


class TestStream(FileStream):
    def __init__(self, size, path, reversible=False):
        super().__init__(ReversibleTestFile(size) if reversible else TestFile(size), meta=[Path(path)])


class TestFileGenerator(FileStreamGenerator):
    def __init__(self, files: Iterable[TestStream]):
        super().__init__(files)

    def __iter__(self) -> Iterable[TestStream]:
        for f in self.files:
            yield f

import string
from types import TracebackType
from typing import Iterable, Optional, Type, AnyStr, BinaryIO

from fpipe.exceptions import SeekException
from fpipe.file import File
from fpipe.meta.path import Path


class TestFile(BinaryIO):
    def close(self) -> None:
        self.__closed = True

    def fileno(self):
        pass

    def flush(self):
        pass

    def isatty(self):
        pass

    def readable(self):
        pass

    def readline(self, limit: int = ...):
        pass

    def readlines(self, hint: int = ...):
        pass

    def seekable(self) -> bool:
        return True

    def tell(self):
        pass

    def truncate(self, size: Optional[int] = ...):
        pass

    def writable(self) -> bool:
        pass

    def write(self, s: AnyStr) -> int:
        pass

    def writelines(self, lines: Iterable[AnyStr]):
        pass

    def closed(self):
        return self.__closed

    def mode(self):
        pass

    def name(self) -> str:
        return ''

    def __next__(self):
        raise NotImplementedError

    def __iter__(self):
        raise NotImplementedError

    def __enter__(self):
        raise NotImplementedError

    def __exit__(self, t: Optional[Type[BaseException]],
                 value: Optional[BaseException],
                 traceback: Optional[TracebackType]):
        raise NotImplementedError

    def __init__(self, size):
        self.size = size
        self.offset = 0
        self.__closed = False

    def seek(self, offset=0, whence=0):
        if whence == 0:
            self.offset = offset
        elif whence == 1:
            self.offset += offset
        elif whence == 2:
            self.offset = self.size - offset
        else:
            raise SeekException()

    def read(self, n=None):
        remaining = max(self.size - self.offset, 0)
        if n:
            remaining = min(remaining, n)
        self.offset += remaining
        return b'x' * remaining


class ReversibleTestFile(TestFile):

    def __next__(self):
        pass

    def __iter__(self):
        pass

    def __enter__(self):
        pass

    def __exit__(self, t: Optional[Type[BaseException]],
                 value: Optional[BaseException],
                 traceback: Optional[TracebackType]):
        pass

    def __init__(self, size):
        super().__init__(size)
        self.letters = bytearray(string.ascii_letters, encoding='utf-8')
        self.letter_count = len(self.letters)

    def read(self, n=None):
        remaining = max(self.size - self.offset, 0)
        if n:
            remaining = min(remaining, n)
        last_count = self.offset
        self.offset += remaining
        return bytearray(self.letters[i % self.letter_count] for i in
                         range(last_count, last_count + remaining))


class TestStream(File):
    def __init__(self, size, path, reversible=False):
        if reversible:
            super().__init__(stream=ReversibleTestFile(size), meta=[Path(path)])
        else:
            super().__init__(stream=TestFile(size), meta=[Path(path)])

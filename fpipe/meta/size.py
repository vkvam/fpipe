from fpipe.exceptions import FileInfoException
from fpipe.meta.abstract import FileMetaValue
from fpipe.meta.fileinfo import FileMetaCalculator


class Size(FileMetaValue[int]):
    pass


class SizeCalculated(FileMetaCalculator[int]):
    def __init__(self):
        self.done = False
        self.v = 0

    def write(self, b: bytes):
        self.v += len(b)
        if b == b'':
            self.done = True

    @property
    def value(self) -> int:
        if self.done:
            return self.v
        else:
            raise FileInfoException("?!")  # TODO: Fix

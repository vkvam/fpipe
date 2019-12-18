import hashlib

from fpipe.exceptions import FileInfoException
from fpipe.meta.fileinfo import FileMetaCalculator


class MD5Calculated(FileMetaCalculator[str]):
    def __init__(self):
        self.__sig = hashlib.md5()
        self.done = False
        self.v: str = ''

    def write(self, b: bytes):
        self.__sig.update(b)
        if b == b'':
            self.v = self.__sig.hexdigest()
            self.done = True

    @property
    def value(self) -> str:
        if self.done:
            return self.v
        else:
            raise FileInfoException("?!")  # TODO: Fix

import hashlib

from fpipe.generators.fileinfo import FileInfoException, FileMetaCalculator

from fpipe.file.filemeta import MetaStr


class MD5CheckSum(MetaStr, FileMetaCalculator):
    def __init__(self):
        super().__init__('')
        self.__sig = hashlib.md5()
        self.done = False

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

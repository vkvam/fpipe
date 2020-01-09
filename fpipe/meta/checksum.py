import hashlib
from typing import Union

from fpipe.meta.abstract import FileMeta, FileMetaCalculator


class MD5Calculator(FileMetaCalculator[str]):
    def __init__(self):
        super().__init__(MD5)
        self.__md5 = hashlib.md5()

    def write(self, s: Union[bytes, bytearray]):
        self.__md5.update(s)
        if not s:
            self.calculable.value = self.__md5.hexdigest()


class MD5(FileMeta[str]):
    @staticmethod
    def get_calculator() -> FileMetaCalculator[str]:
        return MD5Calculator()

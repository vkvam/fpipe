import hashlib
from typing import Union

from fpipe.meta.abstract import FileMetaFuture, FileMetaCalculator


class MD5Calculator(FileMetaCalculator[str]):
    def __init__(self, calculable: "MD5"):
        super().__init__(calculable)
        self.__sig = hashlib.md5()

    def write(self, s: Union[bytes, bytearray]):
        self.__sig.update(s)
        if not s:
            self.calculable.set_value(self.__sig.hexdigest())


class MD5(FileMetaFuture[str]):
    @staticmethod
    def get_calculator() -> FileMetaCalculator[str]:
        return MD5Calculator(MD5())

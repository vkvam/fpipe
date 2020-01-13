import hashlib
from typing import Union

from fpipe.meta.abstract import FileData, FileDataCalculator


class MD5Calculator(FileDataCalculator[str]):
    def __init__(self):
        super().__init__(MD5)
        self.__md5 = hashlib.md5()

    def write(self, s: Union[bytes, bytearray]):
        self.__md5.update(s)
        if not s:
            self.calculable.value = self.__md5.hexdigest()


class MD5(FileData[str]):
    @staticmethod
    def get_calculator() -> FileDataCalculator[str]:
        return MD5Calculator()

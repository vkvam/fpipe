from typing import Union

from fpipe.meta.abstract import FileData, FileDataCalculator, T


class SizeCalculator(FileDataCalculator):
    def __init__(self):
        super().__init__(Size)
        self.__byte_count = 0

    def write(self, s: Union[bytes, bytearray]):
        self.__byte_count += len(s)
        if not s:
            self.calculable.value = self.__byte_count


class Size(FileData[int]):
    @staticmethod
    def get_calculator() -> FileDataCalculator[T]:
        return SizeCalculator()

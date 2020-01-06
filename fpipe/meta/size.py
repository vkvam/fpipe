from typing import Union

from fpipe.meta.abstract import FileMetaFuture, FileMetaCalculator, T


class SizeCalculator(FileMetaCalculator):
    def __init__(self, calculable: "Size"):
        super().__init__(calculable)
        self.v = 0

    def write(self, s: Union[bytes, bytearray]):
        self.v += len(s)
        if not s:
            self.calculable.set_value(self.v)


class Size(FileMetaFuture[int]):
    @staticmethod
    def get_calculator() -> FileMetaCalculator[T]:
        return SizeCalculator(Size())

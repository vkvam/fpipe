from fpipe.meta.abstract import FileMetaFuture, FileMetaCalculator, T


class SizeCalculator(FileMetaCalculator):
    def __init__(self, calculable: "Size"):
        super().__init__(calculable)
        self.v = 0

    def write(self, b: bytes):
        self.v += len(b)
        if not b:
            self.calculable.set_value(self.v)


class Size(FileMetaFuture[int]):
    @staticmethod
    def get_calculator() -> FileMetaCalculator[T]:
        return SizeCalculator(Size())

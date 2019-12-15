from fpipe.generators.fileinfo import FileInfoException, FileMetaCalculator

from fpipe.file.filemeta import MetaInt


# TODO:  Move to somewhere more general
class Size(MetaInt):
    pass


class SizeCalculated(Size, FileMetaCalculator):
    def __init__(self):
        super().__init__(0)
        self.done = False

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

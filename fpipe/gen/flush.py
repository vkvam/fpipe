from fpipe.file.file import FileStream
from fpipe.gen.callable import MethodGen


class Flush(MethodGen):
    """
    Generator that empties files from generators, then yields them.
    """

    def executor(self, source: FileStream):
        read = source.file.read
        size = 2 ** 14
        while read(size):
            pass

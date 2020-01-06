from fpipe.file.file import FileStream
from fpipe.gen.callable import MethodGen
from fpipe.utils.const import PIPE_BUFFER_SIZE


class Flush(MethodGen):
    """
    Generator that empties files from generators, then yields them.
    """

    def executor(self, source: FileStream):
        read = source.file.read
        size = PIPE_BUFFER_SIZE
        while read(size):
            pass

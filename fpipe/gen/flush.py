from fpipe.file.file import File
from fpipe.gen.generator import FileGenerator
from fpipe.utils.const import PIPE_BUFFER_SIZE


class Flush(FileGenerator):
    """
    Generator that empties files from generators, then yields them.
    """

    def process(self, source: File,
                generated_meta_container: File):
        if source.file:
            read = source.file.read
            size = PIPE_BUFFER_SIZE
            while read(size):
                pass

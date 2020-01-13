from fpipe.exceptions import FileDataException
from fpipe.file.file import File
from fpipe.gen.generator import FileGenerator
from fpipe.meta.stream import Stream
from fpipe.utils.const import PIPE_BUFFER_SIZE


class Flush(FileGenerator):
    """
    Generator that empties files from generators, then yields them.
    """

    def process(self, source: File,
                generated_meta_container: File):
        try:
            stream_read = source[Stream].read
            size = PIPE_BUFFER_SIZE
            while stream_read(size):
                pass
        except FileDataException:
            pass

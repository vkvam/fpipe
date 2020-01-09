import threading
from typing import Type, Optional, Generator, Tuple

from fpipe.file.file import FileStream, File
from fpipe.gen.generator import SOURCE, FileGenerator, FileGeneratorResponse
from fpipe.meta.abstract import FileMeta
from fpipe.utils.bytesloop import BytesLoop
from fpipe.utils.const import PIPE_BUFFER_SIZE


class Meta(FileGenerator[FileStream, FileStream]):
    """Generator producing FileMeta by doing calculations on a FileStream
    """
    def __init__(self, *file_meta: Type[FileMeta]):
        """

        :param file_meta: a FileMeta with a link to a FileMetaCalculator
        through FileMeta.get_calculator()
        """
        super().__init__()
        self.file_meta: Tuple[Type[FileMeta], ...] = file_meta
        self.bufsize = PIPE_BUFFER_SIZE

    def process(
            self,
            source: SOURCE,
            process_meta: File) -> Optional[
        Generator[
            FileGeneratorResponse,
            None,
            None
        ]
    ]:
        buf_size = self.bufsize

        with BytesLoop(self.bufsize) as byte_loop:
            mata_calculators = [
                f.get_calculator() for f in self.file_meta
            ]

            meta_calculators_write = [
                f.write for f in mata_calculators if f
            ]

            def write_to_meta_calculators():
                while True:
                    s = source.file.read(buf_size)
                    for write in meta_calculators_write:
                        write(s)
                    byte_loop.write(s)
                    if not s:
                        break

            proc_thread = threading.Thread(
                target=write_to_meta_calculators,
                name=f"{self.__class__.__name__}",
                daemon=True,
            )

            yield FileGeneratorResponse(
                FileStream(
                    byte_loop,
                    parent=source,
                    meta=(
                        c.calculable for c in mata_calculators if c
                    ),
                ),
                proc_thread
            )

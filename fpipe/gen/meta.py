import threading
from typing import Type, Iterator

from fpipe.file.file import FileStream
from fpipe.gen.abstract import FileGenerator
from fpipe.meta.abstract import FileMetaFuture
from fpipe.utils.bytesloop import BytesLoop
from fpipe.utils.const import PIPE_BUFFER_SIZE


class Meta(FileGenerator):
    def __init__(self, *file_meta: Type[FileMetaFuture]):
        super().__init__()
        self.file_meta = file_meta
        self.bufsize = PIPE_BUFFER_SIZE

    def __iter__(self) -> Iterator[FileStream]:
        for source in self.source_files:
            buf_size = self.bufsize

            with BytesLoop(self.bufsize) as byte_loop:
                calculators = [f.get_calculator() for f in self.file_meta if f]
                calc_calls = [f.write for f in calculators if f]

                def __process():

                    while True:
                        b = source.file.read(buf_size)
                        for c in calc_calls:
                            c(b)
                        byte_loop.write(b)
                        if not b:
                            break

                # TODO: Pass all threads to the end, and start/join them
                # all in another thread that tries to join all threads in
                # sequence. Need to fix so that file read/write can be aborted
                # if any of the threads fail.
                proc_thread = threading.Thread(
                    target=__process,
                    name=f"{self.__class__.__name__}",
                    daemon=True,
                )
                proc_thread.start()
                yield FileStream(
                    byte_loop,
                    parent=source,
                    meta=[c.calculable for c in calculators if c],
                )
                proc_thread.join()

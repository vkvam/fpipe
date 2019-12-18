import threading
from typing import Iterable, Type

from fpipe.file.file import FileStream
from fpipe.gen.abstract import FileStreamGenerator
from fpipe.meta.fileinfo import FileMetaCalculator
from fpipe.utils.bytesloop import BytesLoop


class FileInfoGenerator(FileStreamGenerator):
    def __init__(self, file_meta_calculators: Iterable[Type[FileMetaCalculator]]):
        super().__init__()
        self.file_meta_calculators = file_meta_calculators
        self.bufsize = 2 ** 14

    def __iter__(self) -> Iterable[FileStream]:
        for source in self.files:
            byte_loop = BytesLoop()
            buf_size = self.bufsize
            calculators = [f() for f in self.file_meta_calculators]
            calc_calls = [f.write for f in calculators]

            def __process():
                while True:
                    b = source.file.read(buf_size)
                    if calc_calls:
                        for c in calc_calls:
                            c(b)

                    byte_loop.write(b)
                    if not b:  # EOF
                        break
            # TODO: Pass all threads to the end, and start/join them
            # all in another thread that tries to join all threads in sequence.
            # Need to fix so that file read/write can be aborted if any of the threads fail.
            proc_thread = threading.Thread(target=__process, name=f'{self.__class__.__name__}', daemon=True)
            proc_thread.start()
            yield FileStream(byte_loop, parent=source, meta=calculators)
            proc_thread.join()

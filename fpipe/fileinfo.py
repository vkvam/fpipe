import threading
from abc import abstractmethod
from typing import Iterable, List

from fpipe import FileMeta
from .utils import BytesLoop
from .file import FileStream, File, FileStreamGenerator


class FileMetaCalculator(FileMeta):
    def __init__(self):
        pass

    @abstractmethod
    def write(self, b: bytes):
        pass


class FileInfoException(Exception):
    pass


class FileInfoGenerator(FileStreamGenerator):
    def __init__(self, files: Iterable[File], file_meta_calculators: List[type(FileMetaCalculator)]):
        super().__init__(files)
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
                    for c in calc_calls:
                        c(b)

                    byte_loop.write(b)
                    if not b:  # EOF
                        break

            proc_thread = threading.Thread(target=__process, name=f'{self.__class__.__name__}', daemon=True)
            proc_thread.start()

            yield FileStream(byte_loop, parent=source, meta=calculators)
            proc_thread.join()

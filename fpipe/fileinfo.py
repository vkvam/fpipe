import threading
from typing import Generator, Iterable

from .utils import BytesLoop, Stats
from .abstract import FileGenerator, Stream, File, FileInfoCalculator, FileInfo
import hashlib


class FileInfoException(Exception):
    pass


class ChecksumCalculator(FileInfoCalculator):
    def __init__(self, source_calculator: 'FileInfoCalculator'):
        super().__init__(source_calculator)
        self.size = 0
        self.sig = hashlib.md5()
        self.all_bytes_processed = False

    def write(self, b: bytes):
        self.size += len(b)
        self.sig.update(b)
        if not b:
            self.all_bytes_processed = True

    def get(self) -> FileInfo:
        if not self.all_bytes_processed:
            raise FileInfoException("Can not return checksum or size before file has been completely read")
        source_file_info = self.source_calculator.get()
        source_file_info.size = self.size
        source_file_info.checksum_md5 = self.sig.hexdigest()
        return source_file_info


class FileInfoGenerator(FileGenerator):
    def __init__(self, files: Iterable[File], calculator: type(FileInfoCalculator)):
        super().__init__(files)
        self.calculator = calculator
        self.bufsize = 2 ** 14

    def get_files(self) -> Generator[Stream, None, None]:
        for source in self.files:
            byte_loop = BytesLoop()
            buf_size = self.bufsize
            stats = Stats(self.__class__.__name__)

            calculator = self.calculator(source.file_info_generator)

            def __process():
                calc = calculator.write
                while True:
                    b = source.file.read(buf_size)
                    stats.r(b)
                    calc(b)
                    byte_loop.write(b)
                    if not b:  # EOF
                        break

            proc_thread = threading.Thread(target=__process)
            proc_thread.start()

            yield Stream(byte_loop, calculator)
            proc_thread.join()

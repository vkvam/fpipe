import threading
from typing import Generator, Iterable, cast

from .utils import BytesLoop, Stats
from .abstract import Stream, File, FileMetaCalculated, FileMeta, FileStreamGenerator
import hashlib


class FileInfoException(Exception):
    pass


class CalculatedFileMeta(FileMeta):
    def __init__(self):
        self._size = None
        self._size_count = 0
        self.__sig = hashlib.md5()
        self.__all_bytes_processed = False
        self.__exception = None

        self._checksum_md5 = None

    def _write(self, b: bytes):
        self._size_count += len(b)
        self.__sig.update(b)
        if not b:
            self._size = self._size_count
            self._checksum_md5 = self.__sig.hexdigest()

    @property
    def checksum_md5(self):
        if not self._checksum_md5:
            raise FileInfoException("Can not return checksum before file has been completely read")
        return self._checksum_md5

    @property
    def size(self):
        if not self._size:
            raise FileInfoException("Can not return size before file has been completely read")
        return self._size


class FileInfoStream(Stream):
    @property
    def meta(self) -> CalculatedFileMeta:
        return cast(CalculatedFileMeta, super().meta)


class FileInfoGenerator(FileStreamGenerator):
    def __init__(self, files: Iterable[File], calculator: type(FileMetaCalculated)):
        super().__init__(files)
        self.calculator = calculator
        self.bufsize = 2 ** 14

    def __iter__(self) -> Iterable[FileInfoStream]:
        for source in self.files:
            byte_loop = BytesLoop()
            buf_size = self.bufsize
            # stats = Stats(self.__class__.__name__)

            calculator = self.calculator()

            def __process():
                calc = calculator._write
                while True:
                    b = source.file.read(buf_size)
                    # stats.r(b)
                    calc(b)

                    byte_loop.write(b)
                    if not b:  # EOF
                        break

            proc_thread = threading.Thread(target=__process, name=f'{self.__class__.__name__}', daemon=True)
            proc_thread.start()

            yield Stream(byte_loop, calculator, parent=source)
            proc_thread.join()

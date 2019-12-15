import threading
from time import sleep
from types import TracebackType
from typing import BinaryIO, Optional, Type, Iterator, AnyStr, Iterable, List


class BytesLoop(BinaryIO):
    def __init__(self, buf_size=2 ** 14, lock_wait=0.00001):
        self.buffer = bytearray()
        self.done = False
        self.lock = threading.Lock()
        self.buf_size = buf_size
        self.lock_wait = lock_wait
        # self.stats = Stats(self.__class__.__name__)

    def reset(self):
        self.done = False
        self.buffer.clear()

    def __r(self, n=None):
        self.lock.acquire()
        chunk = self.buffer[:n]
        while not chunk and not self.done:
            self.lock.release()
            sleep(self.lock_wait)  # Allow writes
            self.lock.acquire()
            chunk = self.buffer[:n]
        del self.buffer[:len(chunk)]
        self.lock.release()

        return chunk

    def read(self, n=None) -> bytes:
        chunk = self.__r(n)
        if n is None:
            ret = chunk
            while chunk:
                chunk = self.__r(n)
                ret += chunk
            return ret
        return chunk

    def write(self, data: bytes):
        data_len = len(data)
        if not data_len:
            self.done = True  # EOF
            return

        while True:
            self.lock.acquire()
            remaining_buffer = self.buf_size - len(self.buffer)
            chunk_length = min(remaining_buffer, data_len)

            if chunk_length == data_len:
                self.buffer += data
                self.lock.release()
                break
            else:
                if not isinstance(data, bytearray):
                    data = bytearray(data)
                chunk = data[:remaining_buffer]
                del data[:chunk_length]
                data_len -= chunk_length
                self.buffer += chunk
                self.lock.release()
                sleep(self.lock_wait)

    def __enter__(self) -> BinaryIO:
        raise NotImplementedError()

    def close(self) -> None:
        raise NotImplementedError()

    def fileno(self) -> int:
        raise NotImplementedError()

    def flush(self) -> None:
        raise NotImplementedError()

    def isatty(self) -> bool:
        return False

    def readable(self) -> bool:
        return True

    def readline(self, limit: int = ...) -> AnyStr:
        raise NotImplementedError()

    def readlines(self, hint: int = ...) -> List[AnyStr]:
        raise NotImplementedError()

    def seek(self, offset: int, whence: int = ...) -> int:
        raise NotImplementedError()

    def seekable(self) -> bool:
        return False

    def tell(self) -> int:
        raise NotImplementedError()

    def truncate(self, size: Optional[int] = ...) -> int:
        raise NotImplementedError()

    def writable(self) -> bool:
        raise NotImplementedError()

    def writelines(self, lines: Iterable[AnyStr]) -> None:
        raise NotImplementedError()

    def __next__(self) -> AnyStr:
        raise NotImplementedError()

    def __iter__(self) -> Iterator[AnyStr]:
        raise NotImplementedError()

    def __exit__(self, t: Optional[Type[BaseException]], value: Optional[BaseException],
                 traceback: Optional[TracebackType]) -> bool:
        raise NotImplementedError()

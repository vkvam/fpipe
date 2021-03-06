from threading import Event
from typing import Optional, Type, Iterator, AnyStr, Iterable, List, \
    BinaryIO, Union

from fpipe.utils.const import PIPE_BUFFER_SIZE


class BytesLoop(BinaryIO):
    def __init__(self,
                 buf_size: int = PIPE_BUFFER_SIZE):
        self.__buffer = bytearray()
        self.__buf_size: int = buf_size
        self.__done = False
        self.__closed = False
        self.__bytes_written = 0
        self.__bytes_read = 0

        self.read_ready = Event()
        self.write_ready = Event()
        self.write_ready.set()

    def __read_chunk(self, n=-1) -> bytes:
        chunk = b''
        while not self.__done or self.__bytes_written > self.__bytes_read:

            self.read_ready.wait()

            chunk = self.__buffer[:n] if n > 0 else self.__buffer[:]
            try:
                if chunk:
                    chunk_len = len(chunk)
                    del self.__buffer[:chunk_len]
                    self.__bytes_read += chunk_len
                    return chunk
            finally:
                self.read_ready.clear()
                self.write_ready.set()
        return chunk

    def read(self, n=-1) -> bytes:
        chunk = self.__read_chunk(n)
        if n > 0:
            return chunk

        ret = chunk
        while chunk:
            chunk = self.__read_chunk(n)
            ret += chunk
        return ret

    def write(self, s: Union[bytes, bytearray]) -> int:
        data_len = len(s)

        if not isinstance(s, bytearray):
            s = bytearray(s)

        self.__bytes_written += data_len
        if not data_len:
            self.__done = True  # EOF
            self.write_ready.clear()
            self.read_ready.set()
            return 0

        while True:
            self.write_ready.wait()

            remaining_buffer = self.__buf_size - len(self.__buffer)
            chunk_length = min(remaining_buffer, data_len)

            if chunk_length == data_len:
                self.__buffer += s
                break
            else:
                chunk = s[:remaining_buffer]
                del s[:chunk_length]
                data_len -= chunk_length
                self.__buffer += chunk

            self.write_ready.clear()
            self.read_ready.set()
        return data_len

    def __enter__(self) -> BinaryIO:
        return self

    def close(self) -> None:
        self.__buffer.clear()
        self.__bytes_read = 0
        self.__bytes_written = 0
        self.__closed = True

    def fileno(self) -> int:
        raise NotImplementedError

    def flush(self) -> None:
        while self.read(self.__buf_size):
            pass

    def isatty(self) -> bool:
        return False

    def readable(self) -> bool:
        return True

    def writable(self) -> bool:
        return True

    def readline(self, limit: int = ...) -> AnyStr:
        raise NotImplementedError

    def readlines(self, hint: int = ...) -> List[AnyStr]:
        raise NotImplementedError

    def seek(self, offset: int, whence: int = ...) -> int:
        raise NotImplementedError

    def seekable(self) -> bool:
        return False

    def tell(self) -> int:
        raise NotImplementedError

    def truncate(self, size: Optional[int] = ...) -> int:
        raise NotImplementedError

    def writelines(self, lines: Iterable[AnyStr]) -> None:
        raise NotImplementedError

    @property
    def closed(self):
        return self.__closed

    def mode(self):
        raise NotImplementedError

    def name(self) -> str:
        raise NotImplementedError

    def __next__(self) -> AnyStr:
        raise NotImplementedError

    def __iter__(self) -> Iterator[AnyStr]:
        raise NotImplementedError

    def __exit__(
            self,
            t: Optional[Type[BaseException]],
            value: Optional[BaseException],
            traceback=None,
    ) -> bool:
        self.close()
        return t is None

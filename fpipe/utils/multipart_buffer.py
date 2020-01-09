from typing import Tuple, Union


class MultipartBuffer(object):
    def __init__(self, chunk_size: int, str_encoding: str = "utf-8"):
        self.buffer: bytearray = bytearray()
        self.part_number: int = 1
        self.chunk_size: int = chunk_size
        self.count: int = 0
        self.str_encoding = str_encoding

    def add(self, s: Union[bytes, bytearray]):
        self.buffer += s
        self.count += len(s)

    def empty(self) -> bool:
        return self.count == 0

    def full(self) -> bool:
        return self.count >= self.chunk_size

    def get(self) -> Tuple[bytearray, int]:
        try:
            return self.buffer[: self.chunk_size], self.part_number
        finally:
            del self.buffer[: self.chunk_size]
            self.count -= max(0, self.chunk_size)
            self.part_number += 1

    def clear(self):
        del self.buffer[:]
        self.part_number = 1
        self.count = 0

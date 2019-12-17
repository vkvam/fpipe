from typing import Tuple


class Buffer(object):
    def __init__(self, chunk_size, str_encoding='utf-8'):
        self.buffer = bytearray()
        self.part_number = 1
        self.chunk_size = chunk_size
        self.str_encoding = str_encoding

        self.count = 0

    def add(self, data):
        data_count = len(data)
        if isinstance(data, bytes) or isinstance(data, bytearray):
            self.buffer += data
        elif isinstance(data, str):
            self.buffer += data.encode(self.str_encoding)
        else:
            raise TypeError(f"{type(data)} can not be stored in S3 object")
        self.count += data_count

    def empty(self) -> bool:
        return self.count == 0

    def full(self) -> bool:
        return self.count >= self.chunk_size

    def get(self) -> Tuple[bytearray, int]:
        try:
            return self.buffer[:self.chunk_size], self.part_number
        finally:
            del self.buffer[:self.chunk_size]
            self.count -= max(0, self.chunk_size)
            self.part_number += 1

    def clear(self):
        del self.buffer[:]
        self.part_number = 1
        self.count = 0

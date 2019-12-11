import threading
from time import sleep


class BytesLoop:
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

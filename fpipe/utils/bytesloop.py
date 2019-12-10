import threading
from time import sleep
from .stats import Stats


class BytesLoop:
    def __init__(self, bufsize=2 ** 14):
        self.buffer = bytearray()
        self.done = False
        self.lock = threading.Lock()
        self.bufsize = bufsize
        self.stats = Stats(self.__class__.__name__)

        self.write_limit = 1
        self.writes = 0

    def __r(self, n=None):
        self.lock.acquire()
        chunk = self.buffer[:n]
        while not chunk and not self.done:
            self.lock.release()
            sleep(0.00001)  # Allow writes
            self.lock.acquire()
            chunk = self.buffer[:n]

        del self.buffer[:len(chunk)]
        self.lock.release()

        self.stats.r(chunk)
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

    def write(self, b: bytes):
        while len(self.buffer) >= self.bufsize:
            sleep(0.00000001)
        self.lock.acquire()
        self.buffer += b
        self.lock.release()
        self.stats.w(b)
        if b == b'':
            self.done = True

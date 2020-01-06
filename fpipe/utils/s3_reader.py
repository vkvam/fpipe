import math
import threading
from typing import (
    Optional,
    List,
    Tuple,
    BinaryIO,
    Iterator,
    AnyStr,
    Iterable,
    Type,
)

from fpipe.exceptions import SeekException, FileException


class S3FileReader(BinaryIO):
    def __init__(
            self,
            s3_client,
            s3_resource,
            bucket,
            key,
            cache_size=5 * 2 ** 20,
            cache_chunk_count_limit=4,
            lock: Optional[threading.Lock] = None,
            meta_lock: Optional[threading.Lock] = None,
            version: Optional[str] = None,
            seekable: bool = True,
    ):
        self.s3_client = s3_client
        self.s3_resource = s3_resource

        self.bucket = bucket
        self.key = key
        self.version = version

        self.cache_chunk_size = cache_size
        self.cache_chunk_count_limit = cache_chunk_count_limit

        self.bytes_received = 0
        self.chunk_lookups = 0
        self._size = 0

        # Cache blocks that each contain a byte-range of the object limited
        # in size by by cache_chunk_size.
        self.cache_chunks: List[Tuple[int, bytes]] = []
        self.last_chunk = None
        self.offset = 0
        self.read_lock = lock
        self.meta_lock = meta_lock
        self.__seekable: bool = seekable
        self.obj_body = None
        self.locked = self.meta_lock or self.read_lock
        self.__closed = False

        if self.meta_lock:
            self.meta_lock.acquire()

        if self.read_lock:
            self.read_lock.acquire()
        else:
            self.__initialize()

    def __enter__(self) -> "S3FileReader":
        return self

    def __exit__(
            self,
            t: Optional[Type[BaseException]],
            value: Optional[BaseException],
            traceback=None,
    ) -> bool:
        self.close()
        return t is None

    def __initialize(self):
        if self.version:
            versions = self.s3_resource.Bucket(
                self.bucket
            ).object_versions.filter(Prefix=self.key)
            for version in versions:
                this_ver = version.get().get("VersionId")
                if this_ver == self.version:
                    self._size = version.size
                    return
        else:
            for obj in self.s3_resource.Bucket(self.bucket).objects.filter(
                    Prefix=self.key
            ):
                if obj.key == self.key:
                    self._size = obj.size
                    return
        raise FileException(
            f"Could not locate S3 object s3://{self.bucket}/{self.key}"
        )

    def size(self):
        return self._size

    def tell(self):
        return self.offset

    def seek(self, offset, whence=0):
        if self.locked:
            self._unlock()
        if not self.__seekable:
            raise SeekException("S3 seek not enabled")
        if whence == 0:
            self.offset = offset
        elif whence == 1:
            self.offset += offset
        elif whence == 2:
            self.offset = self._size - offset
        else:
            raise SeekException(f"Invalid whence {whence}, should be 0,1 or 2")

    def _unlock(self):
        # Mechanism to wait until object is available
        if self.read_lock and self.read_lock.locked():
            self.read_lock.acquire()
            self.__initialize()
            self.read_lock = None

        # Once the read lock is released we can release metadata
        if self.meta_lock:
            self.meta_lock.release()
            self.meta_lock = None
        self.locked = False

    def read(self, n=-1):
        if self.locked:
            self._unlock()

        if not self.__seekable:
            self.obj_body = (
                self.obj_body
                or self.s3_client.get_object(
                    Bucket=self.bucket,
                    Key=self.key,
                    **({"VersionId": self.version} if self.version else {}),
                )["Body"]
            )
            return self.obj_body.read(n)

        end = self._size
        if n > 0:
            end = min(end, self.offset + n)

        data = b''
        while self.offset < end:
            if self.last_chunk is None:
                self.last_chunk = self._get_chunk_for_offset()
            chunk_start, chunk_bytes = self.last_chunk

            while chunk_start is not None:
                while (
                        chunk_start
                        <= self.offset
                        < chunk_start + self.cache_chunk_size
                        and self.offset < end
                ):
                    chunk_offset = self.offset - chunk_start
                    self.offset = min(end, chunk_start + self.cache_chunk_size)
                    if data:
                        data += chunk_bytes[chunk_offset:end - chunk_start]
                    else:
                        data = chunk_bytes[chunk_offset:end - chunk_start]

                if self.offset < end:
                    self.last_chunk = (
                        chunk_start,
                        chunk_bytes,
                    ) = self._get_chunk_for_offset()
                else:
                    break

            if self.offset < end:
                self.last_chunk = self._append_cache_chunk()
        return data

    def _get_chunk_for_offset(self):
        self.chunk_lookups += 1
        chunk_index = int(
            math.floor(self.offset / self.cache_chunk_size)
            * self.cache_chunk_size
        )
        return next(
            (
                (chunk_start, chunk_bytes)
                for chunk_start, chunk_bytes in self.cache_chunks
                if chunk_start == chunk_index
            ),
            (None, None),
        )

    def _append_cache_chunk(self):

        chunk_start = int(
            math.floor(self.offset / self.cache_chunk_size)
            * self.cache_chunk_size
        )
        chunk_end = min(chunk_start + self.cache_chunk_size, self.size()) - 1

        response = self.s3_client.get_object(
            Bucket=self.bucket,
            Key=self.key,
            Range="bytes={0}-{1}".format(str(chunk_start), str(chunk_end)),
            **({"VersionId": self.version} if self.version else {}),
        )

        if len(self.cache_chunks) >= self.cache_chunk_count_limit:
            self.cache_chunks.pop(0)

        chunk = (chunk_start, response["Body"].read())
        self.cache_chunks.append(chunk)

        self.bytes_received += chunk_end - chunk_start + 1
        return chunk

    def close(self) -> None:
        if self.obj_body:
            self.obj_body.close()

        self.cache_chunks.clear()
        self.__closed = True

    def fileno(self) -> int:
        raise NotImplementedError

    def flush(self) -> None:
        while self.read(2 ** 20):
            pass

    def isatty(self) -> bool:
        raise NotImplementedError

    def readable(self) -> bool:
        return True

    def readline(self, limit: int = ...) -> AnyStr:
        raise NotImplementedError

    def readlines(self, hint: int = ...) -> List[AnyStr]:
        raise NotImplementedError

    def seekable(self) -> bool:
        return self.__seekable

    def truncate(self, size: Optional[int] = ...) -> int:
        raise NotImplementedError

    def writable(self) -> bool:
        return False

    def write(self, s: AnyStr) -> int:
        raise NotImplementedError

    def closed(self):
        return self.__closed

    def mode(self):
        raise NotImplementedError

    def name(self) -> str:
        raise NotImplementedError

    def writelines(self, lines: Iterable[AnyStr]) -> None:
        raise NotImplementedError

    def __next__(self) -> AnyStr:
        raise NotImplementedError

    def __iter__(self) -> Iterator[AnyStr]:
        raise NotImplementedError

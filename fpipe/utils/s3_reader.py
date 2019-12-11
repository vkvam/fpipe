#!/usr/bin/env python3

import math
from typing import Optional

import threading


class S3FileReader(object):
    def __init__(self,
                 s3_client,
                 s3_resource,
                 bucket,
                 key,
                 cache_size=2 ** 21,
                 cache_chunk_count_limit=16,
                 lock: Optional[threading.Lock] = None,
                 version: Optional[str] = None):
        self.s3_client = s3_client
        self.s3_resource = s3_resource

        self.bucket = bucket
        self.key = key
        self.version = version

        self.cache_chunk_size = cache_size
        self.cache_chunk_count_limit = cache_chunk_count_limit

        self.bytes_recieved = 0
        self.chunk_lookups = 0
        self._size = 0

        # Cache blocks that each contain a byte-range of the object limited in size by by cache_chunk_size.
        self.cache_chunks = []
        self.last_chunk = None
        self.offset = 0
        self.lock = lock

        if self.lock:
            self.lock.acquire()
        else:
            self.__initialize()

    def __enter__(self):
        return self

    def __exit__(self, *args, **xargs):
        return True

    def __initialize(self):
        if self.version:
            versions = self.s3_resource.Bucket(self.bucket).object_versions.filter(Prefix=self.key)
            for version in versions:
                this_ver = version.get().get('VersionId')
                if this_ver == self.version:
                    self._size = version.size
                    break
        else:
            for obj in self.s3_resource.Bucket(self.bucket).objects.filter(Prefix=self.key):
                if obj.key == self.key:
                    self._size = obj.size

        if self._size == 0:
            raise Exception("Size is 0")

    def size(self):
        return self._size

    def tell(self):
        return self.offset

    def seek(self, offset, whence=0):
        if whence == 0:
            self.offset = offset
        elif whence == 1:
            self.offset += offset
        elif whence == 2:
            self.offset = self.size() + offset
        else:
            raise Exception("Invalid whence")

    def read(self, count=-1):
        if self.lock and self.lock.locked():
            # Wait until object has been written
            self.lock.acquire()
            self.__initialize()
            self.lock = None

        end = self.size()
        if count > 0:
            end = min(end, self.offset + count)

        data = bytearray()
        while self.offset < end:
            if self.last_chunk is None:
                self.last_chunk = self._get_chunk_for_offset()

            chunk_start, chunk_bytes = self.last_chunk

            while chunk_start is not None:
                while chunk_start <= self.offset < chunk_start + self.cache_chunk_size and self.offset < end:
                    chunk_offset = self.offset - chunk_start
                    self.offset = min(end, chunk_start + self.cache_chunk_size)
                    data += chunk_bytes[chunk_offset:end - chunk_start]

                if self.offset < end:
                    self.last_chunk = chunk_start, chunk_bytes = self._get_chunk_for_offset()
                else:
                    break

            if self.offset < end:
                self.last_chunk = self._append_cache_chunk()

        return data

    def _get_chunk_for_offset(self):
        self.chunk_lookups += 1
        chunk_index = self._get_chunk_index()
        for chunk_start, chunk_bytes in self.cache_chunks:
            if chunk_start == chunk_index:
                return chunk_start, chunk_bytes
        return None, None

    def _get_chunk_index(self):
        return int(math.floor(self.offset / self.cache_chunk_size) * self.cache_chunk_size)

    def _append_cache_chunk(self):

        chunk_start = int(math.floor(self.offset / self.cache_chunk_size) * self.cache_chunk_size)
        chunk_end = min(chunk_start + self.cache_chunk_size, self.size()) - 1

        response = self.s3_client.get_object(
            Bucket=self.bucket,
            Key=self.key,
            Range='bytes={0}-{1}'.format(str(chunk_start), str(chunk_end)),
            **{
                'VersionId': self.version
            } if self.version else {}
        )

        if len(self.cache_chunks) >= self.cache_chunk_count_limit:
            self.cache_chunks.pop(0)

        chunk = (chunk_start, response['Body'].read())
        self.cache_chunks.append(chunk)

        self.bytes_recieved += chunk_end - chunk_start + 1
        return chunk

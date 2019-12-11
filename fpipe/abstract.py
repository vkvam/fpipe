from abc import abstractmethod

from typing import Iterable, Optional

from dataclasses import dataclass


@dataclass
class FileMeta:
    path: str = None


class FileMetaCalculated(FileMeta):
    """
    Class that calculates file info from a stream of bytes
    """

    def __init__(self, calculator: Optional['FileMetaCalculated'] = None):
        self.calculator = calculator

    @abstractmethod
    def write(self, b: bytes):
        pass


class File:
    def __init__(self,
                 meta: Optional[FileMeta] = None,
                 parent: Optional['File'] = None):
        self._meta = meta
        self.parent = parent

    @property
    def meta(self) -> FileMeta:
        return self._meta


class Stream(File):
    """
    A non seekable file-like
    """

    def __init__(self, file, meta: Optional[FileMeta] = None, parent: Optional['Stream'] = None):
        super().__init__(meta=meta, parent=parent)
        self.__f = file

    @property
    def file(self):
        return self.__f


class SeekableStream(Stream):
    """
    A seekable file-like
    """
    pass


class FileGenerator:
    """
    A class that generates files based on an input
    """

    def __init__(self, files: Iterable[File]):
        self.files = files

    @abstractmethod
    def get_files(self) -> None:
        pass

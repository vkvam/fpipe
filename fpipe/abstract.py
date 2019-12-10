from abc import abstractmethod

from typing import Iterable, Optional

from dataclasses import dataclass


@dataclass
class FileInfo:
    path: str = None
    size: int = None
    checksum_md5: str = None


class FileInfoCalculator:
    """
    Class that calculates file info from a stream of bytes
    """

    def __init__(self, source_calculator: Optional['FileInfoCalculator'] = None):
        self.source_calculator = source_calculator

    @abstractmethod
    def write(self, b: bytes):
        pass

    @abstractmethod
    def get(self) -> FileInfo:
        pass


class File:
    def __init__(self, file_info_generator: FileInfoCalculator):
        self.file_info_generator = file_info_generator


class Stream(File):
    """
    A non seekable file-like
    """

    def __init__(self, f, metadata):
        super().__init__(metadata)
        self.__f = f

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

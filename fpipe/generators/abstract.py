from abc import abstractmethod
from typing import Iterable, TypeVar, Generic

from fpipe.file import FileStream, File

T = TypeVar('T', File, FileStream)


class FileGenerator(Generic[T]):
    """
    A class that generates files based on an input
    """

    def __init__(self, files: Iterable[T]):
        self.files: Iterable[T] = files

    @abstractmethod
    def __iter__(self) -> Iterable[T]:
        raise NotImplementedError


class FileStreamGenerator(FileGenerator[T]):
    """
    A class that generates streams based on an input
    """

    @abstractmethod
    def __iter__(self) -> Iterable[T]:
        pass

    def flush(self):
        for f in self:
            f.file.flush()


class IncompatibleFileTypeException(Exception):
    def __init__(self, file: File):
        super(f"{file.__class__.__name__} not supported")

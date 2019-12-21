from abc import abstractmethod
from typing import Iterable, TypeVar, Generic, List, Union, Generator, Iterator

from fpipe.file.file import FileStream, File, SeekableFileStream

T = TypeVar("T", File, FileStream, SeekableFileStream)
T2 = TypeVar("T2", File, FileStream, SeekableFileStream)


class FileGenerator(Generic[T, T2]):
    """
    A class that generates files based on an input
    """

    def __init__(self):
        self.sources: List[Union[Iterable[T], T]] = []

    def reset(self):
        self.sources.clear()

    def chain(self, source: Union[Iterable[T], T]) -> "FileGenerator[T,T2]":
        self.sources.append(source)
        return self

    def flush(self) -> None:
        for f in self:
            if isinstance(f, FileStream):
                f.file.flush()

    def flush_iter(self) -> Iterator[Union[T, T2]]:
        for f in self:
            if isinstance(f, FileStream):
                f.file.flush()
                yield f

    @property
    def files(self) -> Iterator[T]:
        for source in self.sources:
            if isinstance(source, File):
                yield source
            elif isinstance(source, Generator):
                yield from source
            elif isinstance(source, Iterable):
                for s in source:
                    yield s

    @abstractmethod
    def __iter__(self) -> Iterator[Union[T, T2]]:
        raise NotImplementedError


class IncompatibleFileTypeException(Exception):
    def __init__(self, file: File):
        super(f"{file.__class__.__name__} not supported")

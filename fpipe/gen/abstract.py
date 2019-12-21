from abc import abstractmethod
from typing import Iterable, TypeVar, Generic, List, Union, Generator

from fpipe.file.file import FileStream, File

T = TypeVar("T", File, FileStream)


class FileGenerator(Generic[T]):
    """
    A class that generates files based on an input
    """

    def __init__(self):
        self.sources: List[Union[Iterable[File], File]] = []

    def reset(self):
        self.sources.clear()

    def chain(self, source: Union[Iterable[File], File]) -> "FileGenerator[T]":
        self.sources.append(source)
        return self

    def flush(self) -> None:
        for f in self:
            if isinstance(f, FileStream):
                f.file.flush()

    def flush_iter(self) -> Iterable[T]:
        for f in self:
            if isinstance(f, FileStream):
                f.file.flush()
                yield f

    @property
    def files(self) -> Iterable[T]:
        for source in self.sources:
            if isinstance(source, File):
                yield source
            elif isinstance(source, Generator):
                yield from source
            elif isinstance(source, Iterable):
                for s in source:
                    yield s

    @abstractmethod
    def __iter__(self) -> Iterable[T]:
        raise NotImplementedError


class IncompatibleFileTypeException(Exception):
    def __init__(self, file: File):
        super(f"{file.__class__.__name__} not supported")

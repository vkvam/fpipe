from abc import abstractmethod
from typing import Iterable, TypeVar, Generic, List, Union, Generator

from fpipe.file.file import FileStream, File

T = TypeVar('T', File, FileStream)


class FileGenerator(Generic[T]):
    """
    A class that generates files based on an input
    """

    def __init__(self):
        self.sources: List[Union[Iterable[T], T]] = []

    def reset(self):
        self.sources.clear()

    def chain(self, source: Union[Iterable[T], T]) -> 'FileGenerator':
        self.sources.append(source)
        return self

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

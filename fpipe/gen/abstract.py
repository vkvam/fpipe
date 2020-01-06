from abc import abstractmethod
from typing import Iterable, TypeVar, Generic, List, Union, Generator, Iterator

from fpipe.file.file import FileStream, File, SeekableFileStream

SOURCE = TypeVar("SOURCE", File, FileStream, SeekableFileStream)
TARGET = TypeVar("TARGET", File, FileStream, SeekableFileStream)


class FileGenerator(Generic[SOURCE, TARGET]):
    """
    A class that generates files based on an input
    """

    def __init__(self):
        self.sources: List[Union[Iterable[SOURCE], SOURCE]] = []

    def reset(self):
        self.sources.clear()

    def chain(self, source: Union[Iterable[SOURCE], SOURCE]) -> \
            "FileGenerator[SOURCE,TARGET]":
        self.sources.append(source)
        return self

    def flush(self) -> None:
        for f in self:
            if isinstance(f, FileStream):
                f.file.flush()

    def flush_iter(self) -> Iterator[Union[SOURCE, TARGET]]:
        for f in self:
            if isinstance(f, FileStream):
                f.file.flush()
                yield f

    @property
    def source_files(self) -> Iterator[SOURCE]:
        for source in self.sources:
            if isinstance(source, File):
                yield source
            elif isinstance(source, Generator):
                yield from source
            elif isinstance(source, Iterable):
                for s in source:
                    yield s

    @abstractmethod
    def __iter__(self) -> Iterator[Union[SOURCE, TARGET]]:
        raise NotImplementedError


class IncompatibleFileTypeException(Exception):
    def __init__(self, file: File):
        super(f"{file.__class__.__name__} not supported")

from abc import abstractmethod
from threading import Thread
from typing import Callable, Optional, Generator, Iterator, Generic, Union, \
    List, Iterable, TypeVar
from fpipe.file import FileStream, File, SeekableFileStream

SOURCE = TypeVar("SOURCE", File, FileStream, SeekableFileStream)
TARGET = TypeVar("TARGET", File, FileStream, SeekableFileStream)


class CallableResponse(Generic[SOURCE, TARGET]):
    def __init__(self, file: SOURCE, *thread: Thread):
        self.file: SOURCE = file
        self.threads = thread


class FileGenerator(Generic[SOURCE, TARGET]):
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

    def __iter__(self) -> Iterator[Union[SOURCE, TARGET]]:
        for source in self.source_files:
            responses: Optional[
                Generator[CallableResponse, None, None]
            ] = self.process(source)
            if responses:
                for resp in responses:
                    if resp:

                        if resp.threads:
                            for t in resp.threads:
                                t.start()

                        if resp.file:
                            yield resp.file

                        if resp.threads:
                            for t in resp.threads:
                                t.join()
                    else:
                        yield source
            else:
                yield source

    @abstractmethod
    def process(
            self, source: SOURCE
    ) -> Optional[Generator[CallableResponse, None, None]]:
        pass


class Method(FileGenerator[SOURCE, TARGET]):
    """
    Convenience File Generator when processing can be handled by a single
    method.
    """
    def __init__(
            self,
            executor: Optional[
                Callable[
                    [SOURCE], Optional[Generator[CallableResponse, None, None]]
                ]
            ] = None,
    ):
        super().__init__()
        self.callable: Optional[
            Callable[
                [SOURCE], Optional[Generator[CallableResponse, None, None]]
            ]
        ] = executor

    def process(
            self, source: SOURCE
    ) -> Optional[Generator[CallableResponse, None, None]]:
        if self.callable:
            return self.callable(source)
        return None

from abc import abstractmethod
from threading import Thread
from typing import Callable, Optional, Generator, Iterator, Generic, Union, \
    List, Iterable, TypeVar
from fpipe.file import FileStream, File, SeekableFileStream
from fpipe.meta.abstract import FileMeta

SOURCE = TypeVar("SOURCE", File, FileStream, SeekableFileStream)
TARGET = TypeVar("TARGET", File, FileStream, SeekableFileStream)


class FileGeneratorResponse(Generic[SOURCE, TARGET]):
    def __init__(self, file: SOURCE, *thread: Thread):
        self.file: SOURCE = file
        self.threads = thread


MetaResolver = Union[
    FileMeta,
    Callable[
        [File],
        FileMeta

    ]
]


class FileGenerator(Generic[SOURCE, TARGET]):
    def __init__(
            self,
            process_meta: Optional[
                Union[Iterable[MetaResolver], MetaResolver]] = None
    ):
        self.sources: List[Union[Iterable[SOURCE], SOURCE]] = []
        self.process_meta: Iterable[MetaResolver]

        if isinstance(process_meta, Iterable):
            self.process_meta = process_meta
        elif process_meta is None:
            self.process_meta = []
        else:
            self.process_meta = [process_meta]

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
                Generator[FileGeneratorResponse, None, None]
            ] = self.process(
                source,
                File(
                    parent=source,
                    meta=(
                        m if isinstance(m, FileMeta) else m(source) for m in
                        self.process_meta
                    )
                )
            )

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
            self,
            source: SOURCE,
            generated_meta_container: File
    ) -> Optional[Generator[FileGeneratorResponse, None, None]]:
        pass


class Method(FileGenerator[SOURCE, TARGET]):
    """
    Convenience File Generator for cases where processing can be
    handled by a single method.
    """

    def __init__(
            self,
            executor: Optional[
                Callable[
                    [SOURCE], Optional[
                        Generator[FileGeneratorResponse, None, None]
                    ]
                ]
            ] = None
    ):
        super().__init__()
        self.callable: Optional[
            Callable[
                [SOURCE], Optional[
                    Generator[FileGeneratorResponse, None, None]
                ]
            ]
        ] = executor

    def process(
            self,
            source: SOURCE,
            generated_meta_container: File
    ) -> Optional[Generator[FileGeneratorResponse, None, None]]:
        if self.callable:
            return self.callable(source)
        return None

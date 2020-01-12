from abc import abstractmethod
from threading import Thread
from typing import Callable, Optional, Generator, Iterator, Union, List, \
    Iterable
from fpipe.file import File
from fpipe.meta.abstract import FileMeta


class FileGeneratorResponse:
    def __init__(self, file: File, *thread: Thread):
        self.file: File = file
        self.threads = thread


MetaResolver = Union[
    FileMeta,
    Callable[
        [File],
        FileMeta

    ]
]


class FileGenerator:
    def __init__(
            self,
            process_meta: Optional[
                Union[Iterable[MetaResolver], MetaResolver]
            ] = None
    ):
        self.sources: List[Union[Iterable[File], File]] = []
        self.process_meta: Iterable[MetaResolver]

        if isinstance(process_meta, Iterable):
            self.process_meta = process_meta
        elif process_meta is None:
            self.process_meta = []
        else:
            self.process_meta = [process_meta]

    def reset(self):
        self.sources.clear()

    def chain(self, source: Union[Iterable[File], File]) -> "FileGenerator":
        self.sources.append(source)
        return self

    def flush(self) -> None:
        for f in self:
            # if isinstance(f, File):
            if f.file:
                f.file.flush()

    def flush_iter(self) -> Iterator[Union[File, File]]:
        for f in self:
            # if isinstance(f, File):
            if f.file:
                f.file.flush()
                yield f

    @property
    def source_files(self) -> Iterator[File]:
        for source in self.sources:
            if isinstance(source, File):
                yield source
            elif isinstance(source, Generator):
                yield from source
            elif isinstance(source, Iterable):
                for s in source:
                    yield s

    def __iter__(self) -> Iterator[Union[File, File]]:
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

    @abstractmethod
    def process(
            self,
            source: File,
            process_meta: File
    ) -> Optional[Generator[FileGeneratorResponse, None, None]]:
        raise NotImplementedError


class Method(FileGenerator):
    """
    Convenience File Generator for cases where processing can be
    handled by a single method.
    """

    def __init__(
            self,
            executor:
            Callable[
                [File], Optional[
                    Generator[FileGeneratorResponse, None, None]
                ]
            ]
    ):
        super().__init__()
        self.callable: Callable[
            [File], Optional[
                Generator[FileGeneratorResponse, None, None]
            ]
        ] = executor

    def process(
            self,
            source: File,
            process_meta: File
    ) -> Optional[Generator[FileGeneratorResponse, None, None]]:
        return self.callable(source)

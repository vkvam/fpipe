from abc import abstractmethod
from dataclasses import dataclass
from threading import Thread
from typing import Iterable, Callable, Optional, Generator

from fpipe.file.file import FileStream, File
from fpipe.gen.abstract import T, FileGenerator


@dataclass
class CallableResponse:
    def __init__(self, file: File, *thread: Thread):
        self.file = file
        self.threads = thread


class MethodGen(FileGenerator[T]):
    def __init__(self):
        super().__init__()

    @abstractmethod
    def executor(
        self, source: File
    ) -> Optional[Generator[CallableResponse, None, None]]:
        pass

    def __iter__(self) -> Iterable[FileStream]:
        for source in self.files:
            responses: Optional[
                Generator[CallableResponse, None, None]
            ] = self.executor(source)
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


class Method(MethodGen[T]):
    def __init__(
        self,
        executor: Optional[
            Callable[[File], Optional[Generator[CallableResponse, None, None]]]
        ] = None,
    ):
        super().__init__()
        self.callable = executor

    def executor(
        self, source: File
    ) -> Optional[Generator[CallableResponse, None, None]]:
        return self.callable(source)

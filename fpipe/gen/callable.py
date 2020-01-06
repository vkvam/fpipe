from abc import abstractmethod
from threading import Thread
from typing import Callable, Optional, Generator, Iterator, Generic, Union

from fpipe.gen.abstract import SOURCE, TARGET, FileGenerator


class CallableResponse(Generic[SOURCE, TARGET]):
    def __init__(self, file: SOURCE, *thread: Thread):
        self.file: SOURCE = file
        self.threads = thread


class MethodGen(FileGenerator[SOURCE, TARGET]):
    def __init__(self):
        super().__init__()

    @abstractmethod
    def executor(
        self, source: SOURCE
    ) -> Optional[Generator[CallableResponse, None, None]]:
        pass

    def __iter__(self) -> Iterator[Union[SOURCE, TARGET]]:
        for source in self.source_files:
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


class Method(MethodGen[SOURCE, TARGET]):
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

    def executor(
        self, source: SOURCE
    ) -> Optional[Generator[CallableResponse, None, None]]:
        if self.callable:
            return self.callable(source)
        return None

from abc import abstractmethod
from threading import Thread
from typing import Callable, Optional, Generator, Iterator, Generic, Union

from fpipe.gen.abstract import T, T2, FileGenerator


class CallableResponse(Generic[T, T2]):
    def __init__(self, file: T, *thread: Thread):
        self.file: T = file
        self.threads = thread


class MethodGen(FileGenerator[T, T2]):
    def __init__(self):
        super().__init__()

    @abstractmethod
    def executor(
        self, source: T
    ) -> Optional[Generator[CallableResponse, None, None]]:
        pass

    def __iter__(self) -> Iterator[Union[T, T2]]:
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


class Method(MethodGen[T, T2]):
    def __init__(
        self,
        executor: Optional[
            Callable[[T], Optional[Generator[CallableResponse, None, None]]]
        ] = None,
    ):
        super().__init__()
        self.callable: Optional[
            Callable[[T], Optional[Generator[CallableResponse, None, None]]]
        ] = executor

    def executor(
        self, source: T
    ) -> Optional[Generator[CallableResponse, None, None]]:
        if self.callable:
            return self.callable(source)
        return None

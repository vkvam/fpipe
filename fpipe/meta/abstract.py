from abc import abstractmethod
from typing import TypeVar, Type, cast, Generic, Optional, Callable, Union

from fpipe.exceptions import FileMetaException

T = TypeVar("T")


class FileMeta(Generic[T]):
    @property
    @abstractmethod
    def value(self) -> T:
        raise NotImplementedError


class FileMetaValue(FileMeta[T]):
    def __init__(self, value: T):
        self.v = value

    @property
    def value(self) -> T:
        return self.v


class MetaMap:
    def __init__(self):
        self.metas = {}

    def __contains__(self, t: Type[T]):
        return t in self.metas.keys()

    def set(self, t: T):
        self.metas[type(t)] = t

    def __getitem__(self, t: Type[T]) -> T:
        return cast(T, self.metas[t])


class FileMetaCalculator(Generic[T]):
    def __init__(self, calculable: "FileMetaFuture[T]"):
        self.calculable = calculable

    @abstractmethod
    def write(self, s: Union[bytes, bytearray]):
        pass


class FileMetaFuture(FileMeta[T]):
    def __init__(
        self,
        value: Optional[T] = None,
        future: Optional[Callable[[], T]] = None,
    ):
        self.__v = value
        self.__future = future

    @staticmethod
    def get_calculator() -> Optional[FileMetaCalculator[T]]:
        return None

    def set_value(self, v: T):
        self.__v = v

    @property
    def value(self) -> T:
        if self.__v:
            return self.__v
        elif self.__future:
            return self.__future()
        raise FileMetaException(self)

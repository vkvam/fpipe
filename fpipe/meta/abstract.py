from abc import abstractmethod
from typing import TypeVar, Type, cast, Generic, Optional, Callable, Union

from fpipe.exceptions import FileDataException

T = TypeVar("T")


class FileDataCalculator(Generic[T]):
    """abstract class for something that can produce a FileData
    """

    def __init__(self, calculable: "Type[FileData[T]]"):
        self.calculable = calculable()

    @abstractmethod
    def write(self, s: Union[bytes, bytearray]):
        raise NotImplementedError


class FileData(Generic[T]):
    """Abstract class for all FileData

    FileData value can be set in of the following ways:
    - Immediately with through a value set with __init__(value)
    - In the future through a method set with __init__(future)
    - In the future by a FileDataCalculator (see fpipe.gen.meta.Meta)
    """

    def __init__(
            self,
            value: Optional[T] = None,
            future: Optional[Callable[[], T]] = None,
    ):
        self.__value = value
        self.__future = future

    @staticmethod
    def get_calculator() -> Optional[FileDataCalculator[T]]:
        return None

    @property
    def value(self) -> T:
        if self.__value:
            return self.__value
        elif self.__future:
            return self.__future()
        raise FileDataException(self)

    @value.setter
    def value(self, v: T):
        self.__value = v


class MetaMap:
    def __init__(self):
        self.metas = {}

    def __contains__(self, t: Type[T]):
        return t in self.metas.keys()

    def set(self, t: T):
        self.metas[type(t)] = t

    def __getitem__(self, t: Type[T]) -> T:
        return cast(T, self.metas[t])

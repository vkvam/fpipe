from abc import abstractmethod
from typing import TypeVar, Type, cast, Generic

T = TypeVar('T')


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
        self.metas = {
        }

    def set(self, t: T):
        self.metas[type(t)] = t

    def __getitem__(self, t: Type[T]) -> T:
        return cast(T, self.metas[t])

import datetime
from typing import TypeVar, Type, cast

T = TypeVar('T', bound='Meta')


class FileMeta:
    pass


class MetaStr(FileMeta):
    def __init__(self, value: str):
        self.v = value

    @property
    def value(self) -> str:
        return self.v


class MetaDateTime(FileMeta):
    def __init__(self, value: datetime.datetime):
        self.v = value

    @property
    def value(self) -> datetime.datetime:
        return self.v


class MetaInt(FileMeta):
    def __init__(self, value: int):
        self.v = value

    @property
    def value(self) -> int:
        return self.v


class MetaFloat(FileMeta):
    def __init__(self, value: float):
        self.v = value

    @property
    def value(self) -> float:
        return self.v


class MetaBool(FileMeta):
    def __init__(self, value: bool):
        self.v = value

    @property
    def value(self) -> bool:
        return self.v


class MetaMap:
    def __init__(self):
        self.metas = {
        }

    def set(self, t: T):
        self.metas[type(t)] = t

    def __getitem__(self, t: Type[T]) -> T:
        return cast(T, self.metas[t])

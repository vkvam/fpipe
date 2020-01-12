from typing import Optional, Type, Iterable, Union, BinaryIO

from fpipe.exceptions import FileMetaException
from fpipe.meta.abstract import FileMeta, MetaMap, T


class File:
    """container for FileMeta related to a file
    """

    def __init__(
            self,
            file: Optional[BinaryIO] = None,
            parent: Optional['File'] = None,
            meta: Optional[Union[FileMeta, Iterable[FileMeta]]] = None
    ):
        self.parent = parent
        self.meta_map = MetaMap()
        meta = [meta] if isinstance(meta, FileMeta) else meta
        if meta:
            for m in meta:
                self.meta_map.set(m)

        self.__file = file

    @classmethod
    def meta_prioritized(cls, t: Type[T], *sources: 'File'):
        error = FileMetaException(t)
        for s in sources:
            try:
                return cls.__meta(t, s)
            except FileMetaException:
                pass
        raise error

    @classmethod
    def __meta(cls,
               t: Type[T],
               obj: Optional['File'] = None,
               nth: Optional[int] = None) -> T:
        count = 0
        while obj is not None:
            try:
                if nth is None or nth == 0:
                    return obj.meta_map[t]
                else:

                    if count == nth:
                        return obj.meta_map[t]
                    if t in obj.meta_map:
                        count += 1
                    obj = obj.parent
            except KeyError:
                if obj:
                    obj = obj.parent
                else:
                    raise FileMetaException(t)
        raise FileMetaException(t)

    def meta(self, t: Type[T],
             nth: Optional[int] = None) -> T:

        return self.__meta(t, self, nth)

    @property
    def file(self) -> Optional[BinaryIO]:
        return self.__file

from typing import Optional, Type, IO, Iterable, Union

from fpipe.exceptions import FileMetaException
from fpipe.meta.abstract import FileMeta, MetaMap, T


class File:
    def __init__(self,
                 parent: Optional['File'] = None,
                 meta: Optional[Union[FileMeta, Iterable[FileMeta]]] = None):
        self.parent = parent
        self.meta_map = MetaMap()
        meta = [meta] if isinstance(meta, FileMeta) else meta
        if meta:
            for m in meta:
                self.meta_map.set(m)

    # def meta(self, item: Type[FileMeta]) -> FileMeta:
    def meta(self, t: Type[T], nth: Optional[int] = None) -> T:
        obj: Optional[File] = self
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
                obj = obj.parent
        raise FileMetaException(t)


class FileStream(File):
    """
    A non seekable file-like
    """

    def __init__(self,
                 file: IO[bytes],
                 parent: Optional['File'] = None,
                 meta: Optional[Union[FileMeta, Iterable[FileMeta]]] = None):
        super().__init__(parent=parent, meta=meta)
        self.__f = file

    @property
    def file(self) -> IO[bytes]:
        return self.__f


class SeekableFileStream(FileStream):
    """
    A seekable file-like
    """
    pass

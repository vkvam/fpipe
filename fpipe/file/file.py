from typing import Optional, Type, IO, Iterable, Union

from fpipe.meta.abstract import FileMeta, MetaMap, T


class File:
    def __init__(self,
                 parent: Optional['File'] = None,
                 meta: Iterable[FileMeta] = ()):
        self.parent = parent
        self.meta_map = MetaMap()
        if meta:
            for m in meta:
                self.meta_map.set(m)

    # def meta(self, item: Type[FileMeta]) -> FileMeta:
    def meta(self, t: Type[T]) -> T:
        obj: Optional[File] = self
        while obj is not None:
            try:
                return obj.meta_map[t]
            except KeyError:
                obj = obj.parent
        raise KeyError(f"Not found {t}")


class FileStream(File):
    """
    A non seekable file-like
    """

    def __init__(self,
                 file: IO[bytes],
                 parent: Optional['File'] = None,
                 meta: Optional[Union[FileMeta, Iterable[FileMeta]]] = None):
        super().__init__(parent=parent, meta=[meta] if isinstance(meta, FileMeta) else meta)
        self.__f = file

    @property
    def file(self) -> IO[bytes]:
        return self.__f


class SeekableFileStream(FileStream):
    """
    A seekable file-like
    """
    pass

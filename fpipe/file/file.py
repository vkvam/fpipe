from abc import abstractmethod
from typing import Optional, Iterable, Type, List

from fpipe.file.filemeta import FileMeta, MetaMap, T, MetaStr


class Path(MetaStr):
    pass


class File:
    def __init__(self,
                 parent: Optional['File'] = None,
                 meta: Optional[List[FileMeta]] = None):
        self.parent = parent
        self.meta_map = MetaMap()
        if meta:
            for m in meta:
                self.meta_map.set(m)

    # def meta(self, item: Type[FileMeta]) -> FileMeta:
    def meta(self, t: Type[T]) -> T:
        obj: File = self
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
                 file,
                 parent: Optional['File'] = None,
                 meta: Optional[List[FileMeta]] = None):
        super().__init__(parent=parent, meta=meta)
        self.__f = file

    @property
    def file(self):
        return self.__f


class SeekableFileStream(FileStream):
    """
    A seekable file-like
    """
    pass


class FileGenerator:
    """
    A class that generates files based on an input
    """

    def __init__(self, files: Iterable[File]):
        self.files = files

    @abstractmethod
    def __iter__(self) -> Iterable[File]:
        pass


class FileStreamGenerator(FileGenerator):
    """
    A class that generates streams based on an input
    """

    @abstractmethod
    def __iter__(self) -> Iterable[FileStream]:
        pass

    def start(self):
        for f in self:
            read = f.file.read
            while read(2 ** 14):
                pass


class IncompatibleFileTypeException(Exception):
    def __init__(self, _class):
        pass

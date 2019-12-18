from abc import abstractmethod

from fpipe.meta.abstract import FileMeta, T


class FileMetaCalculator(FileMeta[T]):

    @abstractmethod
    def write(self, b: bytes):
        pass
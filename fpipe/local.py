from typing import Union, Iterable

from .abstract import File, FileGenerator, Stream, SeekableStream, FileMetaCalculated, FileMeta


class LocalFileInfoCalculator(FileMetaCalculated):
    def __init__(self, path):
        super().__init__()
        self.path = path

    def write(self, b: bytes):
        pass

    def get(self) -> FileMeta:
        return FileMeta(path=self.path)


class LocalFile(File):
    def __init__(self, path: str):
        super().__init__(LocalFileInfoCalculator(path))
        self.path = path


class LocalSeekableStream(SeekableStream):
    pass


class LocalFileGenerator(FileGenerator):
    def get_files(self) -> Iterable[Union[LocalSeekableStream]]:
        for source in self.files:
            try:
                if isinstance(source, LocalFile):
                    with open(source.path, 'rb') as f:
                        yield LocalSeekableStream(f, source.meta)
                elif isinstance(source, Stream):
                    pass
            except Exception as e:
                raise e

import threading
from typing import Union, Iterable

from .abstract import File, Stream, SeekableStream, FileMetaCalculated, FileMeta, FileStreamGenerator


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


class LocalFileGenerator(FileStreamGenerator):
    def __init__(self, files: Iterable[File], pass_through=False):
        super().__init__(files)
        self.pass_through = pass_through

    def get_files(self) -> Iterable[Union[LocalSeekableStream, Stream]]:
        for source in self.files:
            try:
                if isinstance(source, LocalFile):
                    with open(source.path, 'rb') as f:
                        yield LocalSeekableStream(f, parent=source)
                elif isinstance(source, Stream):
                    def __process():
                        with open(source.meta.path, 'wb') as f:
                            while True:
                                b = source.file.read(2 ** 14)
                                f.write(b)
                                if not b:
                                    break

                    if self.pass_through:
                        proc_thread = threading.Thread(target=__process, name=f'{self.__class__.__name__}', daemon=True)
                        proc_thread.start()
                        yield Stream(source.file, parent=source)
                        proc_thread.join()
                    else:
                        __process()
                        with open(source.meta.path, 'rb') as f:
                            yield LocalSeekableStream(f, parent=source)
            except Exception:
                raise

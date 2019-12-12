import threading
from typing import Union, Iterable, Callable

from fpipe.utils import BytesLoop
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
    def __init__(self, files: Iterable[File], pass_through=False, pathname_resolver: Callable[[File], str] = None):
        """

        :param files: files to process
        :param pass_through: pass through the source instead of waiting for writes to complete
        :param pathname_resolver: a function that sets the path for files written, the starting file is the source file
        """
        super().__init__(files)
        self.pass_through = pass_through
        self.pathname_resolver = pathname_resolver

    def __iter__(self) -> Iterable[Union[LocalSeekableStream, Stream]]:
        for source in self.files:
            try:
                if isinstance(source, LocalFile):
                    with open(source.path, 'rb') as f:
                        yield LocalSeekableStream(f, parent=source)
                elif isinstance(source, Stream):
                    def __process(byte_loop=None):
                        path_name = self.pathname_resolver(source) if self.pathname_resolver else source.meta.path
                        with open(path_name, 'wb') as f2:
                            while True:
                                b = source.file.read(2 ** 14)
                                if byte_loop:
                                    byte_loop.write(b)
                                f2.write(b)
                                if not b:
                                    break

                    if self.pass_through:
                        pass_through = BytesLoop()
                        proc_thread = threading.Thread(target=__process, args=(pass_through,),
                                                       name=f'{self.__class__.__name__}', daemon=True)
                        proc_thread.start()
                        yield Stream(pass_through, parent=source)
                        proc_thread.join()
                    else:
                        __process()
                        with open(source.meta.path, 'rb') as f:
                            yield LocalSeekableStream(f, parent=source)
            except Exception:
                raise

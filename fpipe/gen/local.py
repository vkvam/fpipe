import threading
from typing import Union, Iterable, Callable

from fpipe.file.file import File, FileStream, SeekableFileStream
from fpipe.file.local import LocalFile
from fpipe.gen.abstract import FileStreamGenerator, IncompatibleFileTypeException
from fpipe.meta.path import Path
from fpipe.utils.bytesloop import BytesLoop


class LocalFileGenerator(FileStreamGenerator):
    def __init__(self,
                 pass_through=False,
                 pathname_resolver: Callable[[File], str] = None
                 ):
        """
        :param files: iterator with files to process
        :param pass_through: pass through the source instead of waiting for writes to complete
        :param pathname_resolver: a function that sets the path for files written
        """
        super().__init__()
        self.pass_through = pass_through
        self.pathname_resolver = pathname_resolver

    def __iter__(self) -> Iterable[Union[SeekableFileStream, FileStream]]:
        for source in self.files:
            try:
                if isinstance(source, LocalFile):
                    with open(source.meta(Path).value, 'rb') as f:
                        yield SeekableFileStream(f, parent=source)
                elif isinstance(source, FileStream):
                    def __process(byte_loop=None):
                        path_name = self.pathname_resolver(
                            source
                        ) if self.pathname_resolver else source.meta(Path).value

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
                        proc_thread = threading.Thread(target=__process,
                                                       args=(pass_through,),
                                                       name=f'{self.__class__.__name__}', daemon=True)
                        proc_thread.start()
                        yield FileStream(pass_through, parent=source)
                        proc_thread.join()
                    else:
                        __process()
                        with open(source.meta(Path).value, 'rb') as f:
                            yield SeekableFileStream(f, parent=source)
                else:
                    raise IncompatibleFileTypeException(source)
            except Exception:
                raise

import threading
from typing import Callable, Optional

from fpipe.file.file import File, FileStream, SeekableFileStream
from fpipe.file.local import LocalFile
from fpipe.gen.generator import FileGenerator, FileGeneratorResponse
from fpipe.meta.path import Path
from fpipe.utils.bytesloop import BytesLoop
from fpipe.utils.const import PIPE_BUFFER_SIZE


def _process(
    source: FileStream, path_name: str, byte_loop: Optional[BytesLoop] = None
):
    with open(path_name, "wb") as f2:
        while True:
            b = source.file.read(PIPE_BUFFER_SIZE)
            if byte_loop:
                byte_loop.write(b)
            f2.write(b)
            if not b:
                break


class Local(FileGenerator[FileStream, FileStream]):
    def __init__(
        self,
        pass_through=False,
        pathname_resolver: Callable[[File], str] = None,
    ):
        """
        :param files: iterator with files to process
        :param pass_through: pass through the source instead of waiting for
        writes to complete
        :param pathname_resolver: callable that sets the path to write to
        """
        super().__init__()
        self.pass_through = pass_through
        self.pathname_resolver = pathname_resolver

    def process(self, source: File):
        path_name = (
            self.pathname_resolver(source)
            if self.pathname_resolver
            else source.meta(Path).value
        )
        path_meta = Path(path_name)

        if isinstance(source, LocalFile):
            with open(source.meta(Path).value, "rb") as f:
                yield FileGeneratorResponse(
                    SeekableFileStream(f, parent=source, meta=path_meta)
                )
        elif isinstance(source, FileStream):
            if self.pass_through:
                with BytesLoop() as byte_loop:
                    proc_thread = threading.Thread(
                        target=_process,
                        args=(source, path_name, byte_loop),
                        name=f"{self.__class__.__name__}",
                        daemon=True,
                    )
                    yield FileGeneratorResponse(
                        FileStream(byte_loop, parent=source, meta=path_meta),
                        proc_thread,
                    )
            else:
                _process(source, path_name)
                with open(source.meta(Path).value, "rb") as f:
                    yield FileGeneratorResponse(
                        SeekableFileStream(f, parent=source, meta=path_meta)
                    )

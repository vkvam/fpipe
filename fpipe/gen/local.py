import threading
from typing import Optional, Union, Iterable

from fpipe.file.file import File, FileStream, SeekableFileStream
from fpipe.gen.generator import FileGenerator, FileGeneratorResponse, \
    MetaResolver
from fpipe.meta.path import Path
from fpipe.utils.bytesloop import BytesLoop
from fpipe.utils.const import PIPE_BUFFER_SIZE


def _process(
        source: FileStream, path_name: str,
        byte_loop: Optional[BytesLoop] = None
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
            process_meta: Optional[
                Union[Iterable[MetaResolver], MetaResolver]] = None
    ):
        """
        :param pass_through: pass through the source instead of waiting for
        writes to complete
        :param process_meta: callable that can produce FileMeta
        needed by self.process()
        """
        super().__init__(process_meta)
        self.pass_through = pass_through

    def process(self,
                source: File,
                process_meta_container: File):
        path = File.meta_prioritized(
            Path,
            process_meta_container,
            source
        )

        if isinstance(source, FileStream):
            if self.pass_through:
                with BytesLoop() as byte_loop:
                    proc_thread = threading.Thread(
                        target=_process,
                        args=(source, path.value, byte_loop),
                        name=f"{self.__class__.__name__}",
                        daemon=True,
                    )
                    yield FileGeneratorResponse(
                        FileStream(byte_loop, parent=source, meta=path),
                        proc_thread,
                    )
            else:
                _process(source, path.value)
                with open(path.value, "rb") as f:
                    yield FileGeneratorResponse(
                        SeekableFileStream(f, parent=source, meta=path)
                    )
        else:
            with open(path.value, "rb") as f:
                yield FileGeneratorResponse(
                    SeekableFileStream(f, parent=source, meta=path)
                )

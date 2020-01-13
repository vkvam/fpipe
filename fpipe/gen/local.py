import threading
from typing import Optional, Union, Iterable, BinaryIO

from fpipe.exceptions import FileDataException
from fpipe.file.file import File
from fpipe.gen.generator import FileGenerator, FileGeneratorResponse, \
    MetaResolver
from fpipe.meta.path import Path
from fpipe.meta.stream import Stream
from fpipe.utils.bytesloop import BytesLoop
from fpipe.utils.const import PIPE_BUFFER_SIZE
from fpipe.utils.meta import meta_prioritized


class Local(FileGenerator):
    def __init__(
            self,
            pass_through=False,
            process_meta: Optional[
                Union[Iterable[MetaResolver], MetaResolver]] = None
    ):
        """
        :param pass_through: pass through the source instead of waiting for
        writes to complete
        :param process_meta: callable that can produce FileData
        needed by self.process()
        """
        super().__init__(process_meta)
        self.pass_through = pass_through

    @staticmethod
    def __process_stream(
            source_stream: BinaryIO, path_name: str,
            byte_loop: Optional[BytesLoop] = None
    ):
        with open(path_name, "wb") as f2:
            while True:
                b = source_stream.read(PIPE_BUFFER_SIZE)
                if byte_loop:
                    byte_loop.write(b)
                f2.write(b)
                if not b:
                    break

    def process(self, source: File, process_meta: File):

        path = meta_prioritized(
            Path,
            process_meta,
            source
        )

        try:
            source_stream = source[Stream]
            if self.pass_through:
                with BytesLoop() as byte_loop:
                    proc_thread = threading.Thread(
                        target=Local.__process_stream,
                        args=(source_stream, path, byte_loop),
                        name=f"{self.__class__.__name__}",
                        daemon=True,
                    )
                    yield FileGeneratorResponse(
                        File(stream=byte_loop, parent=source, meta=Path(
                            path
                        )),
                        proc_thread,
                    )
            else:
                Local.__process_stream(source_stream, path)
                with open(path, "rb") as f:
                    yield FileGeneratorResponse(
                        File(stream=f, parent=source, meta=Path(path))
                    )
        except FileDataException:
            with open(path, "rb") as f:
                yield FileGeneratorResponse(
                    File(stream=f, parent=source, meta=Path(path))
                )

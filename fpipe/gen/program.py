import shlex
import subprocess
import threading
from typing import Optional, Generator, Union, List, BinaryIO

from fpipe.exceptions import FileDataException
from fpipe.file.file import File
from fpipe.gen.generator import FileGenerator, FileGeneratorResponse
from fpipe.meta.stream import Stream
from fpipe.utils.bytesloop import BytesLoop
from fpipe.utils.const import PIPE_BUFFER_SIZE


class Program(FileGenerator):

    def __init__(
            self,
            command: Union[List[str], str],
            buffer_size=PIPE_BUFFER_SIZE,
            std_err=False,
            posix=True
    ):
        # TODO: It should be possible to adapt the command according to
        # the contents of a prevoious file, indicating that cmd
        # should be set on FileMeta, not on the generator
        """
        :param command: if a string is passed, shell
        :param buffer_size: buffer_size for subprocess stdin and stdout
        :param std_err: handle stderr (currently unsupported)
        :param posix=True, used by shlex to determine how to parse command
        """
        super().__init__()

        self.command = (
            shlex.split(command, posix=posix)
            if isinstance(command, str)
            else command
        )
        self.buf_size = buffer_size
        self.std_err = std_err
        if std_err:
            raise NotImplementedError(
                "Handling of stderr currently not supported"
            )

    @staticmethod
    def __std_in_to_cmd(source_file: BinaryIO, proc, buf_size):
        while True:
            read_chunk = source_file.read(buf_size)
            # stats.r(read_chunk)
            proc.stdin.write(read_chunk)

            if not read_chunk:  # EOF
                proc.stdin.close()
                break

    @staticmethod
    def __stdout_to_file(byte_loop, proc, buf_size):
        while True:
            e = proc.stdout.read(buf_size)
            # stats.w(e)
            byte_loop.write(e)

            if not e:
                proc.stdout.close()  # EOF
                break

    def process(
            self,
            source: File,
            generated_meta_container: File
    ) -> Optional[Generator[FileGeneratorResponse, None, None]]:
        buf_size = self.buf_size

        source_stream: Optional[BinaryIO]
        try:
            source_stream = source[Stream]
        except FileDataException:
            source_stream = None

        with BytesLoop() as byte_loop:
            with subprocess.Popen(
                self.command,
                stdin=subprocess.PIPE if source_stream else None,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE if self.std_err else subprocess.DEVNULL,
                bufsize=buf_size
            ) as proc:
                threads = [
                    threading.Thread(
                        target=self.__stdout_to_file,
                        args=(byte_loop, proc, buf_size),
                        name=f"{self.__class__.__name__} STD-OUT",
                        daemon=True,
                    )
                ]
                if source_stream:
                    threads.append(
                        threading.Thread(
                            target=self.__std_in_to_cmd,
                            args=(source_stream, proc, buf_size),
                            name=f"{self.__class__.__name__} STD-OUT",
                            daemon=True,
                        )
                    )
                yield FileGeneratorResponse(
                    File(stream=byte_loop, parent=source), *threads
                )

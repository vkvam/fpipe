import subprocess
import threading
from typing import Optional, Generator

from fpipe.file import File
from fpipe.file.file import FileStream
from fpipe.gen.callable import CallableGen, CallableResponse
from fpipe.utils.bytesloop import BytesLoop


class ProcessGen(CallableGen[FileStream]):

    def __init__(self, cmd, buf_size=2 ** 14, std_err=False):
        super().__init__()
        self.cmd = cmd
        self.buf_size = buf_size
        self.std_err = std_err
        if std_err:
            raise NotImplementedError("Handling of stderr currently not supported")

    @staticmethod
    def __std_in_to_cmd(source: FileStream, proc, buf_size):
        while True:
            read_chunk = source.file.read(buf_size)
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

    def executor(self, source: File) -> Optional[Generator[CallableResponse, None, None]]:
        buf_size = self.buf_size
        run_std_in = isinstance(source, FileStream)

        with BytesLoop(self.buf_size) as byte_loop:
            with subprocess.Popen(self.cmd,
                                  stdin=subprocess.PIPE if run_std_in else None,
                                  stdout=subprocess.PIPE,
                                  stderr=subprocess.PIPE if self.std_err else subprocess.DEVNULL,
                                  shell=isinstance(self.cmd, str)
                                  ) as proc:
                threads = [
                    threading.Thread(
                        target=self.__stdout_to_file,
                        args=(byte_loop, proc, buf_size),
                        name=f'{self.__class__.__name__} STD-OUT',
                        daemon=True
                    )
                ]
                if run_std_in:
                    threads.append(threading.Thread(target=self.__std_in_to_cmd,
                                                    args=(source, proc, buf_size),
                                                    name=f'{self.__class__.__name__} STD-OUT',
                                                    daemon=True))
                yield CallableResponse(FileStream(byte_loop, parent=source), *threads)

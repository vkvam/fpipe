import subprocess
import threading
from typing import Iterable

from .abstract import Stream, File, FileStreamGenerator
from .utils import BytesLoop


class ProcessFileGenerator(FileStreamGenerator):
    def __init__(self, files: Iterable[File], cmd, buf_size=2**14):
        super().__init__(files)
        self.cmd = cmd
        self.byte_loop = BytesLoop(buf_size)

    def __iter__(self) -> Iterable[Stream]:
        for source in self.files:
            buf_size = self.byte_loop.buf_size
            # stats = Stats(self.__class__.__name__)

            with subprocess.Popen(self.cmd,
                                  stdin=subprocess.PIPE,
                                  stdout=subprocess.PIPE,
                                  shell=isinstance(self.cmd, str)
                                  ) as proc:

                def __stdout_to_file():
                    while True:
                        e = proc.stdout.read(buf_size)
                        # stats.w(e)
                        self.byte_loop.write(e)

                        if not e:
                            proc.stdout.close()  # EOF
                            break

                def __std_in_to_cmd():
                    while True:
                        read_chunk = source.file.read(buf_size)
                        # stats.r(read_chunk)
                        proc.stdin.write(read_chunk)

                        if not read_chunk:  # EOF
                            proc.stdin.close()
                            break

                stdout_thread = threading.Thread(target=__stdout_to_file, name=f'{self.__class__.__name__} STD-OUT',
                                                 daemon=True)
                stdout_thread.start()

                stdin_thread = threading.Thread(target=__std_in_to_cmd, name=f'{self.__class__.__name__} STD-OUT',
                                                daemon=True)
                stdin_thread.start()
                yield Stream(self.byte_loop, parent=source)
                stdin_thread.join()
                stdout_thread.join()
            # Should be non-problematic reusing the byte-loop since files are yielded sequentially
            self.byte_loop.reset()

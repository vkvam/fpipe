import datetime
import io
import tarfile
from unittest import TestCase

from typing import Tuple

from fpipe.file.file import File
from fpipe.gen import Program, Tar
from fpipe.meta import Path, Size, Modified
from fpipe.meta.stream import Stream
from test_utils.test_file import ReversibleTestFile

FILE_NAME = "test.json"
ENCODING = 'utf-8'
TIME = 1574679361
TAR_FILES = [
    (f'{"folder/" * level}{FILE_NAME}', 2 ** level) for level in range(2, 20)
]


def __get_text_file(content: str, encoding: str) -> Tuple[io.BytesIO, int]:
    f = io.BytesIO()
    f.write(content.encode(encoding))
    try:
        return f, f.tell()
    finally:
        f.seek(0)


def create_nested_tar(tar: tarfile.TarFile):
    for path, file_size in TAR_FILES:
        tar_info = tarfile.TarInfo()
        tar_info.mtime = TIME
        tar_info.size = file_size
        tar_info.path = path
        f = ReversibleTestFile(file_size)
        tar.addfile(tar_info, f)
        yield f, path, file_size, TIME


class TestTar(TestCase):
    def test_tar(self):
        f = io.BytesIO()
        with tarfile.open(fileobj=f, mode='w') as tar:
            source_files = list(create_nested_tar(tar))
        f.seek(0)

        source_file_content = [
            (f.seek() or f.read(), p, s, t) for f, p, s, t in source_files
        ]

        # Pass it through ProcessFileGenerator to ensure we are working
        # with a non-seekable stream
        proc = Program(command='cat /dev/stdin').chain(File(f))
        for f in Tar().chain(proc):
            source_content, source_path, \
            source_size, source_time = source_file_content.pop(0)
            self.assertEqual(source_content, f[Stream].read())
            self.assertEqual(source_path, f[Path])
            self.assertEqual(source_size, f[Size])
            self.assertEqual(
                datetime.datetime.fromtimestamp(
                    source_time
                ), f[Modified]
            )
        self.assertEqual(len(source_file_content), 0)

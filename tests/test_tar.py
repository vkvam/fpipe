import io
import tarfile
from unittest import TestCase

from typing import Tuple

from fpipe.calculators import Size
from fpipe.file import FileStream
from fpipe.file.file import Path
from fpipe.generators import ProcessFileGenerator, TarFileInfo
from fpipe.generators import TarFileGenerator
from fpipe.generators.tar import ModifiedTar
from test_utils.test_file import ReversibleTestFile

FILE_NAME = "test.json"
ENCODING = 'utf-8'
TIME = 1574679361
TAR_FILES = [(f'{"folder/" * level}{FILE_NAME}', 2 ** level) for level in range(2, 20)]


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

        source_file_content = [(f.reset() and f.read(), p, s, t) for f, p, s, t in source_files]

        tar_stream = FileStream(f)

        # Pass it through ProcessFileGenerator to ensure we are working with a non-seekable stream
        proc = ProcessFileGenerator((tar_stream,), cmd='cat /dev/stdin')
        proc = TarFileGenerator(proc)
        for f in proc:
            source_content, source_path, source_size, source_time = source_file_content.pop(0)
            self.assertEqual(source_content, f.file.read())
            self.assertEqual(source_path, f.meta(Path).value)
            self.assertEqual(source_size, f.meta(Size).value)
            self.assertEqual(source_time, f.meta(ModifiedTar).value)
        self.assertEqual(len(source_file_content), 0)

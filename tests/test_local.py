import hashlib
import os

from unittest import TestCase

from fpipe.file import ByteFile, LocalFile
from fpipe.gen import Meta, Local
from fpipe.meta import MD5, Path, Size
from fpipe.meta.stream import Stream
from fpipe.workflow import WorkFlow


class TestLocal(TestCase):
    @staticmethod
    def __checksum(data: bytes):
        sig = hashlib.md5()
        sig.update(data)
        return sig.hexdigest()

    def test_local_passthrough(self):
        file_names = ('.x.test', '.y.test')
        try:
            workflow = WorkFlow(
                Local(pass_through=True),
                Meta(Size, MD5)
            )

            count = 0

            for f in workflow.compose(
                    ByteFile(b'x' * 2 ** 26, Path(file_names[0])),
                    ByteFile(b'y' * 2 ** 26, Path(file_names[1]))):
                content = f[Stream].read()
                with open(f[Path], 'rb') as local_file:
                    self.assertEqual(content, local_file.read())
                    count += 1
            self.assertEqual(count, 2)
        finally:
            for f_n in file_names:
                os.remove(f_n)

    def test_local(self):
        file_names = ('.x.test', '.y.test')
        append_to_file_name = '.dat'
        try:
            workflow = WorkFlow(
                Local(
                    process_meta=lambda x: Path(
                        x[Path] + append_to_file_name
                    )
                ),
                Meta(Size, MD5)
            )
            count = 0
            for f in workflow.compose(
                    ByteFile(
                        b'x' * 10,
                        Path(file_names[0])),
                    ByteFile(
                        b'y' * 20,
                        Path(file_names[1]))
            ):
                content = f[Stream].read()
                with open(
                        f[Path],
                        'rb') as local_file:
                    count += 1
                    self.assertEqual(content, local_file.read())

            workflow = WorkFlow(Local())

            for f in workflow.compose(
                    LocalFile(file_names[0] + append_to_file_name)):
                self.assertEqual(len(f[Stream].read()), 10)
                count += 1

            self.assertEqual(count, 3)
        finally:
            for f_n in file_names:
                try:
                    os.remove(f_n)
                except:
                    pass
                try:
                    os.remove(f_n + append_to_file_name)
                except:
                    pass

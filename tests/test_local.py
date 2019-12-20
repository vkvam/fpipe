import hashlib
import os

from unittest import TestCase

from fpipe.file import ByteFile, LocalFile
from fpipe.gen import MetaGen, LocalGen
from fpipe.meta import MD5Calculated, Path, SizeCalculated
from fpipe.workflow import WorkFlow


class TestMeta(TestCase):
    @staticmethod
    def __checksum(data: bytes):
        sig = hashlib.md5()
        sig.update(data)
        return sig.hexdigest()

    def test_local_passthrough(self):
        file_names = ('.x.test', '.y.test')
        try:
            workflow = WorkFlow(
                LocalGen(pass_through=True),
                MetaGen(SizeCalculated, MD5Calculated)
            )

            count = 0

            for f in workflow.compose(ByteFile(b'x' * 2**26, Path(file_names[0])),
                                      ByteFile(b'y' * 2**26, Path(file_names[1]))):
                content = f.file.read()
                with open(f.meta(Path).value, 'rb') as local_file:
                    self.assertEqual(content, local_file.read())
                    count += 1
            self.assertEqual(count, 2)
        finally:
            for f_n in file_names:
                os.remove(f_n)

    def test_local(self):
        file_names = ('.x.test', '.y.test')
        try:
            workflow = WorkFlow(
                LocalGen(),
                MetaGen(SizeCalculated, MD5Calculated)
            )
            count = 0
            for f in workflow.compose(ByteFile(b'x' * 10, Path(file_names[0])),
                                      ByteFile(b'y' * 20, Path(file_names[1]))):
                content = f.file.read()
                with open(f.meta(Path).value, 'rb') as local_file:
                    count += 1
                    self.assertEqual(content, local_file.read())

            workflow = WorkFlow(LocalGen())

            for f in workflow.compose(LocalFile(file_names[0])):
                self.assertEqual(len(f.file.read()), 10)
                count += 1

            self.assertEqual(count, 3)
        finally:
            for f_n in file_names:
                os.remove(f_n)

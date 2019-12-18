import hashlib

from unittest import TestCase

from fpipe.file import ByteFile
from fpipe.gen import FileInfoGenerator, LocalFileGenerator
from fpipe.meta import MD5Calculated, Path, SizeCalculated
from fpipe.workflow import WorkFlow


class TestMeta(TestCase):
    @staticmethod
    def __checksum(data: bytes):
        sig = hashlib.md5()
        sig.update(data)
        return sig.hexdigest()

    def test_local(self):
        workflow = WorkFlow(
            LocalFileGenerator(pass_through=True),
            FileInfoGenerator((SizeCalculated, MD5Calculated))
        )

        for f in workflow.start(ByteFile(b'x' * 10, Path('x')), ByteFile(b'y' * 20, Path('y'))):
            content = f.file.read()
            with open(f.meta(Path).value, 'rb') as local_file:
                self.assertEqual(content, local_file.read())

import hashlib

from unittest import TestCase

from fpipe.file import ByteFile, LocalFile
from fpipe.gen import FileInfoGenerator, LocalFileGenerator
from fpipe.meta import MD5Calculated, Path, SizeCalculated
from fpipe.workflow import WorkFlow


class TestMeta(TestCase):
    @staticmethod
    def __checksum(data: bytes):
        sig = hashlib.md5()
        sig.update(data)
        return sig.hexdigest()

    def test_local_passthrough(self):
        workflow = WorkFlow(
            LocalFileGenerator(pass_through=True),
            FileInfoGenerator((SizeCalculated, MD5Calculated))
        )

        count = 0

        for f in workflow.start(ByteFile(b'x' * 10, Path('x')), ByteFile(b'y' * 20, Path('y'))):
            content = f.file.read()
            with open(f.meta(Path).value, 'rb') as local_file:
                self.assertEqual(content, local_file.read())
                count += 1
        self.assertEqual(count, 2)

    def test_local(self):
        workflow = WorkFlow(
            LocalFileGenerator(),
            FileInfoGenerator((SizeCalculated, MD5Calculated))
        )
        count = 0
        for f in workflow.start(ByteFile(b'x' * 10, Path('x')), ByteFile(b'y' * 20, Path('y'))):
            content = f.file.read()
            with open(f.meta(Path).value, 'rb') as local_file:
                count += 1
                self.assertEqual(content, local_file.read())

        workflow = WorkFlow(LocalFileGenerator())

        for f in workflow.start(LocalFile('x')):
            self.assertEqual(len(f.file.read()), 10)
            count += 1

        self.assertEqual(count, 3)

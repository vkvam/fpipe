import hashlib

from unittest import TestCase

from fpipe.gen import ProcessFileGenerator, FileInfoGenerator
from fpipe.meta import MD5Calculated, Path,  SizeCalculated
from fpipe.exceptions import FileInfoException
from fpipe.workflow import WorkFlow
from test_utils.test_file import ReversibleTestFile, TestStream


class TestMeta(TestCase):
    @staticmethod
    def __checksum(data: bytes):
        sig = hashlib.md5()
        sig.update(data)
        return sig.hexdigest()

    def test_workflow(self):
        stream_sizes = [2 ** i for i in range(18, 22)]

        # Get expected results from FileInfoGenerators
        md5_of_files = [
            self.__checksum(ReversibleTestFile(s).read()) for s in stream_sizes
        ]

        md5_of_reversed_files = [
            self.__checksum(bytes(reversed(ReversibleTestFile(s).read()))) for s in stream_sizes
        ]

        # Get checksum for initial files
        workflow = WorkFlow(
            FileInfoGenerator([MD5Calculated, SizeCalculated]),
            ProcessFileGenerator("rev|tr -d '\n'"),
            FileInfoGenerator([MD5Calculated, SizeCalculated])
        )
        for f in workflow.start(TestStream(s, f'{s}', reversible=True) for s in stream_sizes):
            f.file.read(1)
            # Assert that we are not able to retrieve calculated data before files have been completely read
            with self.assertRaises(FileInfoException):
                x = f.meta(MD5Calculated).value

            with self.assertRaises(FileInfoException):
                x = f.meta(SizeCalculated).value
            f.file.read()

            # Assert that checksum created in two different ways are equal
            self.assertEqual(f.parent.parent.meta(MD5Calculated).value, md5_of_files.pop(0))
            self.assertEqual(f.meta(MD5Calculated).value, md5_of_reversed_files.pop(0))
            self.assertEqual(f.meta(Path).value, str(f.meta(SizeCalculated).value))
        # Assert that we've checked all files
        self.assertEqual(len(md5_of_files) + len(md5_of_reversed_files), 0)

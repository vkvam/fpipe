from unittest import TestCase

from fpipe.meta import SizeCalculated, MD5Calculated
from fpipe.gen import FileInfoGenerator, ProcessFileGenerator
from fpipe.exceptions import FileInfoException
from test_utils.test_file import TestStream


class TestFileIO(TestCase):
    def test_chaining_test_stream(self):
        size = 2 ** 28

        signal = False

        for file in FileInfoGenerator([SizeCalculated, MD5Calculated]).chain(
                ProcessFileGenerator(
                    "cat /dev/stdin"
                ).chain(
                        TestStream(
                            size,
                            'xyz'
                        )

                )
        ):

            with self.assertRaises(FileInfoException):
                x = file.meta(SizeCalculated).value

            with self.assertRaises(FileInfoException):
                x = file.meta(MD5Calculated).value

            while file.file.read(2 ** 20):
                ...

            self.assertEqual(file.meta(SizeCalculated).value, size)
            self.assertNotEqual(file.meta(MD5Calculated).value, '')
            signal = True
        self.assertTrue(signal)

from unittest import TestCase

from fpipe.file import File
from fpipe.meta import SizeCalculated, MD5Calculated
from fpipe.gen import MetaGen, ProcessGen
from fpipe.exceptions import FileInfoException
from test_utils.test_file import TestStream


class TestFileIO(TestCase):
    def test_no_std_in(self):
        size = 2**20
        chunk = 2**14
        count = 0
        for f in ProcessGen(f"head -c {size} /dev/random").chain(File()):
            read = f.file.read
            while True:
                b = read(chunk)
                if not b:
                    break
                count += len(b)
        self.assertEqual(size, count)

    def test_process(self):
        size = 2 ** 28

        signal = False

        for file in MetaGen(SizeCalculated, MD5Calculated).chain(
                ProcessGen(
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

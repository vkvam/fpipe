from unittest import TestCase

from fpipe.file import File
from fpipe.meta import Size, MD5
from fpipe.gen import Meta, Program
from fpipe.exceptions import FileMetaException
from fpipe.utils.const import PIPE_BUFFER_SIZE
from test_utils.test_file import TestStream


class TestProcess(TestCase):
    def test_no_std_in(self):
        size = 2**22
        chunk = PIPE_BUFFER_SIZE
        count = 0
        for f in Program(f"head -c {size} /dev/random").chain(File()):
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

        for file in Meta(Size, MD5).chain(
                Program(
                    "cat /dev/stdin"
                ).chain(
                    TestStream(
                        size,
                        'xyz'
                    )

                )
        ):

            with self.assertRaises(FileMetaException):
                x = file.meta(Size).value

            with self.assertRaises(FileMetaException):
                x = file.meta(MD5).value

            while file.file.read(PIPE_BUFFER_SIZE):
                ...

            self.assertEqual(file.meta(Size).value, size)
            self.assertNotEqual(file.meta(MD5).value, '')
            signal = True
        self.assertTrue(signal)

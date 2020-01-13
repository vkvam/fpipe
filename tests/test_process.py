from unittest import TestCase

from fpipe.file import File
from fpipe.meta import Size, MD5
from fpipe.gen import Meta, Program
from fpipe.exceptions import FileDataException
from fpipe.meta.stream import Stream
from fpipe.utils.const import PIPE_BUFFER_SIZE
from test_utils.test_file import TestStream


class TestProcess(TestCase):
    def test_no_std_in(self):
        size = 2**22
        chunk = PIPE_BUFFER_SIZE
        count = 0
        for f in Program(f"head -c {size} /dev/random").chain(File()):
            read = f[Stream].read
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

            with self.assertRaises(FileDataException):
                x = file[Size]

            with self.assertRaises(FileDataException):
                x = file[MD5]

            while file[Stream].read(PIPE_BUFFER_SIZE):
                ...

            self.assertEqual(file[Size], size)
            self.assertNotEqual(file[MD5], '')
            signal = True
        self.assertTrue(signal)

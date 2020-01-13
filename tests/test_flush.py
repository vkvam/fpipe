from copy import copy

from unittest import TestCase

from fpipe.file import File
from fpipe.gen import Meta
from fpipe.gen import Method
from fpipe.gen.flush import Flush
from fpipe.meta import Size
from fpipe.meta.stream import Stream
from fpipe.workflow import WorkFlow
from test_utils.test_file import TestStream


class TestFlush(TestCase):

    def test_flush_gen(self):
        stream_sizes = [2 ** i for i in range(18, 22)]
        stream_sizes_copy = copy(stream_sizes)

        def assert_file_properties(file_stream: File):
            _f = file_stream[Stream]
            self.assertTrue(_f.readable())
            self.assertTrue(_f.writable())

        def assert_file_sizes(file_stream: File):
            self.assertEqual(file_stream[Size], stream_sizes_copy.pop(0))

        workflow = WorkFlow(
            Meta(Size),
            Flush(),
            Method(assert_file_properties),
            Method(assert_file_sizes)
        )

        workflow.compose(TestStream(s, f'{s}', reversible=True) for s in stream_sizes).flush()

    def test_flush(self):
        stream_sizes = [2 ** i for i in range(18, 22)]
        stream_sizes_copy = copy(stream_sizes)

        def assert_file_properties(file_stream: File):
            _f = file_stream[Stream]
            self.assertTrue(_f.readable())
            self.assertTrue(_f.writable())

        workflow = WorkFlow(
            Meta(Size),
            Method(assert_file_properties)
        )

        for f in workflow.compose(TestStream(s, f'{s}', reversible=True) for s in stream_sizes).flush_iter():
            self.assertEqual(f[Size], stream_sizes_copy.pop(0))

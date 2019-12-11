from unittest import TestCase
from fpipe.fileinfo import FileInfoException, FileInfoGenerator, CalculatedFileMeta
from moto import mock_s3, mock_iam, mock_config
from fpipe.s3 import S3FileGenerator
from test_utils.test_file import TestStream, TestFileGenerator


class TestFileIO(TestCase):
    @mock_s3
    @mock_iam
    @mock_config
    def test_s3_chain(self):
        import boto3
        bucket = "get-testy"
        session = boto3.Session()
        client = session.client("s3")
        resource = session.resource("s3")
        client.create_bucket(Bucket=bucket)

        size = 2 ** 23

        test_stream = TestStream(
            size,
            'xyz.tar'
        )

        gen = S3FileGenerator(
            TestFileGenerator(
                (test_stream,)
            ).get_files(),
            client,
            resource,
            bucket=bucket
        )

        gen = FileInfoGenerator(gen.get_files(), CalculatedFileMeta)
        for f in gen.get_files():
            with self.assertRaises(FileInfoException):
                # It is not possible to retrieve size from S3FileGenerator before object has been written
                x = f.parent.meta.size
            with self.assertRaises(FileInfoException):
                # It is not possible to retrieve size from FileInfoGenerator before the complete stream has been read
                x = f.meta.size

            cnt = f.file.read()
            test_stream.file.reset()

            self.assertEqual(f.meta.size, size)
            self.assertEqual(f.parent.meta.size, size)
            self.assertEqual(cnt, test_stream.file.read())

            # CalculatedFileMeta does not have the modified property
            with self.assertRaises(AttributeError):
                x = f.meta.mod
            # But S3FileGenerator does
            x = f.parent.meta.modified


import datetime
from copy import copy
from unittest import TestCase
from fpipe.fileinfo import FileInfoException, FileInfoGenerator, CalculatedFileMeta
from moto import mock_s3, mock_iam, mock_config
from fpipe.s3 import S3FileGenerator, S3File, S3Prefix
from test_utils.test_file import TestStream, TestFileGenerator


class TestFileIO(TestCase):

    def __init_s3(self, bucket="aws"):
        import boto3
        session = boto3.Session()
        client = session.client("s3")
        resource = session.resource("s3")
        client.create_bucket(Bucket=bucket)
        return client, resource, bucket

    def __create_objects(self, client, bucket, all_files):
        for key, body in all_files:
            client.put_object(
                Bucket=bucket,
                Body=body,
                Key=key
            )

    @mock_s3
    @mock_iam
    @mock_config
    def test_s3_meta(self):
        client, resource, bucket = self.__init_s3()
        size = 2 ** 23

        test_stream = TestStream(
            size,
            'xyz'
        )

        gen = S3FileGenerator(
            TestFileGenerator(
                (test_stream,)
            ),
            client,
            resource,
            bucket=bucket
        )

        gen = FileInfoGenerator(gen, CalculatedFileMeta)
        for f in gen:
            with self.assertRaises(FileInfoException):
                # It is not possible to retrieve size from S3FileGenerator before object has been written
                x = f.parent.meta.size
            with self.assertRaises(FileInfoException):
                # It is not possible to retrieve size from FileInfoGenerator before the complete stream has been read
                x = f.meta.size

            cnt = f.file.read()
            test_stream.file.reset()

            self.assertEqual(f.meta.size, size)
            self.assertEqual(f.parent.meta.mime, 'application/octet-stream')

            self.assertEqual(f.parent.meta.size, size)
            self.assertEqual(cnt, test_stream.file.read())

            # CalculatedFileMeta does not have the modified property
            with self.assertRaises(AttributeError):
                x = f.meta.modified
            # But S3FileGenerator does
            self.assertIsInstance(f.parent.meta.modified, datetime.datetime)

    @mock_s3
    @mock_iam
    @mock_config
    def test_s3_get_prefix(self):
        client, resource, bucket = self.__init_s3()
        prefixes = ["a", "b"]

        all_files = [
            (f'{y}/{x}', y.encode('utf-8') * x)
            for y in prefixes
            for x in range(1, 10)

        ]
        all_files_copy = copy(all_files)

        self.__create_objects(client, bucket, all_files)
        gen = S3FileGenerator((S3Prefix(bucket, prefix) for prefix in prefixes), client, resource)
        for f in gen:
            source_key, source_body = all_files_copy.pop(0)
            self.assertEqual(f.file.read(), source_body)
        self.assertEqual(len(all_files_copy), 0)

    @mock_s3
    @mock_iam
    @mock_config
    def test_s3_key(self):
        client, resource, bucket = self.__init_s3()
        prefixes = ["a", "b"]

        all_files = [
            (f'{y}/{x}', y.encode('utf-8') * x)
            for y in prefixes
            for x in range(1, 10)

        ]

        self.__create_objects(client, bucket, all_files)
        gen = S3FileGenerator((S3File(bucket, key) for key, _ in all_files), client, resource)

        all_files_copy = copy(all_files)
        for f in gen:
            source_key, source_body = all_files_copy.pop(0)
            self.assertEqual(source_key, f.meta.key)
            self.assertEqual(f.file.read(), source_body)
        self.assertEqual(len(all_files_copy), 0)

    @mock_s3
    @mock_iam
    @mock_config
    def test_bucket_versioning(self):
        client, resource, bucket = self.__init_s3()
        client.put_bucket_versioning(
            Bucket=bucket,
            VersioningConfiguration={
                'Status': 'Enabled'
            }
        )

        sizes = [10, 20]

        test_streams = [
            TestStream(
                size,
                'xyz'
            ) for size in sizes]

        gen = S3FileGenerator(
            TestFileGenerator(
                test_streams
            ),
            client,
            resource,
            bucket=bucket
        )

        versions = [[f.meta.key, f.file.read() and f.meta.version] for f in gen]

        # Horrible hack since moto does not return VersionId for multipart uploads
        for idx, version in enumerate(resource.Bucket(bucket).object_versions.filter(Prefix='xyz')):
            obj = version.get()
            version = obj.get('VersionId')
            versions[idx][1] = version

        s3_files = [S3File(bucket, key, version) for key, version in versions]

        gen = S3FileGenerator(
            s3_files,
            client,
            resource,
            bucket=bucket
        )

        for f in gen:
            content = f.file.read()
            self.assertEqual(f.meta.version, versions.pop(0)[1])
            t_stream = test_streams.pop(0)
            t_stream.file.reset()
            self.assertEqual(content, t_stream.file.read())

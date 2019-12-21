import datetime
import io
import tarfile
from copy import copy, deepcopy
from queue import Queue
from threading import Event
from unittest import TestCase

from typing import IO, Iterable, List

import botocore
from boto.file import Key
from mock import patch, Mock

from fpipe.exceptions import SeekException, FileException, FileMetaException
from fpipe.gen import Meta, S3, Tar
from fpipe.file import S3File, S3PrefixFile
from fpipe.meta import Mime, Modified, Version, Path, Size

from moto import mock_s3, mock_iam, mock_config

from fpipe.meta.checksum import MD5
from fpipe.utils.s3_writer_worker import worker, CorruptedMultipartError
from fpipe.workflow import WorkFlow
from test_utils.test_file import TestStream


class TestS3(TestCase):

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
        size = 2 ** 20

        test_stream = TestStream(
            size,
            'xyz'
        )

        gen = S3(
            client,
            resource,
            bucket=bucket,
            seekable=False
        ).chain(
            test_stream
        )

        gen = Meta(Size, MD5).chain(gen)
        for f in gen:
            with self.assertRaises(FileMetaException):
                # It is not possible to retrieve size from S3FileGenerator before object has been written
                x = f.parent.meta(Size).value
            with self.assertRaises(FileMetaException):
                # It is not possible to retrieve size from FileMetaGenerator before the complete stream has been read
                x = f.meta(MD5).value
            cnt = f.file.read()
            test_stream.file.seek(0)
            size_calc = f.meta(Size).value
            self.assertEqual(size_calc, size)

            self.assertEqual(f.meta(Mime).value, 'application/octet-stream')
            self.assertEqual(f.meta(Size, 1).value, size)

            self.assertEqual(cnt, test_stream.file.read())
            self.assertIsInstance(f.parent.meta(Modified).value,
                                  datetime.datetime)
            self.assertIsInstance(f.meta(Modified).value, datetime.datetime)

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

        gen = S3(
            client,
            resource,
            seekable=False
        ).chain(
            S3PrefixFile(bucket, prefix) for prefix in prefixes
        )

        for f in gen:
            source_key, source_body = all_files_copy.pop(0)
            self.assertEqual(f.file.read(), source_body)
        self.assertEqual(len(all_files_copy), 0)

    @mock_s3
    @mock_iam
    @mock_config
    @patch('fpipe.utils.s3_writer_worker.checksum_validates')
    def test_s3_writer_worker_exception_checksum(self, mocked):
        mocked.return_value = False
        client, resource, bucket = self.__init_s3()
        key = 'huhu'
        arguments = {
            "Bucket": bucket,
            "Key": key,
            "ContentType": "application/octet-stream",
        }

        mpu = client.create_multipart_upload(**arguments)

        q = Queue()
        q.put((b'', 1))
        q.put((b'', 1))
        with self.assertRaises(CorruptedMultipartError):
            worker(client,
                   Event(), q, Queue(), Queue(), bucket, key, mpu["UploadId"],
                   1)

    @mock_s3
    @mock_iam
    @mock_config
    @patch('fpipe.utils.s3_writer_worker.checksum_validates')
    def test_s3_writer_worker_exception_botocore(self, mocked):
        from botocore import exceptions
        mocked.return_value = False
        parsed_response = {
            'Error': {'Code': '500', 'Message': 'Error Uploading'}}
        e = exceptions.ClientError(parsed_response, 'UploadPartCopy')

        mocked.side_effect = e
        client, resource, bucket = self.__init_s3()
        key = 'huhu'
        arguments = {
            "Bucket": bucket,
            "Key": key,
            "ContentType": "application/octet-stream",
        }

        mpu = client.create_multipart_upload(**arguments)

        q = Queue()
        q.put((b'', 1))
        q.put((b'', 1))
        with self.assertRaises(exceptions.ClientError):
            worker(client,
                   Event(), q, Queue(), Queue(), bucket, key, mpu["UploadId"],
                   1)

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
        gen = S3(
            client,
            resource,
            seekable=False
        ).chain((S3File(bucket, key) for key, _ in all_files))

        all_files_copy = copy(all_files)
        for f in gen:
            source_key, source_body = all_files_copy.pop(0)
            self.assertEqual(source_key, f.meta(Path).value)
            self.assertEqual(f.file.read(), source_body)
        self.assertEqual(len(all_files_copy), 0)

    @mock_s3
    @mock_iam
    @mock_config
    def test_s3_seek(self):
        client, resource, bucket = self.__init_s3()
        file_size = 2 ** 20
        test_stream = TestStream(file_size, 'xyz', reversible=True)
        test_file = deepcopy(test_stream.file)

        def seek(files: Iterable[IO[bytes]], n=None, whence=0):
            for file in files:
                file.seek(n, whence)

        def assert_file_content(files_content: List[bytes], length):
            self.assertEqual(*files_content)
            for c in files_content:
                self.assertEqual(len(c), length)

        signal = False
        for f in S3(client,
                    resource,
                    bucket=bucket,
                    seekable=True
                    ).chain(test_stream):
            s3_file = f.file

            extract_length = 13 + 42 + 69 + 101 + 404 + 420

            files = [s3_file, test_file]
            # Seek relative to end
            seek(files, extract_length, 2)

            # Seek relative to current position
            seek(files, -10, 1)
            seek(files, 10, 1)

            # Assert files have seeked identical by comparing them
            assert_file_content([f.read() for f in files], extract_length)

            seek(files, 0)

            # Assert files have seeked identical by comparing them
            assert_file_content([f.read(extract_length) for f in files],
                                extract_length)
            signal = True

        self.assertTrue(signal)

    @mock_s3
    @mock_iam
    @mock_config
    def test_exceptions(self):
        client, resource, bucket = self.__init_s3()
        with self.assertRaises(SeekException):
            for f in S3(client, resource, bucket=bucket).chain(
                    TestStream(1, 'xyz', reversible=True)):
                f.file.seek(0, 3)

        with self.assertRaises(FileException):
            for f in S3(client, resource).chain(
                    TestStream(1, 'xyz', reversible=True)):
                f.file.seek(0, 3)

        with self.assertRaises(FileException):
            for f in S3(client, resource).chain(S3File(bucket, 'x')):
                f.file.read(1)

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

        sizes = [2 ** i for i in range(20, 24)]

        test_streams = [
            TestStream(
                size,
                'xyz'
            ) for size in sizes
        ]

        gen = S3(
            client,
            resource,
            bucket=bucket,
            seekable=False
        ).chain(test_streams)

        # Note: f.meta(S3Version) will raise exception since moto does not give version for multiparts
        # versions = [[f.meta(Key), f.file.read() and f.meta(Version)] for f in gen]
        versions = [[f.file.read() and f.meta(Path).value, '?'] for f in gen]

        for idx, version in enumerate(
                resource.Bucket(bucket).object_versions.filter(Prefix='xyz')):
            obj = version.get()
            version = obj.get('VersionId')
            versions[idx][1] = version

        s3_files = [S3File(bucket, key, version) for key, version in versions]

        gen = S3(
            client,
            resource,
            bucket=bucket,
            seekable=False
        ).chain(s3_files)
        for f in gen:
            content = f.file.read()
            self.assertEqual(f.meta(Version).value, versions.pop(0)[1])
            t_stream = test_streams.pop(0)
            t_stream.file.seek(0)
            self.assertEqual(content, t_stream.file.read())
        self.assertEqual(len(test_streams), 0)

    def __create_tar(self, tar: tarfile.TarFile, path, size):
        content = b'z' * size
        f = io.BytesIO(content)
        tar_info = tarfile.TarInfo()
        tar_info.path = path
        tar_info.size = size
        tar.addfile(tar_info, f)
        return content

    @mock_s3
    @mock_iam
    @mock_config
    def test_readme_example(self):
        file = ('abc', 2 ** 14)
        f = io.BytesIO()

        with tarfile.open(fileobj=f, mode='w') as tar:
            source_content = self.__create_tar(tar, *file)
        f.seek(0)

        tar_content = f.read()

        bucket = 'bucket'
        tar_key = 'source.tar'

        client, resource, bucket = self.__init_s3(bucket)
        client.put_object(
            Bucket=bucket,
            Body=tar_content,
            Key=tar_key
        )

        # client = boto3.client('s3')
        # resource = boto3.resource('s3')
        bucket = 'bucket'
        key = 'source.tar'

        WorkFlow(
            S3(client, resource),
            Tar(),
            S3(
                client, resource, bucket=bucket,
                pathname_resolver=lambda x: f'MyPrefix/{x.meta(Path).value}'
            )
        ).compose(
            S3File(bucket, key)
        ).flush()

        o = client.get_object(Bucket=bucket, Key='MyPrefix/abc')
        self.assertEqual(o['Body'].read(), source_content)

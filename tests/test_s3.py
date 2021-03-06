import datetime
import io
import tarfile
from copy import copy, deepcopy
from queue import Queue
from threading import Event
from unittest import TestCase

from typing import IO, Iterable, List

from botocore import exceptions
from mock import patch

from fpipe.exceptions import SeekException, FileException, FileDataException
from fpipe.gen import Meta, S3, Tar
from fpipe.file import S3File, S3PrefixFile, ByteFile
from fpipe.meta import Mime, Modified, Version, Path, Size, Bucket

from moto import mock_s3, mock_iam, mock_config

from fpipe.meta.checksum import MD5
from fpipe.meta.stream import Stream
from fpipe.utils.const import PIPE_BUFFER_SIZE
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
            client.put_object(Bucket=bucket, Body=body, Key=key)

    @mock_s3
    @mock_iam
    @mock_config
    def test_s3(self):
        client, resource, bucket = self.__init_s3()
        size = 2 ** 20

        test_stream = TestStream(size, "xyz")

        gen = S3(client, resource, seekable=False,
                 process_meta=Bucket(bucket)).chain(
            test_stream
        )

        gen = Meta(Size, MD5).chain(gen)
        for f in gen:
            with self.assertRaises(FileDataException):
                # It is not possible to retrieve size from S3FileGenerator
                # before object has been written
                x = f.parent[Size]
            with self.assertRaises(FileDataException):
                # It is not possible to retrieve size from FileDataGenerator
                # before the complete stream has been read
                x = f[MD5]
            cnt = f[Stream].read()
            test_stream[Stream].seek(0)
            size_calc = f[Size]
            self.assertEqual(size_calc, size)

            self.assertEqual(f[Mime], "application/octet-stream")
            self.assertEqual(f[Size, 1], size)

            self.assertEqual(cnt, test_stream[Stream].read())
            self.assertIsInstance(f.parent[Modified],
                                  datetime.datetime)
            self.assertIsInstance(f[Modified], datetime.datetime)

    @mock_s3
    @mock_iam
    @mock_config
    def test_s3_get_prefix(self):
        client, resource, bucket = self.__init_s3()
        prefixes = ["a", "b"]

        all_files = [
            (f"{y}/{x}", y.encode("utf-8") * x) for y in prefixes for x in
            range(1, 10)
        ]
        all_files_copy = copy(all_files)

        self.__create_objects(client, bucket, all_files)

        gen = S3(client, resource, seekable=False).chain(
            S3PrefixFile(bucket, prefix) for prefix in prefixes
        )

        for f in gen:
            source_key, source_body = all_files_copy.pop(0)
            self.assertEqual(f[Stream].read(), source_body)
        self.assertEqual(len(all_files_copy), 0)

    @mock_s3
    @mock_iam
    @mock_config
    @patch("fpipe.utils.s3_writer_worker.checksum_validates")
    def test_s3_writer_worker_exception_checksum(self, mocked):
        mocked.return_value = False
        client, resource, bucket = self.__init_s3()
        key = "huhu"
        arguments = {
            "Bucket": bucket,
            "Key": key,
            "ContentType": "application/octet-stream",
        }

        mpu = client.create_multipart_upload(**arguments)

        q = Queue()
        q.put((b"", 1))
        q.put((b"", 1))
        with self.assertRaises(CorruptedMultipartError):
            worker(
                client, Event(), q, Queue(), Queue(), bucket, key,
                mpu["UploadId"], 1
            )

    @mock_s3
    @mock_iam
    @mock_config
    @patch(
        "fpipe.utils.s3_writer.S3FileWriter._S3FileWriter__finalize_multipart")
    def test_s3_writer_worker_exception_checksum_2(self, mocked):
        parsed_response = {
            "Error": {"Code": "500", "Message": "Error Uploading"}}
        e = exceptions.ClientError(parsed_response, "UploadPartCopy")
        mocked.side_effect = e

        client, resource, bucket = self.__init_s3()
        test_stream = TestStream(60, "xyz", reversible=True)

        # TODO: Should really raise S3WriteException
        with self.assertRaises(FileException):
            S3(
                client, resource, seekable=True, process_meta=Bucket(bucket)
            ).chain(test_stream).flush()

    @mock_s3
    @mock_iam
    @mock_config
    @patch("fpipe.utils.s3_writer_worker.checksum_validates")
    def test_s3_writer_worker_exception_botocore(self, mocked):
        from botocore import exceptions

        mocked.return_value = False
        parsed_response = {
            "Error": {"Code": "500", "Message": "Error Uploading"}}
        e = exceptions.ClientError(parsed_response, "UploadPartCopy")

        mocked.side_effect = e
        client, resource, bucket = self.__init_s3()
        key = "huhu"
        arguments = {
            "Bucket": bucket,
            "Key": key,
            "ContentType": "application/octet-stream",
        }

        mpu = client.create_multipart_upload(**arguments)

        q = Queue()
        q.put((b"", 1))
        q.put((b"", 1))
        with self.assertRaises(exceptions.ClientError):
            worker(
                client, Event(), q, Queue(), Queue(), bucket, key,
                mpu["UploadId"], 1
            )

    @mock_s3
    @mock_iam
    @mock_config
    def test_s3_key(self):
        client, resource, bucket = self.__init_s3()
        prefixes = ["a", "b"]

        all_files = [
            (f"{y}/{x}", y.encode("utf-8") * x) for y in prefixes for x in
            range(1, 10)
        ]

        self.__create_objects(client, bucket, all_files)
        gen = S3(client, resource, seekable=False).chain(
            S3File(bucket, key) for key, _ in all_files
        )

        all_files_copy = copy(all_files)
        for f in gen:
            source_key, source_body = all_files_copy.pop(0)
            self.assertEqual(source_key, f[Path])
            self.assertEqual(f[Stream].read(), source_body)
        self.assertEqual(len(all_files_copy), 0)

    @mock_s3
    @mock_iam
    @mock_config
    def test_s3_seek(self):
        client, resource, bucket = self.__init_s3()
        file_size = 2 ** 20
        test_stream = TestStream(file_size, "xyz", reversible=True)
        test_file = deepcopy(test_stream[Stream])

        def seek(files: Iterable[IO[bytes]], n=None, whence=0):
            for file in files:
                file.seek(n, whence)

        def assert_file_content(files_content: List[bytes], length):
            self.assertEqual(*files_content)
            for c in files_content:
                self.assertEqual(len(c), length)

        signal = False
        for f in S3(client, resource, seekable=True,
                    process_meta=Bucket(bucket)).chain(
            test_stream
        ):
            s3_file = f[Stream]

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
        # Assert we can not seek
        with self.assertRaises(SeekException):
            for f in S3(client, resource, process_meta=Bucket(bucket)).chain(
                    TestStream(1, "xyz", reversible=True)
            ):
                f[Stream].seek(0, 3)

        # Assert we can not evaluate bucket
        with self.assertRaises(FileDataException):
            for f in S3(client, resource).chain(
                    TestStream(1, "xyz", reversible=True)):
                f[Stream].seek(0, 3)

        with self.assertRaises(FileException):
            for f in S3(client, resource).chain(S3File(bucket, "x")):
                f[Stream].read(1)

    @mock_s3
    @mock_iam
    @mock_config
    def test_seek(self):
        client, resource, bucket = self.__init_s3()
        key = "key"
        picked_nr = 3
        been_there = False
        for f in (
                S3(client, resource, seekable=True,
                   process_meta=Bucket(bucket))
                        .chain(
                    ByteFile(b"0123456789" * (10 ** 7), meta=Path(key)))
                        .flush_iter()
        ):
            file = f[Stream]
            if file.seekable() and not file.writable() and not file.closed:
                for start in (0, 10 ** 6, 10 ** 5 + 100, 10 ** 2 + 100):
                    file.seek(start + picked_nr)
                    self.assertEqual(file.tell(), start + picked_nr)
                    self.assertEqual(int(file.read(1)), picked_nr)
                    been_there = True
        self.assertTrue(been_there)

    @mock_s3
    @mock_iam
    @mock_config
    def test_seek(self):
        client, resource, bucket = self.__init_s3()
        key = "key"
        picked_nr = 3
        been_there = False
        for f in S3(client, resource, seekable=True,
                    process_meta=Bucket(bucket)).chain(
            ByteFile(b"0123456789" * (10 ** 7), meta=Path(key))
        ).flush_iter():
            file = f[Stream]
            if file.seekable() and not file.writable() and not file.closed:
                for start in (0, 10 ** 6, 10 ** 5 + 100, 10 ** 2 + 100):
                    with self.assertRaises(SeekException):
                        file.seek(0, 4)
                    file.seek(start + picked_nr)
                    self.assertEqual(file.tell(), start + picked_nr)
                    self.assertEqual(int(file.read(1)), picked_nr)
                    been_there = True
        self.assertTrue(been_there)

    @mock_s3
    @mock_iam
    @mock_config
    def test_bucket_versioning(self):
        client, resource, bucket = self.__init_s3()
        client.put_bucket_versioning(
            Bucket=bucket, VersioningConfiguration={"Status": "Enabled"}
        )

        sizes = [2 ** i for i in range(20, 24)]

        test_streams = [TestStream(size, "xyz") for size in sizes]

        gen = S3(client, resource, process_meta=Bucket(bucket)).chain(
            test_streams
        )

        # Note: f[S3Version] will raise exception since moto
        # does not give version for multiparts
        versions = [[f[Stream].read() and f[Path], "?"] for f in gen]

        for idx, version in enumerate(
                resource.Bucket(bucket).object_versions.filter(Prefix="xyz")
        ):
            obj = version.get()
            version = obj.get("VersionId")
            versions[idx][1] = version

        s3_files = [S3File(bucket, key, version) for key, version in versions]

        gen = S3(client, resource, seekable=False).chain(s3_files)
        for f in gen:
            content = f[Stream].read()
            self.assertEqual(f[Version], versions.pop(0)[1])
            t_stream = test_streams.pop(0)
            t_stream[Stream].seek(0)
            self.assertEqual(content, t_stream[Stream].read())
        self.assertEqual(len(test_streams), 0)

    def __create_tar(self, tar: tarfile.TarFile, path, size):
        content = b"z" * size
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
        file = ("abc", PIPE_BUFFER_SIZE)
        f = io.BytesIO()

        with tarfile.open(fileobj=f, mode="w") as tar:
            source_content = self.__create_tar(tar, *file)
        f.seek(0)

        tar_content = f.read()

        bucket = "bucket"
        tar_key = "source.tar"

        client, resource, bucket = self.__init_s3(bucket)
        client.put_object(Bucket=bucket, Body=tar_content, Key=tar_key)

        key = "source.tar"

        WorkFlow(
            S3(client, resource),
            Tar(),
            S3(
                client,
                resource,
                process_meta=(
                    lambda x: Path(f"MyPrefix/{x[Path]}"),
                ),
            ),
        ).compose(S3File(bucket, key)).flush()

        o = client.get_object(Bucket=bucket, Key="MyPrefix/abc")
        self.assertEqual(o["Body"].read(), source_content)

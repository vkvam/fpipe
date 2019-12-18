import threading
from concurrent.futures import ThreadPoolExecutor
from queue import Queue
from typing import IO, AnyStr, List, Optional, Iterable, Iterator

from botocore.client import BaseClient

from fpipe.utils.part_buffer import Buffer
from fpipe.utils.s3_writer_worker import S3FileProgress, worker


class S3FileWriter(IO[bytes], ThreadPoolExecutor):
    MIN_BLOCK_SIZE = 5 * 2 ** 20

    def __init__(self,
                 s3_client: BaseClient,  # Most suitable type found in boto3/botocore
                 bucket: str,
                 key: str,
                 mime: str,
                 block_size: int = MIN_BLOCK_SIZE,
                 full_control: str = None,
                 worker_limit: int = 4,
                 progress_queue: Optional[Queue] = None,
                 max_part_upload_retries: int = 10,
                 queue_timeout=300):
        """

        :param s3_client: S3 client created with boto3.client('s3')
        :param bucket: S3 bucket name
        :param key: S3 key name
        :param mime: mime type of object
        :param block_size: block size lower limit of 5MB
        :param full_control: String to set full object control to other than the uploading user/account
        :param worker_limit: limit for parallel uploads
        :param progress_queue: a queue providing upload status feedback
        :param max_part_upload_retries: max retries if a part fails uploading
        :type queue_timeout: Timeout for adding new data to queue
        """
        super().__init__()
        if mime is None:
            raise Exception("Mime type must be set")
        block_size = max(self.MIN_BLOCK_SIZE, block_size)  # Block size must be over 5MB (aws rule)
        self.client = s3_client
        self.bucket = bucket
        self.key = key
        self.mime = mime
        self.buffer = Buffer(block_size)
        self.full_control = full_control
        self.progress_queue = progress_queue if progress_queue else Queue()

        self.stop_workers_request = threading.Event()
        self.work_queue = Queue(maxsize=worker_limit)
        self.result_queue = Queue()
        self.worker_limit = worker_limit
        self.max_part_upload_retries = max_part_upload_retries
        self.queue_timeout = queue_timeout
        self.mpu_res = None

    def __enter__(self) -> 'S3FileWriter':
        self.buffer.clear()

        arguments = {
            'Bucket': self.bucket, 'Key': self.key, 'ContentType': self.mime
        }
        if self.full_control:
            arguments['GrantFullControl'] = self.full_control

        self.mpu = self.client.create_multipart_upload(**arguments)
        self.progress_queue.put("Created multipart upload")

        for i in range(self.worker_limit):
            self.submit(worker,
                        self.client,
                        self.stop_workers_request,
                        self.work_queue,
                        self.result_queue,
                        self.progress_queue,
                        self.bucket,
                        self.key,
                        self.mpu['UploadId'],
                        self.max_part_upload_retries)

        return self

    def write(self, b: bytes):
        self.buffer.add(b)

        if self.buffer.full():
            self.work_queue.put(self.buffer.get(), timeout=self.queue_timeout)

    def __exit__(self, *args, **xargs):
        return self.close()

    def close(self):
        if not self.buffer.empty():
            self.work_queue.put(self.buffer.get(), timeout=self.queue_timeout)

        self.stop_workers_request.set()
        self.work_queue.join()

        results = []

        while not self.result_queue.empty():
            results.append(self.result_queue.get_nowait())

        results.sort(key=lambda x: x['PartNumber'])
        self.mpu_res = self.client.complete_multipart_upload(UploadId=self.mpu['UploadId'],
                                                             MultipartUpload={'Parts': results},
                                                             Bucket=self.bucket,
                                                             Key=self.key)

        self.progress_queue.put(S3FileProgress("Multipart", "Multipart upload complete"))
        return True

    # TODO: Fix all under
    def readable(self) -> bool:
        return False

    def fileno(self) -> int:
        raise NotImplementedError

    def flush(self) -> None:
        raise NotImplementedError

    def isatty(self) -> bool:
        raise NotImplementedError

    def read(self, n: int = ...) -> AnyStr:
        raise NotImplementedError

    def readline(self, limit: int = ...) -> AnyStr:
        raise NotImplementedError

    def readlines(self, hint: int = ...) -> List[AnyStr]:
        raise NotImplementedError

    def seek(self, offset: int, whence: int = ...) -> int:
        raise NotImplementedError

    def seekable(self) -> bool:
        raise NotImplementedError

    def tell(self) -> int:
        raise NotImplementedError

    def truncate(self, size: Optional[int] = ...) -> int:
        raise NotImplementedError

    def writable(self) -> bool:
        return True

    def writelines(self, lines: Iterable[AnyStr]) -> None:
        raise NotImplementedError

    def __next__(self) -> AnyStr:
        raise NotImplementedError

    def __iter__(self) -> Iterator[AnyStr]:
        raise NotImplementedError

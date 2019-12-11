import hashlib
import queue
import threading
from concurrent.futures import ThreadPoolExecutor

import gc

DEFAULT_BLOCK_SIZE = 8 * 2 ** 20


class S3FileWriter(ThreadPoolExecutor):
    def __init__(self,
                 s3_client,
                 bucket,
                 key,
                 block_size=DEFAULT_BLOCK_SIZE,
                 full_control=None,
                 worker_limit=4,
                 mime=None,
                 progress_listener_queue=None):
        super().__init__()
        if mime is None:
            raise Exception("Mime type must be set")
        block_size = max(2 ** 20 * 5, block_size)  # Block size must be over 5MB (aws rule)
        self.client = s3_client
        self.bucket = bucket
        self.key = key
        self.mime = mime
        self.buffer = Buffer(block_size)
        self.full_control = full_control
        self.progress_listener_queue = progress_listener_queue if progress_listener_queue else queue.Queue()

        self.stop_workers_request = threading.Event()
        self.work_queue = queue.Queue(maxsize=worker_limit)
        self.result_queue = queue.Queue()
        self.worker_limit = worker_limit

        self.mpu_res = None

    def __enter__(self):
        self.buffer.reset()

        arguments = {
            'Bucket': self.bucket, 'Key': self.key, 'ContentType': self.mime
        }
        if self.full_control:
            arguments['GrantFullControl'] = self.full_control

        self.mpu = self.client.create_multipart_upload(**arguments)
        self.progress_listener_queue.put("Created multipart upload")

        for i in range(self.worker_limit):
            self.submit(S3FileWriter.worker,
                        self.client,
                        self.stop_workers_request,
                        self.work_queue,
                        self.result_queue,
                        self.progress_listener_queue,
                        self.bucket,
                        self.key,
                        self.mpu['UploadId'])

        return self

    def write(self, body):
        self.buffer.add(body)

        if self.buffer.is_full():
            self.work_queue.put(self.buffer.get(), timeout=360)

    def __exit__(self, *args, **xargs):
        return self.close()

    def close(self):
        if not self.buffer.is_empty():
            self.work_queue.put(self.buffer.get(), timeout=360)

        self.stop_workers_request.set()

        results = []

        while self.work_queue.unfinished_tasks > 0 or self.result_queue.unfinished_tasks > 0:
            try:
                res = self.result_queue.get(timeout=1)
                results.append(res)
                self.result_queue.task_done()
            except queue.Empty:
                pass
        results.sort(key=lambda x: x['PartNumber'])
        self.mpu_res = self.client.complete_multipart_upload(UploadId=self.mpu['UploadId'],
                                                             MultipartUpload={'Parts': results},
                                                             Bucket=self.bucket,
                                                             Key=self.key)

        self.progress_listener_queue.put(S3FileProgress("Multipart", "Multipart upload complete"))
        return True

    @staticmethod
    def worker(client, stoprequest, work_queue, result_queue, progress_queue, bucket, key, upload_id):
        retries = 0

        while not stoprequest.isSet() or work_queue.unfinished_tasks:
            try:
                data, part_index = work_queue.get(timeout=0.1)
            except queue.Empty:
                continue

            try:

                part = client.upload_part(
                    PartNumber=part_index,
                    Body=data,
                    Bucket=bucket,
                    Key=key,
                    UploadId=upload_id
                )

                progress_queue.put(S3FileProgress("GC count", gc.get_count()))
                hash_md5 = hashlib.md5()
                hash_md5.update(data)
                ftp_etag = hash_md5.hexdigest()

                s3_etag = part['ETag']

                if ftp_etag != s3_etag[1:-1]:
                    progress = S3FileProgress("Part nr {} upload".format(part_index),
                                              "Incorrect checksum {}!={}".format(ftp_etag, s3_etag))
                    progress_queue.put(progress)
                    raise Exception(progress)
                else:
                    progress_queue.put(
                        S3FileProgress("Part nr {} upload".format(part_index), 'ok [{}]'.format(len(data)))
                    )

                result_queue.put({
                    'PartNumber': part_index,
                    'ETag': s3_etag
                })
                work_queue.task_done()
                gc.collect()  # HACK: Memory leaks for some reason, this solves it.
            except:
                if retries < 10:
                    work_queue.put((data, part_index))
                    retries += 1
                else:
                    raise


class Buffer(object):
    def __init__(self, block_size, str_encoding='utf-8'):
        self.buffer = bytearray()
        self.part_number = 1
        self.chunk_size = block_size
        self.str_encoding = str_encoding

    def add(self, data):
        if isinstance(data, bytes) or isinstance(data, bytearray):
            self.buffer += data
        elif isinstance(data, str):
            self.buffer += data.encode(self.str_encoding)
        else:
            raise TypeError(f"{type(data)} can not be stored in S3 object")

    def is_empty(self):
        return len(self.buffer) == 0

    def is_full(self):
        return len(self.buffer) >= self.chunk_size

    def get(self):
        try:
            return self.buffer[:self.chunk_size], self.part_number
        finally:
            del self.buffer[:self.chunk_size]
            self.part_number += 1

    def reset(self):
        del self.buffer[:]
        self.part_number = 1


class S3FileProgress(object):
    def __init__(self, name, data):
        self.name = name
        self.data = data

    def __str__(self):
        return "{}: {}".format(self.name, self.data)

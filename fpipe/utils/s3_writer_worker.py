import hashlib
from queue import Empty, Queue
from threading import Event

import gc
from botocore import exceptions
from botocore.client import BaseClient


class CorruptedMultipartError(Exception):
    pass


class S3FileProgress(object):
    def __init__(self, name, data):
        self.name = name
        self.data = data

    def __str__(self):
        return "{}: {}".format(self.name, self.data)


def checksum_validates(data_etag, s3_etag):
    return data_etag == s3_etag[1:-1]


def worker(
        client: BaseClient,
        stop_workers_request: Event,
        work_queue: Queue,
        result_queue: Queue,
        progress_queue: Queue,
        bucket: str,
        key: str,
        upload_id: str,
        max_retries: int,
):
    retries = 0

    while not stop_workers_request.is_set() or work_queue.qsize() > 0:
        try:
            data, part_index = work_queue.get(timeout=0.05)
        except Empty:
            continue

        try:

            part = client.upload_part(
                PartNumber=part_index,
                Body=data,
                Bucket=bucket,
                Key=key,
                UploadId=upload_id,
            )

            hash_md5 = hashlib.md5()
            hash_md5.update(data)
            data_etag = hash_md5.hexdigest()

            s3_etag = part["ETag"]

            if not checksum_validates(data_etag, s3_etag):
                progress = S3FileProgress(
                    "Part nr {} upload".format(part_index),
                    "Incorrect checksum {}!={}".format(data_etag, s3_etag),
                )
                progress_queue.put(progress)
                raise CorruptedMultipartError(progress)
            else:
                progress_queue.put(
                    S3FileProgress(
                        "Part nr {} upload".format(part_index),
                        "ok [{}]".format(len(data)),
                    )
                )
            work_queue.task_done()
            result_queue.put({"PartNumber": part_index, "ETag": s3_etag})

            gc.collect()  # boto3 has memory leaks for some reason
        except (CorruptedMultipartError, exceptions.ClientError):
            if retries < max_retries:
                work_queue.put((data, part_index))
                retries += 1
            else:
                raise

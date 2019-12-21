class SeekException(Exception):
    pass


class FileException(Exception):
    pass


class S3WriteException(Exception):
    pass


class FileMetaException(Exception):
    def __init__(self, obj: object):
        super().__init__(f"Could not return {obj.__class__.__name__}")

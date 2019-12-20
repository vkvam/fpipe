

class SeekException(Exception):
    pass


class FileException(Exception):
    pass


class S3WriteException(Exception):
    pass


class FileInfoException(Exception):
    def __init__(self, obj: 'FileMetaCalculator'):
        super().__init__(f"Can not return {obj.__class__.__name__} before file has been completely read")

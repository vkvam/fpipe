from typing import Type, Union


class SeekException(Exception):
    pass


class FileException(Exception):
    pass


class S3WriteException(Exception):
    pass


class FileMetaException(Exception):
    def __init__(self, obj: Union[Type, object]):
        if isinstance(obj, type):
            super().__init__(f"Could not return metadata with type: "
                             f"{obj.__name__}")
        else:
            super().__init__(f"Could not return metadata with type: "
                             f"{obj.__class__.__name__}")

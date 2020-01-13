from typing import Optional, Type, Iterable, Union, BinaryIO, Tuple

from fpipe.exceptions import FileDataException
from fpipe.meta.abstract import FileData, MetaMap, T
from fpipe.meta.stream import Stream


class File:
    """container for all FileData related to a file
    """

    def __init__(
            self,
            stream: Optional[BinaryIO] = None,
            parent: Optional['File'] = None,
            meta: Optional[Union[FileData, Iterable[FileData]]] = None
    ):
        self.parent = parent
        self.meta_map = MetaMap()
        meta = [meta] if isinstance(meta, FileData) else meta
        if meta:
            for m in meta:
                self.meta_map.set(m)
        if stream:
            self.meta_map.set(Stream(stream))

    def __getitem__(self,
                    item: Union[
                        Type[FileData[T]],
                        Tuple[Type[FileData[T]], int]
                    ]) -> T:
        """

        :param item: the type of FileData to retrieve, the optional int
        fetches the nth FileData found in any of the parent File objects
        as opposed to the first

        :return: The first or nth value from FileData object
        """
        nth: Optional[int]
        if isinstance(item, tuple):
            data_type, nth = item
        else:
            data_type = item
            nth = None

        obj: Optional[File] = self
        count = 0
        while obj is not None:
            try:
                if nth is None or nth == 0:
                    return obj.meta_map[data_type].value
                else:

                    if count == nth:
                        return obj.meta_map[data_type].value
                    if data_type in obj.meta_map:
                        count += 1
                    obj = obj.parent
            except KeyError:
                if obj is None:
                    break
                obj = obj.parent
        raise FileDataException(data_type)

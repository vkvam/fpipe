import tarfile
from typing import Generator, Iterable, cast

from .abstract import File, FileMeta, Stream, FileStreamGenerator


class TarFileInfo(FileMeta):
    def __init__(self, tar_info):
        super().__init__()
        self.path = tar_info.path
        self.chksum = tar_info.chksum
        self.chksum = tar_info.chksum
        self.create_gnu_header = tar_info.create_gnu_header
        self.create_pax_global_header = tar_info.create_pax_global_header
        self.create_pax_header = tar_info.create_pax_header
        self.create_ustar_header = tar_info.create_ustar_header
        self.devmajor = tar_info.devmajor
        self.devminor = tar_info.devminor
        self.frombuf = tar_info.frombuf
        self.fromtarfile = tar_info.fromtarfile
        self.get_info = tar_info.get_info
        self.gid = tar_info.gid
        self.gname = tar_info.gname
        self.isblk = tar_info.isblk
        self.ischr = tar_info.ischr
        self.isdev = tar_info.isdev
        self.isdir = tar_info.isdir
        self.isfifo = tar_info.isfifo
        self.isfile = tar_info.isfile
        self.islnk = tar_info.islnk
        self.isreg = tar_info.isreg
        self.issparse = tar_info.issparse
        self.issym = tar_info.issym
        self.linkname = tar_info.linkname
        self.linkpath = tar_info.linkpath
        self.mode = tar_info.mode
        self.mtime = tar_info.mtime
        self.name = tar_info.name
        self.offset = tar_info.offset
        self.offset_data = tar_info.offset_data
        self.pax_headers = tar_info.pax_headers
        self.size = tar_info.size
        self.sparse = tar_info.sparse
        self.tobuf = tar_info.tobuf
        self.uid = tar_info.uid
        self.uname = tar_info.uname


class TarFile(File):
    def __init__(self, tar_info: tarfile.TarInfo):
        super().__init__(TarFileInfo(tar_info))


class TarStream(Stream):
    def __init__(self, file, meta: TarFileInfo):
        super().__init__(file, meta)

    @property
    def meta(self) -> TarFileInfo:
        return cast(TarFileInfo, super().meta)


class TarFileGenerator(FileStreamGenerator):
    def __init__(self, files: Iterable[Stream]):
        super().__init__(files)

    def __iter__(self) -> Iterable[TarStream]:
        for source in self.files:
            try:
                with tarfile.open(fileobj=source.file, mode='r|*') as tar_content_stream:
                    for tar_info in tar_content_stream:
                        tar_stream = tar_content_stream.extractfile(tar_info)
                        yield TarStream(tar_stream, TarFileInfo(tar_info))

            except (FileNotFoundError, tarfile.TarError):
                raise

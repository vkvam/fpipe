from .ftp import FTPFileGenerator, FTPFile
from .process import ProcessFileGenerator
from .local import LocalFile, LocalFileGenerator
from .s3 import S3FileGenerator, S3File, S3PrefixFile, S3SeekableFileStream, S3FileInfo
from .tar import TarFileGenerator, TarFile, TarFileStream, TarFileInfo

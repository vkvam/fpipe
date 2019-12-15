from .ftp import FTPFileGenerator, FTPFile
from .process import ProcessFileGenerator
from .local import LocalFile, LocalFileGenerator
from fpipe.generators.s3 import S3FileGenerator, S3File, S3PrefixFile, S3SeekableFileStream, S3FileInfo, S3Version, \
    S3Key, S3Modified, S3Size, S3Mime
from .tar import TarFileGenerator, TarFile, TarFileStream, TarFileInfo
from .fileinfo import FileInfoGenerator, FileMetaCalculator, FileInfoException

import datetime

from fpipe.meta.abstract import FileMetaFuture


class Modified(FileMetaFuture[datetime.datetime]):
    pass

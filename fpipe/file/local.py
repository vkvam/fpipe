from fpipe.file import File
from fpipe.meta import Path


class LocalFile(File):
    def __init__(self, path):
        super().__init__(meta=[Path(path)])

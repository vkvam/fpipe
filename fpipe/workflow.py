from typing import Iterable, List, Union

from fpipe.file import File
from fpipe.gen.abstract import FileGenerator


class WorkFlow:
    def __init__(self, *generators: FileGenerator):
        self.generators: List[FileGenerator] = [*generators]

    def start(self, *source: Union[File, Iterable[File]]):
        last_gen = self.generators[0]
        last_gen.reset()
        for s in source:
            last_gen.chain(s)

        for gen in self.generators[1:]:
            if last_gen:
                gen.reset()
                gen.chain(last_gen)
            last_gen = gen
        return last_gen

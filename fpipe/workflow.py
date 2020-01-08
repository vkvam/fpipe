from typing import Iterable, List, Union, Optional

from fpipe.file import File
from fpipe.gen.generator import FileGenerator


class WorkFlow:
    def __init__(self, first_gen: FileGenerator, *generators: FileGenerator):
        self.first_gen: FileGenerator = first_gen
        self.generators: List[FileGenerator] = [*generators]
        self.last_gen: Optional[FileGenerator] = None

    def compose(self, *source: Union[File, Iterable[File]]) -> FileGenerator:
        """
        Sets up the workflow, but will not process anything before returned
        files are read from.

        :param source: A collection of File or generators of File to run the
        workflow with
        :return: the output FileGenerator of the workflow
        """

        previous_gen = self.first_gen

        previous_gen.reset()
        for s in source:
            previous_gen.chain(s)

        for gen in self.generators:
            if previous_gen:
                gen.reset()
                gen.chain(previous_gen)
            previous_gen = gen
        self.last_gen = previous_gen
        return previous_gen

# fpipe

Framework for processing file-likes through chained pipes

## Installing


Designed for python 3.7, for S3 support you need boto3

```
brew install python3
pip3 install fpipe boto3
```

### Getting started

Example that reads a stream, calculates some file info, writes file to disk and prints the original stream to stdout
```
import io
from fpipe.abstract import Stream, FileMeta
from fpipe.fileinfo import FileInfoGenerator, CalculatedFileMeta
from fpipe.local import LocalFileGenerator

example_streams = (
    Stream(
        io.BytesIO(bytes(f'{name}') * 2 ** 15), FileMeta(f'{name}.file')
    )
    for name in ('x', 'y', 'z')
)

gen = FileInfoGenerator(example_streams, CalculatedFileMeta)
gen = LocalFileGenerator(gen.get_files(), pass_through=True)
for f in gen.get_files():
    while True:
        b = f.file.read(4)
        print(b.decode('utf-8'), end='')
        if not b:
            break
    print(f.parent.meta.checksum_md5)
```

See unittests for more examples

## Run tests

To run tests install tox with pip, go to project root and run `tox`
```
pip3 install tox
tox
```

## Built With

* python3.7
* travis
* tox

## Contributing

Bug-reports and pull requests on github  

## Versioning

https://pypi.org/project/fpipe/#history

## License

This project is licensed under the MIT License - see the [LICENSE.txt](LICENSE.txt) file for details
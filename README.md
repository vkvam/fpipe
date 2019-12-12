# fpipe

Framework for processing file-likes through chained pipes

## Installing


Designed for python 3.7, for S3 support you need boto3

```bash
brew install python3
pip3 install fpipe
# Optional
pip3 insstall boto3
```

### Getting started

Example that reads a stream, calculates some file info, writes file to disk and prints the original stream to stdout
```python
import io
from fpipe.abstract import Stream, FileMeta
from fpipe.fileinfo import FileInfoGenerator, CalculatedFileMeta
from fpipe.local import LocalFileGenerator

example_streams = (
    Stream(
        io.BytesIO(
            bytes(f'{name}', encoding='utf-8') * 2 ** 6
        ),
        FileMeta(f'{name}.file')
    )
    for name in ('x', 'y', 'z')
)
gen = FileInfoGenerator(example_streams, CalculatedFileMeta)
for f in LocalFileGenerator(gen, pass_through=True, pathname_resolver=lambda x: x.parent.meta.path):
    print(f"Name: {f.parent.parent.meta.path}")
    while True:
        b = f.file.read(2)
        print(b.decode('utf-8'), end='')
        if not b:
            break
    print(f"\nChecksum: {f.parent.meta.checksum_md5}\n")
```

See unittests for more examples

## Run tests and verify pypi compatibility 

To run tests install tox and twine with pip, go to project root and run tox
```bash
# python3 -m venv .venv
# Activate virtualenv
source .venv/bin/activate
# Run tests
tox
# Build distribution
python setup.py sdist bdist_wheel
# Validate distribution
twine check dist/*
```


## Built With

* travis
* tox

## Contributing

Bug-reports and pull requests on github  

## Versioning

https://pypi.org/project/fpipe/#history

## License

This project is licensed under the MIT License - see the [LICENSE.txt](LICENSE.txt) file for details
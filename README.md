# fPipe

python framework for data manipulation and metadata extraction built around the python file-like api.


### Installing


Designed for python 3.7, for S3 support you need boto3

```bash
brew install python3
pip3 install fpipe
# Optional
pip3 install boto3
```

### Getting started

Example that reads a stream, calculates some file info, writes file to disk and prints the original stream to stdout
```python
#!/usr/bin/env python3
import io
from fpipe import FileStream, FileMeta
from fpipe.fileinfo import FileInfoGenerator, CalculatedFileMeta
from fpipe.generators import LocalFileGenerator

example_streams = (
    FileStream(
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


### Rationale
NOTE: wip

Unix pipes or chained python generators are ideal when the pipeline does not split and does not require metadata from previous steps.

Working around these challenges may lead to unnecessary complication and caching.

This framework's main intentions are to provide general solutions to the aforementioned challenges through a minimalistic and intuitive API. 



### Design
NOTE: wip

The whole framework is built around 2 main concepts:
- **class File**
    - Base class for files
- **class FileGenerator**
    - Initialized with an iterable of Files and yields a different File 

Files have 3 different type:
- **class FilePointer(File)**: A pointer to a file that could be used to produce a FileStream
- **class FileStream(File)**: File that produces a data stream
- **class FileSeekableStream(FileStream)** File that produces a seekable stream
 
In addition we have **class FileMeta** which provides meta-data often needed by FileGenerators to be able to generate files.  

## Built With

* travis
* tox

## Contributing

Bug-reports and pull requests on github  

## Versioning
Any version change could break the public API (until 1.0.0 release)
 

[Pypi](https://pypi.org/project/fpipe/#history)

[Github](https://github.com/vkvam/fpipe/releases)

## License
    
This project is licensed under the MIT License - see the [LICENSE.txt](https://github.com/vkvam/fpipe/blob/master/LICENSE.txt) file for details
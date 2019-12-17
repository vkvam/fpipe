[![Build Status](https://api.travis-ci.org/vkvam/fpipe.svg?branch=master)](https://travis-ci.org/vkvam/fpipe)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)


# fPipe

python framework for data manipulation and metadata extraction built around the python file-like api.

*Disclaimer: framework is currently in Alpha, production use discouraged*


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
import io

from fpipe.file import FileStream
from fpipe.generators.fileinfo import FileInfoGenerator
from fpipe.generators.local import LocalFileGenerator
from fpipe.meta.path import Path
from fpipe.meta.checksum import MD5Calculated
from fpipe.meta.size import SizeCalculated

test_gen = (
    FileStream(
        file=io.BytesIO(
            bytes(name * 2 ** size, encoding='utf-8')
        ),
        meta=(
            Path(f"{name}.txt"),
        )
    ) for name, size in (
        ('x', 4),
        ('y', 5),
        ('z', 6)
    )
)

info_gen = FileInfoGenerator(test_gen, [SizeCalculated, MD5Calculated])

local_gen = LocalFileGenerator(info_gen, pass_through=True)

for f in local_gen:
    print(f"{'name:':<{10}}{f.meta(Path).value}")

    print(f"{'content:':<{10}}", end='')
    while True:
        c = f.file.read(1).decode('utf-8')
        print(c, end='')
        if not c:
            break
    print(f"\n{'md5:':<{10}}{f.meta(MD5Calculated).value}")
    print(f"{'size:':<{10}}{f.meta(SizeCalculated).value}\n")
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
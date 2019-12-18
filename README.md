[![Build Status](https://api.travis-ci.org/vkvam/fpipe.svg?branch=master)](https://travis-ci.org/vkvam/fpipe)
[![codecov](https://codecov.io/gh/vkvam/fpipe/branch/master/graph/badge.svg)](https://codecov.io/gh/vkvam/fpipe)
[![PyPI version](https://badge.fury.io/py/fpipe.svg)](https://badge.fury.io/py/fpipe)
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

Example that reads a stream, calculates md5 and filesize, writes the file to disk and prints the original stream
```python
from fpipe.gen import FileInfoGenerator, LocalFileGenerator
from fpipe.file import ByteFile
from fpipe.meta import Path, SizeCalculated, MD5Calculated
from fpipe.workflow import WorkFlow

workflow = WorkFlow(
    LocalFileGenerator(pass_through=True),
    FileInfoGenerator((SizeCalculated, MD5Calculated))
)

for f in workflow.start(ByteFile(b'x' * 10, Path('x')), ByteFile(b'y' * 20, Path('y'))):
    print("name:", f.meta(Path).value)
    print("content:", f.file.read().decode('utf-8'))
    print("md5:", f.meta(MD5Calculated).value)
    print("size:", f.meta(SizeCalculated).value, end="\n\n")
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

* [Travis CI](https://travis-ci.org/)
* [Codecov](https://codecov.io/)
* [Tox](https://tox.readthedocs.io/)

## Contributing

Bug-reports and pull requests on github  

## Versioning
Any version change could break the public API (until 1.0.0 release)
 

* [Pypi](https://pypi.org/project/fpipe/#history)
* [Github](https://github.com/vkvam/fpipe/releases)

## License
    
This project is licensed under the MIT License - see the [LICENSE.txt](https://github.com/vkvam/fpipe/blob/master/LICENSE.txt) file for details

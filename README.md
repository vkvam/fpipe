[![Build Status](https://api.travis-ci.org/vkvam/fpipe.svg?branch=master)](https://travis-ci.org/vkvam/fpipe)
[![codecov](https://codecov.io/gh/vkvam/fpipe/branch/master/graph/badge.svg)](https://codecov.io/gh/vkvam/fpipe)
[![PyPI](https://img.shields.io/pypi/v/fpipe)](https://pypi.org/project/fpipe/)
![PyPI - Python Version](https://img.shields.io/pypi/pyversions/fpipe)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Codacy Badge](https://api.codacy.com/project/badge/Grade/b5f52ded1a7a40828bcce39f0982d38c)](https://www.codacy.com/manual/vkvam/fpipe?utm_source=github.com&amp;utm_medium=referral&amp;utm_content=vkvam/fpipe&amp;utm_campaign=Badge_Grade)
# fPipe

fpipe is a simple framework for creating and running data manipulation pipelines.

The need to cache files on disk between steps becomes problematic when performance is a concern.
Unix pipes are well suited for some problems, but become insufficient once things get too complex.

An example is unpacking a tar file from a remote source (e.g. s3/ftp/http) and storing it to another remote store.

*One possible solution using fPipe:*
```python
import boto3
from fpipe.workflow import WorkFlow
from fpipe.gen import S3, Tar
from fpipe.file import S3File
from fpipe.meta import Path

client = boto3.client('s3')
resource = boto3.resource('s3')
bucket = 'bucket'
key = 'source.tar'

WorkFlow(
    S3(client, resource),
    Tar(),
    S3(
        client, resource, bucket=bucket,
        pathname_resolver=lambda x: f'MyPrefix/{x.meta(Path).value}'
    )
).compose(
    S3File(bucket, key)
).flush()
```


### Installing
Framework is tested with Python 3.6 and above.

```bash
brew install python3
# apt, yum, apk...
pip3 install fpipe
# For aws s3 support you will need boto3
pip3 install boto3
```


### Simple example

*Calculates size and md5 of stream, while writing the stream to disk.*
```python
from fpipe.file import ByteFile
from fpipe.gen import Local, Meta
from fpipe.meta import Path, Size, MD5
from fpipe.workflow import WorkFlow

workflow = WorkFlow(
    Local(pass_through=True),
    Meta(Size, MD5)
)

sources = [
    ByteFile(b'x' * 10, Path('x.dat')),
    ByteFile(b'y' * 20, Path('y.dat'))
]

for stream in workflow.compose(sources):
    print(f'\n{"-"*46}\n')
    print("Path name:", stream.meta(Path).value)
    print("Stream content: ", stream.file.read().decode('utf-8'))
    with open(stream.meta(Path).value) as f:
        print("File content:", f.read())
    print("Stream md5:", stream.meta(MD5).value)
    print("Stream size:", stream.meta(Size).value)
```

### Subprocess script example

*Stores original stream, calculates md5, encrypts using cli, stores encrypted file, calculates md5, decrypts and stores decrypted file*

```python
from fpipe.file import ByteFile
from fpipe.gen import Local, Meta, Program
from fpipe.meta import Path, MD5
from fpipe.workflow import WorkFlow

workflow = WorkFlow(
    Meta(MD5),
    Local(pass_through=True),

    Program("gpg --batch --symmetric --passphrase 'secret'"),
    Meta(MD5),
    Local(pass_through=True, pathname_resolver=lambda x: f'{x.meta(Path).value}.gpg'),

    Program("gpg --batch --decrypt --passphrase 'secret'"),
    Meta(MD5),
    Local(pass_through=True, pathname_resolver=lambda x: f'{x.meta(Path).value}.decrypted')
)

sources = (
    ByteFile(b'x' * 10, Path('x.orig')),
    ByteFile(b'y' * 20, Path('y.orig'))
)

for f in workflow.compose(sources).flush_iter():
    print(f'\n{"-"*46}\n')
    print("Original path:", f.meta(Path, 2).value)
    print("Original md5:", f.meta(MD5, 2).value, end='\n\n')
    print("Encrypted path:", f.meta(Path, 1).value)
    print("Encrypted md5:", f.meta(MD5, 1).value, end='\n\n')
    print("Decrypted path:", f.meta(Path).value)
    print("Decrypted md5:", f.meta(MD5).value)

```

See unittests for more examples

## Run tests and verify pypi compatibility 

To run tests install tox and twine with pip, go to project root and run tox
```bash
# Create virtualenv
python3 -m venv .venv
# Activate virtualenv
source .venv/bin/activate
# Run tests
tox -e py37
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
The framework is functional, but in the early stages, so any feedback on alternatives, usefulness, api-design, etc. would be appreciated

See [CONTRIBUTING.md](https://github.com/vkvam/fpipe/blob/master/CONTRIBUTING.md)

## Versioning
 
* [Pypi](https://pypi.org/project/fpipe/#history)
* [Github](https://github.com/vkvam/fpipe/releases)

## License
    
This project is licensed under the MIT License - see the [LICENSE.txt](https://github.com/vkvam/fpipe/blob/master/LICENSE.txt) file for details

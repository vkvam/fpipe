[![Build Status](https://api.travis-ci.org/vkvam/fpipe.svg?branch=master)](https://travis-ci.org/vkvam/fpipe)
[![codecov](https://codecov.io/gh/vkvam/fpipe/branch/master/graph/badge.svg)](https://codecov.io/gh/vkvam/fpipe)
[![PyPI](https://img.shields.io/pypi/v/fpipe)](https://pypi.org/project/fpipe/)
![PyPI - Python Version](https://img.shields.io/pypi/pyversions/fpipe)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
# fPipe

fpipe is a simple framework for creating data manipulation/validation pipelines around the python file-like api.

The need to cache files on disk between different file transformations quickly becomes an issue when speed and hardware concerns are a factor, and unix pipes are not able to deal with the pipeline complexity.

An example is unpacking a tar file from a remote source (e.g. s3/ftp/http) and storing it to another remote store.
A unix pipe is not able to output the files within the tar in addition to metadata about the file, so we can not set or modify the target path.

A proposed solution using fPipe:
```
client = boto3.client('s3')
resource = boto3.resource('s3')
bucket = 'bucket'
key = 'source.tar'

WorkFlow(
    S3Gen(client, resource),
    TarGen(),
    S3Gen(
        client, resource, bucket=bucket,
        pathname_resolver=lambda x: f'MyPrefix/{x.meta(Path).value}'
    )
).compose(
    S3File(bucket, S3Key(key))
).flush()
```

*The framework is functional, but in the early stages, so any feedback on alternatives, usefulness, api-design, etc. would be appreciated*

### Installing

for S3 support you need boto3

```bash
brew install python3
# apt, yum, apk...
pip3 install fpipe
# Optional
pip3 install boto3
```

### Getting started
##### Simple example
*Calculates size and md5 of stream, while storing stream to disk and prints content. When file is read finished, md5 is ready and printed* 

```python
from fpipe.file import ByteFile
from fpipe.gen import LocalGen, MetaGen
from fpipe.meta import Path, SizeCalculated, MD5Calculated
from fpipe.workflow import WorkFlow

workflow = WorkFlow(
    LocalGen(pass_through=True),
    MetaGen(SizeCalculated, MD5Calculated)
)

for stream in workflow.compose(ByteFile(b'x' * 10, Path('x.dat')), ByteFile(b'y' * 20, Path('y.dat'))):
    print(f'\n{"-"*46}\n')
    print("Path name:", stream.meta(Path).value)
    print("Stream content: ", stream.file.read().decode('utf-8'))
    with open(stream.meta(Path).value) as f:
        print("File content:", f.read())
    print("Stream md5:", stream.meta(MD5Calculated).value)
    print("Stream size:", stream.meta(SizeCalculated).value)
```

##### Subprocess script example
*Stores original stream, calculates md5, encrypts using cli, stores, calculates md5, decrypts using cli and stores. Using flush_iter() we know all files have been completely read(), so MD5Calculated will be readable.*

```python
from fpipe.file import ByteFile
from fpipe.gen import LocalGen, MetaGen, ProcessGen
from fpipe.meta import Path, MD5Calculated
from fpipe.workflow import WorkFlow

workflow = WorkFlow(
    MetaGen(MD5Calculated),
    LocalGen(pass_through=True),

    ProcessGen("gpg --batch --symmetric --passphrase 'secret'"),
    MetaGen(MD5Calculated),
    LocalGen(pass_through=True, pathname_resolver=lambda x: f'{x.meta(Path).value}.gpg'),

    ProcessGen("gpg --batch --decrypt --passphrase 'secret'"),
    MetaGen(MD5Calculated),
    LocalGen(pass_through=True, pathname_resolver=lambda x: f'{x.meta(Path).value}.decrypted')
)

for f in workflow.compose(ByteFile(b'x' * 10, Path('x.orig')), ByteFile(b'y' * 20, Path('y.orig'))).flush_iter():
    print(f'\n{"-"*46}\n')
    print("Original path:", f.meta(Path, 2).value)
    print("Original md5:", f.meta(MD5Calculated, 2).value, end='\n\n')

    print("Encrypted path:", f.meta(Path, 1).value)
    print("Encrypted md5:", f.meta(MD5Calculated, 1).value, end='\n\n')

    print("Decrypted path:", f.meta(Path).value)
    print("Decrypted md5:", f.meta(MD5Calculated).value)

```

See unittests for more examples

## Run tests and verify pypi compatibility 

To run tests install tox and twine with pip, go to project root and run tox
```bash
# python3 -m venv .venv
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

Bug-reports and pull requests on github  

## Versioning
Any version change could break the public API (until 1.0.0 release)
 

* [Pypi](https://pypi.org/project/fpipe/#history)
* [Github](https://github.com/vkvam/fpipe/releases)

## License
    
This project is licensed under the MIT License - see the [LICENSE.txt](https://github.com/vkvam/fpipe/blob/master/LICENSE.txt) file for details

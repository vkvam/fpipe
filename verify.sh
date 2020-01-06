#!/bin/bash
# Convenience script for running tests and validating pypi compatibility
set -e
source .venv/bin/activate
python setup.py sdist bdist_wheel
tox -e py37
twine check dist/*

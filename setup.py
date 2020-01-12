from setuptools import setup, find_packages

with open('README.md') as readme:
    long_description = readme.read()

version = '0.0.23'

setup(
    name='fpipe',
    version=version,
    description='Library for working with file-likes as piped streams',
    long_description=long_description,
    long_description_content_type='text/markdown',
    classifiers=[
        'Development Status :: 3 - Alpha',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
        'Intended Audience :: Developers'
    ],
    keywords='pipe file stream',
    url='https://github.com/vkvam/fpipe',
    author='Vemund Kvam',
    author_email='vemund.kvam@gmail.com',
    license='MIT',
    packages=find_packages(exclude=['*.tests']),
    install_requires=[
        'botocore>=1.13.40'
    ],
)

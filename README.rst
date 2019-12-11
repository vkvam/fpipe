=====
fpipe
=====


Framework for processing file-likes with chained pipes through a pull model


Description
===========

Each step in the chain generates file-likes that are picked up by the next step in the chain.
The workflow is pull/read based, so the final consumer will control the rate of each step in the chain.

Examples:
```
# Reverse some files and get the size and checksum of the reversed file
gen = LocalFileGenerator(
    [
        LocalFile('log.txt'),
        LocalFile('log2.txt'),
        LocalFile('log3.txt')
    ]
)

gen = FileInfoGenerator(gen.get_files(), ChecksumCalculator)
gen = ProcessFileGenerator(gen.get_files(), "rev")
gen = FileInfoGenerator(gen.get_files(), ChecksumCalculator)
for f in gen.get_files():
    print(f.file.read().decode('utf-8'))
    print(f.file_info_generator.get())

```


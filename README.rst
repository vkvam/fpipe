=====
fpipe
=====


Framework for processing file-likes through chained pipes


Description
===========

Each step in the chain generates file-likes that are passed on to the next step in the chain.

An example of a workflow could be:
FTP -> Decrypt -> UnTar -> Calculate MD5 checksum -> S3
# Cassandra2Aerospike
A utility to import Cassandra tables into Aerospike.

It may be run without a running instance of Cassandra, for example from a backup server or a specially designated host.

Platforms:
This utility has been tested on Centos 7.5 and macOS 10.14.

Features:
* Multithreaded pipelining model.
  Internal tests show this utility can process about 100,000 rows per second with 1KB rows.
* Fast resume mode:
  Export may start on any key. Upon suspending, the utility will print out the next partition key to resume on next time.

Requirements:
* Cmake 3.1 or above
* A working C++11 compiler
* Aerospike Client libraries (with libev) https://www.aerospike.com/download/client/
* Libev
* Snappy
* LZ4
* ZLib
* OpenSSL
* Pthreads

Building (Linux):
$ cmake .
$ make

Todo:
* Handle clustering columns:
  The Cassandra parser does not understand clustering columns beyond knowing how to ignore them. The behaviour when encountering them is different depending on version.
  For version < MA, they will present as large rows, often with the single column appearing multiple times.
  For version >= MA, they will present as multiple rows with the same partitioning key.
  In each case, merging of multiple SSTables (for column overwrites and deletion) will happen incorrectly.
  Handling of static columns is likewise incorrect.
  
* Handle column types:
  Cassandra's column schema table is currently not read. This means all columns are written to Aerospike as binary blobs, regardless of their intended type.
  

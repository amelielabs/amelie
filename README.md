
![image description](.github/logo.png)

### Full Parallelization of IO and Compute

Amelie is a lightweight, full-featured, in-memory OLTP SQL relational database that
allows full parallelization and lockless transaction processing.

It scales linearly with the number of CPU cores both for IO and Compute separately,
performs automatic partitioning, and generates parallel group plans for
all types of queries.

### Asynchronous Replication

Amelie has support for Hot Backup and Async Logical Replication, which allows
fault-tolerant primary-replica setups to be created.

### Easy to use

It works over HTTP and does not require additional client libraries. Any modern programming language or
tooling that supports HTTP and JSON can be used directly.
Compiles and distributes as a single binary.

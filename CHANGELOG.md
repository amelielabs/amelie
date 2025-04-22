# Amelie Changelog.

## 0.3.0 (22-04-2025)

This release redefines how Amelie works with tables and partitions, solving major cross-compute limitations.

Before this release, Amelie had two types of tables: partitioned tables and shared tables. Partitions are
created on each backend worker (per CPU core) for parallel access or modification.

Partitioned tables cannot be directly joined with other partitioned tables, and the same limitation applies
to subqueries. Instead, the transaction must use CTE or Shared tables to achieve the same effect.

Shared tables are not partitioned (single partition) and are available for concurrent direct read access from any
backend worker. Shared tables support efficient parallel JOIN operations with other partitioned tables,
where shared tables are used as dictionary tables.

While this approach has its benefits, it is also not common and limits adoption, requiring the design of a
database schema around it.

This release solves those problems. Support for the shared tables has been removed, and all tables are
now treated the same (all tables are partitioned).

Amelie now allows cross-partition joins and subqueries from one partitioned table to another
(from one backend worker to another) by coordinating access across CPU cores for different types of queries.

Additionally, the number of partitions per table can now be provided by using the CREATE TABLE () PARTITIONS clause.

## 0.2.0 (11-03-2025)

This release introduces Heap - a custom memory allocator to replace malloc() for Rows.

Heap is designed to work in parallel, shared-nothing environments and is associated with each table partition.

Related improvements:

* About 20-30% less memory usage
* Ability to have zero fragmentation for tables which has fixed column types only
* No multi-thread contention-related issues associated with malloc()
* Instant DROP TABLE of large tables, it is no longer required to free each row individually
* Allows to use Linux Huge Pages (coming soon)
* Allows to implement Copy-On-Write individually for partitions (coming soon)
* Allows to do SELECT FROM USE HEAP to return rows directly from the memory allocator without involving indexes

Heap content replaces the snapshot file format for partitions. Each partition can be saved and restored by
dumping its memory pages directly without involving indexes and iterating through tables. It improves the time
to checkpoint completion by more than 50% for large tables.

This release also introduces a reworked test suite, which is now part of the console application and available
under the amelie test command. This will simplify the development and testing of third-party extensions in
the future. Also, Amelie no longer depends on libcurl for its test suite.

## 0.1.0 (26-02-2025)

We are happy to present the first public release of Amelie - a new Relational SQL Database for High-intensity OLTP workloads.

Amelie is a lightweight, full-featured, in-memory SQL OLTP relational database focusing on performance and throughput for short ACID transactions and real-time analytics.

This release is an MVP and a culmination of years of work.

The short list of features for the first release:

* Serializable ACID Multi-statement transactions
* Secondary indexes (Tree/Hash)
* CTE with DML RETURNING
* Parallel partitioned DML including UPSERT
* Parallel JOINs
* Parallel GROUP BY and ORDER BY
* Partitioned Generated Columns
* Native VECTOR support
* Native JSON support
* Parallel snapshotting and recovery
* Hot Remote Backup
* Asynchronous Logical Replication
* HTTP API

https://amelielabs.io

# Amelie Changelog.

## 0.8.0 (12-12-2025)

This release introduces a switch to separate databases, greatly improved HTTP/endpoints support, a
second test suite dedicated to sqllogictest, and a redefined CLI to support
embedded database instances.

**CREATE/DROP/ALTER DATABASE.**

In this release, Amelie radically switched entirely from dedicated schemas to dedicated, completely isolated databases.

Unlike most of the classical relational databases, Amelie has no separate sessions (HTTP requests are stateless).
This design choice greatly simplifies external connections and the need for connection pooling. Usually, connection
poolers need to account for which user/database connection is for.

One of the main intentions is to make multi-tenant support easier (user-per-database) with proper resource separation.
Additionally, this greatly simplifies many things.

It is possible to DROP DATABASE within the same connection, which should simplify role management.
Databases are implemented at the logical level, separating things in the catalog and sharing system resources.
Databases are currently strictly isolated.

This change affects both SQL semantics and HTTP endpoints, as well as the CLI and embeddable connections. It is now necessary
to specify the working database (unless it is a default one).

**IMPROVED PLANNER.**

This release introduces reworked dedicated planner logic, which unifies local and distributed processing
into a dynamic set of rules.

**IMPROVED HTTP ENDPOINTS AND CONNECTION STRINGS.**

HTTP API now includes two main calls:

```
# execute sql
POST /v1/db/<db_name>

# insert data into a table or call a function
POST /v1/db/<db_name>/<relation_name>
```

All HTTP requests properly understand the `Content-Type` and `Accept` headers and act accordingly. For each call, you can
also pass arguments, set the timezone, specify an additional return format, columns, etc.

There are two types of supported connection strings: one for the `http://` protocol and one for the `amelie://` protocol.
The Amelie protocol URI is used for embeddable databases, allowing the specification of the database and
desired MIME:  `amelie://db_name/table?content-type=application/json`.

**UNIFIED CLI TO ALL DATABASES / PLAIN-TEXT RESULTS.**

Amelie CLI can now run embedded databases like SQLite/DuckDB by default, without the necessity to init
and start the server first (which is still possible).

Depending on how the database is accessed, cli can start the database and run the console (as an embeddable database),
connect to an existing database process via a Unix socket, or connect remotely via the HTTP API.

Everything works transparently and is simple to use.

By default, the HTTP API now sets and supports plain/text output, dedicated for console usage.

**DEDICATED TEST SUITE FOR SQLite TESTS.**

This release introduces an additional test suite - sqllogictests (SQLite tests). The tests themselves are huge, totaling 1.1 GB of
data containing ~6.5 million tests. Most essential tests have passed; others will be included at a later time.
Amelie will support a separate repository for them.

Test suite implemented as an Amelie CLI command `amelie test-slt`.

## 0.7.0 (04-11-2025)

This major release introduces full-featured support for User-Defined Functions (UDF), similar in
spirit to PostgreSQL PL/SQL and other relational databases.

CREATE/DROP/ALTER FUNCTION.

It is now possible and recommended to organize work using functions. Functions are multi-statement,
have full support for control structures, variables, arguments, and are fully transactional.
Functions can execute other functions. Functions cannot execute DDL commands.

Functions are designed to increase performance marginally. They are stored in compiled form,
ready for execution without the necessity for replanning and parsing. All functions are strictly typed
and parallel in nature. Each function consists of two bytecode sections - main and pushdown
(code intended for parallel execution).

CONTROL STRUCTURES.

This release introduces support for all common control structures:

 - if/elsif/else
 - while
 - for
 - break/continue
 - return
 - execute

FULLY ASYNC AND PARALLEL EXECUTION.

The main complexity lies in optimizing functions to reduce round-trip and ideally execute
everything without wait times.

Amelie is using several techniques, including delayed result processing and delayed variable
assignments (until subsequent use), as well as identifying last sending statements to
notify the backend about early completion.

All non-returning statements do not require execution confirmation until the transaction
is completed in full.

GROUP CONFIRMATION AND COMMIT PROTOCOL.

This release introduces some smart techniques to dramatically reduce IPC between backend
workers by introducing GROUP CONFIRMATIONS. The goal is to scale, minimize wait times, and
context switches at all levels. Amelie is using similar techniques as low-latency trading
systems, such as fast IPC queues (LMAX style), etc.

Amelie introduces a delayed COMMIT/ABORT protocol, similar in spirit to consensus protocols,
such as Raft, where transactions have meta-information about the commit state
(log state) from the primary.

COMPILE TIME EXECUTION.

Starting from this release, Amelie will execute almost all built-in functions during
compile time if the function arguments can be calculated during compilation. This is primarily
used for casting, but also supports complex manipulations, such as modifying JSON.

HTTP API AND PREFER HEADER SUPPORT.

This release adds support for the HTTP API to execute functions and also introduces support
for the HTTP Prefer header.

It is now possible to pass the timezone and the preferred return format information directly
during remote execution.

PLANNER IMPROVEMENTS.

This release features significantly improved logic for processing and matching index keys,
as well as understanding outer targets and allowing expressions to be used as keys, where possible.

## 0.6.0 (19-09-2025)

This release introduces a way to use embedded Amelie as a client driver for remote connections
and some significant performance improvements.

Amelie to Amelie connections.

Embedded Amelie API has the `amelie_connect()` function, which is used to open a session for the local database.

This release introduces the URI argument support. If supplied, it will open a remote HTTP
connection instead of a local one, handle the IO/HTTP/TLS and Authentication transparently and in
the most efficient way (using frontends - pool of workers with cooperative multitasking).
Amelie API is asynchronous.

In other words, one embeddable database can be used as a client driver for another remote server.
This change also opens up a lot of useful and fun things to do in the future.

Group Commit Redesigned.

This release introduces redesigned executor/commit logic. The distributed commit + wal path has
entirely moved to the dedicated commit worker. This change allowed for a significant improvement
in group commit/write + fsync performance and a measurable reduction in CPU usage for frontends.
The executor part now works as a transaction serializer only.

Last benchmarks showed that Amelie can now handle 600-700K debit-credit transactions
(TigerBeetle benchmark) with fsync on each write!

Since those transactions are not prepared (SQL needs to be parsed/planned on each execution),
there is still room for improvement.

I've also seen 10M+ batch inserts with fsync on each write.

Lockless IPC and Event Processing / Reduced Context Switches.

Classical multi-threading sync primitives like mutexes/condition variables and even spinlocks can
lead to increased context switches, poor CPU utilization, and increased latencies. Context Switching
is one of the significant problems in low-latency high-performance systems.

In this release, IPC and the event processing between workers were redesigned using lockless
queues (similar to LMAX-Dispurtor), and most of the hot paths were optimized further.

As a result of those changes, Amelie was able to do 20M+ upserts/second on in-memory workloads
for the first time on a dev machine during testing.

## 0.5.0 (13-08-2025)

This release introduces Embeddable Async API interface which allows to use Amelie as a shared
library similar to SQLite.

Embeddable Async API.

Using Amelie as an embeddable database allows to get maximum out of the latency and performance by
avoiding network io/http parsing and authentication. Primary use case for this is fintech and cases which
require high performance processing.

Amelie API is async with separate execute/wait stages which allows it to be integrated into any existing event loops.

Amelie is designed to benefit from parallel execution. It is possible to execute several commands
in parallel to scale the performance and benefit from multi-core processing and things like group commit,
and STRICT SERIALIZABILITY at the same time.

There are no limitations compared to the stand-alone server mode. Unlike SQLite Amelie does not block
writers and have a full network support in the embeddable mode. That means that the replication, network
client connections and remote hot backup will work the same way (or can be disabled completely).

Amelie as a Compute Framework.

This release introduces important internal concept of Amelie as a compute framework. It is one of the goals
towards support of extensions.

The idea is to define and keep the Amelie core as a storage/replication + compute (virtual machine,
executor and parallel processing) and move query parsing out to extensions. This allows to implement
other protocols/parsers as an extensions on top of it and use Amelie core as a framework.

Basically it allows to completely separate SQL parsing from the actual code execution and extend Amelie
with different query languages (think redis, memcache) without paying the price of building on
top of the SQL processing.

## 0.4.0 (25-06-2025)

This release introduces checkpoint compression and a custom command reader (readline/linenoise) replacement.

This release introduces checkpoint file format compression (heap file to be exact) and it is enabled by default
using the zstd compression now.

For performance reasons Amelie checkpoint dumps exact copy of heaps per partition (memory allocator context) to
avoid index scans. Since the heap file format is sparse and designed for faster allocations, the compression
allows to greatly save space here in the range of x10-100 depending on the data being stored. Additionally this
release introduces checkpoint specific options, such as checkpoint_crc and checkpoint_sync.

This versions also introduces custom command line reader to replace linenoise with a simpler implementation with
utf8 support. This will also allow better extensibility in the future.

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

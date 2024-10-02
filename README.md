
![image description](.github/logo.png)

### About

Amelie is a new lightweight OLTP SQL document database with a focus on performance and throughput for short ACID transactions and real-time analytics.

It was designed as a modern alternative to in-memory NoSQL databases.

### Full Parallelization

Amelie has a unique architecture designed for full parallelization and lock-less transaction execution. It treats local machine CPU cores as if they were distributed system nodes, automatically partitioning, and generating parallel group plans for all types of queries.

### Separate Storage an Compute

Amelie is designed as an in-memory SQL database with optional storage for large volumes of aggregated historical data (which we plan to introduce in future releases).

### JSON/HTTP Native

Amelie implements close to standard SQL dialect with seamless JSON integration. It works over HTTP and does not require any additional client libraries or compatibility with other databases.

### Asynchronous Replication

Amelie has support for Hot Backup and Async Replication which allows to create fault tolerant Primary-Replica setups.

### Features

The database has a modern set of standard and unique features, which we continuously improve.

- Serializable ACID Multi-statement transactions 
- CTE with DML RETURNING
- Secondary indexes (Tree/Hash)
- Parallel partitioned DML including UPSERT
- Parallel JOINs
- Parallel GROUP BY and ORDER BY
- Aggregates, Partials, Lambda aggregates
- Relational and Schemaless tables
- Parallel snapshotting and recovery
- Native Vector support
- Hot Backup
- Asynchronous Replication
- JSON-Native and RESTfull

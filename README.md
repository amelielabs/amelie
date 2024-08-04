
![image description](.github/logo.png)

### About

We are building a new lightweight in-memory OLTP SQL database with a focus on performance and throughput for short ACID transactions and real-time analytics. 

Amelie was designed as an alternative to in-memory NoSQL databases.

It has a unique architecture designed for full parallelization, automatic table partitioning, and lock-less execution.
It is based on the idea of automatically treating local machine CPU cores as if they were distributed system nodes, sharding data, partitioning, 
and generating parallel plans for all types of queries.

### Features

The database has a modern set of standard and unique features, which we continuously improve.

- Serializable ACID Multi-statement transactions
- CTE with RETURNING
- Secondary indexes (Tree/Hash)
- Parallel DML including UPSERT
- Parallel JOINs
- Parallel GROUP BY and ORDER BY
- Aggregates
- Lambda aggregates, etc.
- Schemaless tables
- Parallel snapshotting and recovery
- Hot Backup
- Asynchronous replication
- JSON-Native and RESTfull

### Fluent in JSON

While we make the SQL semantics close to standard, Amelie was designed as a document database with
rich and convenient capabilities to work with JSON:

- Support traditional or Schemaless tables for storing only JSON objects
- Create indexes for JSON objects
- Access JSON fields directly
- Execute expressions and subqueries inside JSON during SELECT
- Select FROM or JOIN any JSON document
- JSON is used for results by default

``` SQL
-- Schemaless collection
create table collection (
  obj object,
  primary key (obj.id int)
)
insert into collection {"id": 42, "data": [1,2,3]}

select obj from collection
[{
  "id": 42,
  "data": [1, 2, 3]
}]

select obj.data from collection
[[1, 2, 3]]

-- JSON expressions
select {"metrics": obj.data} from collection
[{
  "metrics": [1, 2, 3]
}]

select {"id": 42, "data": [1,2,3]}.data[2]
3
```

### RESTfull

Amelie works over HTTP and does not require any additional client libraries or compatibility with other databases to work with.
Any modern programming language or tooling which supports HTTP and JSON can be used directly.

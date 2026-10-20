---
title: "From Page To Snapshot: How Trino Writes An Iceberg Table"
date: 2026-07-11T21:42:00.000Z
lastmod: 2026-07-11T21:43:00.000Z
draft: false
series:
  - Learning Trino by Tracing One Iceberg Table from SQL to Files
tags:
  - Trino
  - Iceberg
categories: []
authors:
  - Ziva Li
---
<link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/katex@0.16.2/dist/katex.min.css" integrity="sha384-bYdxxUwYipFNohQlHt0bjN/LCpueqWz13HufFEV1SUatKs1cm4L6fFgCi1jT643X" crossorigin="anonymous">


# From Page To Snapshot


The write path is where Trino’s `Page` model turns into Iceberg files and
Iceberg metadata.


For CTAS (Create Table As Select) and INSERT, the compact mental model is:


```text
workers write data files
coordinator commits Iceberg metadata
```


That split matters. A write is not just “Trino creates a Parquet file.” Workers
receive page batches, write data files through the connector page sink, and
return small commit fragments. The coordinator gathers those fragments and asks
Iceberg to commit a new snapshot.


The shape I want to remember is:


```text
upstream operators
  -> Page
  -> TableWriterOperator
  -> IcebergPageSink.appendPage(...)
  -> IcebergPageSink.finish()
  -> commit fragments
  -> TableFinishOperator
  -> IcebergMetadata.finishCreateTable(...) or finishInsert(...)
  -> Iceberg snapshot and manifest metadata
```


This note only covers the append-style write path: CTAS and INSERT.


## The Setup


Use a small Iceberg table so the write is easy to inspect:


```sql
DROP TABLE IF EXISTS iceberg.write_trace.orders_delta;
DROP SCHEMA IF EXISTS iceberg.write_trace;

CREATE SCHEMA iceberg.write_trace
WITH (location = 'local:///write-trace');
```


Then create the table with CTAS:


```sql
CREATE TABLE iceberg.write_trace.orders_delta
WITH (
    format = 'PARQUET',
    format_version = 2,
    partitioning = ARRAY['orderstatus']
) AS
SELECT
    orderkey,
    custkey,
    orderstatus,
    totalprice
FROM (
    VALUES
        (1, 1001, 'O', CAST(10.00 AS DOUBLE)),
        (2, 1002, 'P', CAST(20.00 AS DOUBLE)),
        (3, 1003, 'F', CAST(30.00 AS DOUBLE)),
        (4, 1004, 'O', CAST(40.00 AS DOUBLE))
) AS t(orderkey, custkey, orderstatus, totalprice);
```


This statement is useful because it is both:


```text
CREATE TABLE:
  define the Iceberg table schema, partitioning, and location

AS SELECT:
  run a query that produces rows and writes them into the new table
```


So CTAS is DDL plus a write query. It needs a table-create path and a data-write
path.


## The Plan Shape


The useful distributed `EXPLAIN` shape for CTAS is roughly:


```text
Fragment 0 [COORDINATOR_ONLY]
  TableCommit[target = iceberg:write_trace.orders_delta@...]
    RemoteSource[sourceFragmentIds = [1]]

Fragment 1 [SOURCE]
  TableWriter[target = iceberg:write_trace.orders_delta@...]
    Values[]
      (1, 1001, 'O', 10.0)
      (2, 1002, 'P', 20.0)
      (3, 1003, 'F', 30.0)
      (4, 1004, 'O', 40.0)
```


The exact symbol names, fragment labels, and writer distribution can vary. The
important operators are:


| Plan operator  | What it means                                                 |
| -------------- | ------------------------------------------------------------- |
| `Values`       | The source side produces rows.                                |
| `TableWriter`  | Workers write rows through a connector page sink.             |
| `RemoteSource` | The coordinator reads writer output from the worker fragment. |
| `TableCommit`  | The coordinator finishes the write.                           |


Read the data flow from the source fragment toward fragment `0`:


```text
Fragment 1
  Values produces pages
  TableWriter writes data files and emits fragments

Fragment 0
  TableCommit consumes fragments
  connector commits the Iceberg snapshot
```


That is the same fragment-reading habit as the
EXPLAIN post:
follow `RemoteSource`, not printed order.


## Phase 1: Workers Write Data Files


The worker-side write boundary is:


```text
upstream operator output
  -> Page
  -> TableWriterOperator.addInput(Page)
  -> ConnectorPageSink.appendPage(Page)
```


For this CTAS, the upstream operator is simple:


```text
ValuesOperator
  -> TableWriterOperator
```


For a CTAS from another table, the upstream side could be a scan:


```text
TableScanOperator
  -> filter/project work
  -> TableWriterOperator
```


For a more complex `INSERT SELECT`, the upstream side could include joins,
aggregations, exchanges, or sorts. The writer does not care where the page came
from. It receives a `Page` batch and passes the rows to the connector sink.


The connector handoff is:


```text
TableWriterOperator
  -> PageSinkManager
  -> IcebergPageSinkProvider
  -> IcebergPageSink
```


At this point, Trino is crossing from engine execution into the Iceberg
connector’s writer.


## What `IcebergPageSink` Does


`IcebergPageSink.appendPage(...)` receives Trino pages and writes rows into
Iceberg data files.


A `Page` is not a file. It is an in-memory batch of rows in Trino’s columnar
`Block` format.


The page sink has to turn those pages into file output:


```text
incoming Page
  -> choose writer or partition writer
  -> write rows into Parquet
  -> close file writers at finish
  -> return CommitTaskData fragments
```


For the CTAS table:


```text
partitioning = ARRAY['orderstatus']
```


the sink may write rows into different partition outputs. The physical file
names and counts are not the lesson. The lesson is that worker tasks write data
files before the coordinator commits the table metadata.


When worker-side writing finishes:


```text
IcebergPageSink.finish()
  -> closes file writers
  -> returns serialized commit fragments
```


Those fragments are small receipts. They describe written files. They are not
the table snapshot by themselves.


## Phase 2: Coordinator Commits Metadata


The coordinator-side boundary is:


```text
TableFinishOperator
  -> metadata finish method
  -> Iceberg commit
```


In the plan, this is the `TableCommit` fragment:


```text
Fragment 0 [COORDINATOR_ONLY]
  TableCommit
    RemoteSource[sourceFragmentIds = [1]]
```


The coordinator receives writer fragments from workers, gathers them, and calls
the connector finish path.


For CTAS, the finish route is:


```text
TableFinishOperator
  -> finishCreateTable(...)
  -> IcebergMetadata.finishCreateTable(...)
```


For CTAS with rows, the create-table finish path still has to commit the data
files. The Iceberg side turns worker fragments into `DataFile` records and
commits an append-style snapshot.


Conceptually:


```text
commit fragments
  -> DataFile objects
  -> AppendFiles
  -> new Iceberg snapshot
  -> new manifest metadata
```


This is the part that makes Iceberg different from “write some files under a
directory.” The table state is the committed snapshot. The data files become
visible because the snapshot metadata points to them.


## INSERT Reuses The Same Shape


After CTAS, append more rows:


```sql
INSERT INTO iceberg.write_trace.orders_delta
VALUES
    (5, 1005, 'F', CAST(50.00 AS DOUBLE)),
    (6, 1006, 'O', CAST(60.00 AS DOUBLE));
```


INSERT is simpler than CTAS because the table already exists.


The begin path changes:


```text
CTAS:
  beginCreateTable(...)
  create a writable table handle for a new table

INSERT:
  beginInsert(...)
  create a writable table handle for an existing table
```


But the worker write model is the same:


```text
upstream pages
  -> TableWriterOperator
  -> IcebergPageSink.appendPage(...)
  -> IcebergPageSink.finish()
  -> commit fragments
```


The coordinator finish route changes:


```text
CTAS:
  finishCreateTable(...)

INSERT:
  finishInsert(...)
```


The Iceberg commit concept is still append-style:


```text
new data files
  -> AppendFiles
  -> new snapshot
```


So INSERT is not a special row-by-row mutation. It writes new files and commits
new metadata that makes those files part of the current table snapshot.


## What To Check In Iceberg


After CTAS, check that the table exists:


```sql
SHOW CREATE TABLE iceberg.write_trace.orders_delta;
```


Check snapshots:


```sql
SELECT
    committed_at,
    snapshot_id,
    parent_id,
    operation,
    summary
FROM iceberg.write_trace."orders_delta$snapshots"
ORDER BY committed_at;
```


After CTAS, one snapshot should be there for the initial append. After INSERT, there should be another snapshot.


Check current files:


```sql
SELECT
    content,
    file_path,
    file_format,
    record_count,
    partition
FROM iceberg.write_trace."orders_delta$files"
ORDER BY content, file_path;
```


For this narrow post, the important check is:


```text
CTAS:
  table exists
  snapshot exists
  data files exist

INSERT:
  another snapshot exists
  additional data-file work is visible
```


## What The Evidence Proves


The evidence types prove different things:


| Evidence          | What it proves                                                 |
| ----------------- | -------------------------------------------------------------- |
| `EXPLAIN`         | The planned write shape: `TableWriter` below `TableCommit`.    |
| `EXPLAIN ANALYZE` | Runtime write stats for that execution. It executes the write. |
| `$snapshots`      | Iceberg committed a new snapshot.                              |
| `$files`          | Current snapshot points to data files.                         |


For a read query, `EXPLAIN ANALYZE` reads data and reports runtime stats. For a
CTAS or INSERT, it runs the write. Use it only when writing to a disposable
trace table.


## Code Anchors


These are the source areas to inspect after the plan shape makes sense:


| Concept                               | Code area                                                                                 |
| ------------------------------------- | ----------------------------------------------------------------------------------------- |
| Build write plan                      | `core/trino-main/src/main/java/io/trino/sql/planner/LogicalPlanner.java`                  |
| Turn write reference into real target | `core/trino-main/src/main/java/io/trino/sql/planner/optimizations/BeginTableWrite.java`   |
| Create worker writer operator         | `core/trino-main/src/main/java/io/trino/sql/planner/LocalExecutionPlanner.java`           |
| Consume pages and emit fragments      | `core/trino-main/src/main/java/io/trino/operator/TableWriterOperator.java`                |
| Finish write on coordinator           | `core/trino-main/src/main/java/io/trino/operator/TableFinishOperator.java`                |
| Create connector page sink            | `core/trino-main/src/main/java/io/trino/split/PageSinkManager.java`                       |
| Begin and finish Iceberg writes       | `plugin/trino-iceberg/src/main/java/io/trino/plugin/iceberg/IcebergMetadata.java`         |
| Create Iceberg page sink              | `plugin/trino-iceberg/src/main/java/io/trino/plugin/iceberg/IcebergPageSinkProvider.java` |
| Write rows into Iceberg files         | `plugin/trino-iceberg/src/main/java/io/trino/plugin/iceberg/IcebergPageSink.java`         |
| Worker file-write receipt             | `plugin/trino-iceberg/src/main/java/io/trino/plugin/iceberg/CommitTaskData.java`          |


The useful first debugger path is:


```text
BeginTableWrite
  -> IcebergMetadata.beginCreateTable(...) or beginInsert(...)
  -> TableWriterOperator.addInput(Page)
  -> IcebergPageSink.appendPage(Page)
  -> IcebergPageSink.finish()
  -> TableFinishOperator
  -> IcebergMetadata.finishCreateTable(...) or finishInsert(...)
```


That path is enough to understand CTAS and INSERT. It is not enough for DELETE
or MERGE, because row-level changes use a merge sink and `RowDelta` commit.


## CTAS vs INSERT


| Statement | Table state before write | Begin method            | Worker role          | Coordinator role                 | Iceberg commit idea                |
| --------- | ------------------------ | ----------------------- | -------------------- | -------------------------------- | ---------------------------------- |
| CTAS      | table does not exist     | `beginCreateTable(...)` | write new data files | create table and commit files    | append snapshot for new table      |
| INSERT    | table already exists     | `beginInsert(...)`      | write new data files | commit files into existing table | append snapshot for existing table |


The shared part is larger than the different part:


```text
both use TableWriterOperator
both use IcebergPageSink
both return commit fragments
both finish on the coordinator
both make data visible through Iceberg snapshot metadata
```


## What To Remember

- CTAS is create-table plus write-data.
- INSERT reuses the same append-style write model against an existing table.
- Workers write physical data files through `IcebergPageSink`.
- Workers return commit fragments, not final table metadata.
- The coordinator commits those fragments into Iceberg snapshot and manifest
metadata.
- The durable Iceberg table state is the snapshot, not just the files sitting in
storage.
- DELETE and MERGE are harder because they are row-change writes. CTAS and
INSERT are the simpler foundation.

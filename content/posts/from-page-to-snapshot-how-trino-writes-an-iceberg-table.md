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


This post follows the write path, where Trino’s in-memory `Page` batches
become Iceberg data files and Iceberg metadata.


For CTAS (Create Table As Select) and INSERT, the short version is:


```text
workers write data files
coordinator commits Iceberg metadata
```


Writing a Parquet file is only half of the work. Workers receive page batches,
write data files through the connector page sink, and return small commit
fragments. The coordinator gathers those fragments and asks Iceberg to commit a
new snapshot.


End to end, the call path looks like this:


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


This post only covers the append-style write path: CTAS and INSERT. DELETE and
MERGE are out of scope.


## The Setup


We will use a small Iceberg table so the write is easy to inspect:


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


This statement is a good starting point because it does two things at once:


```text
CREATE TABLE:
  define the Iceberg table schema, partitioning, and location

AS SELECT:
  run a query that produces rows and writes them into the new table
```


CTAS is DDL plus a write query, so it needs both a table-create path and a
data-write path.


## The Plan Shape


The distributed `EXPLAIN` output for this CTAS looks roughly like this:


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


The exact symbol names, fragment labels, and writer distribution can vary, but
four operators carry the write:


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


This is the same way of reading fragments as in the
[EXPLAIN post]({{< relref "how-to-read-a-trino-distributed-explain-plan.md" >}}):
follow `RemoteSource`, not the printed order.


## Phase 1: Workers Write Data Files


The worker-side write boundary is:


```text
upstream operator output
  -> Page
  -> TableWriterOperator.addInput(Page)
  -> ConnectorPageSink.appendPage(Page)
```


In our CTAS, the upstream operator is just `Values`:


```text
ValuesOperator
  -> TableWriterOperator
```


A CTAS that reads from another table would start with a scan instead:


```text
TableScanOperator
  -> filter/project work
  -> TableWriterOperator
```


A more complex `INSERT SELECT` could put joins, aggregations, exchanges, or sorts
upstream. The writer does not care where the page came from. It receives a
`Page` batch and passes the rows to the connector sink.


From there, the writer hands off to the connector:


```text
TableWriterOperator
  -> PageSinkManager
  -> IcebergPageSinkProvider
  -> IcebergPageSink
```


This is where execution leaves the engine and enters the Iceberg connector’s
writer.


## What `IcebergPageSink` Does


`IcebergPageSink.appendPage(...)` receives Trino pages and writes rows into
Iceberg data files.


A `Page` is an in-memory batch of rows in Trino’s columnar `Block` format, so
the page sink has to turn those pages into file output:


```text
incoming Page
  -> choose writer or partition writer
  -> write rows into Parquet
  -> close file writers at finish
  -> return CommitTaskData fragments
```


Our table is partitioned:


```text
partitioning = ARRAY['orderstatus']
```


so the sink may write rows into different partition outputs. The exact file
names and counts don't matter here. What matters is that worker tasks write data
files before the coordinator commits the table metadata.


When worker-side writing finishes, the sink closes its writers:


```text
IcebergPageSink.finish()
  -> closes file writers
  -> returns serialized commit fragments
```


Those fragments are small receipts that describe the written files. On their
own, they do not change the table snapshot.


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


Because this CTAS produces rows, the create-table finish path also has to commit
the data files. The Iceberg side turns worker fragments into `DataFile` records
and commits an append-style snapshot:


```text
commit fragments
  -> DataFile objects
  -> AppendFiles
  -> new Iceberg snapshot
  -> new manifest metadata
```


This step is what separates an Iceberg write from writing files under a
directory. The table state is the committed snapshot, and the data files become
visible only because the snapshot metadata points to them.


## INSERT Reuses The Same Shape


Once the table exists, we can append more rows:


```sql
INSERT INTO iceberg.write_trace.orders_delta
VALUES
    (5, 1005, 'F', CAST(50.00 AS DOUBLE)),
    (6, 1006, 'O', CAST(60.00 AS DOUBLE));
```


INSERT is simpler than CTAS because the table already exists. The first
difference is the begin path:


```text
CTAS:
  beginCreateTable(...)
  create a writable table handle for a new table

INSERT:
  beginInsert(...)
  create a writable table handle for an existing table
```


The worker side is the same as before:


```text
upstream pages
  -> TableWriterOperator
  -> IcebergPageSink.appendPage(...)
  -> IcebergPageSink.finish()
  -> commit fragments
```


On the coordinator, the finish method differs:


```text
CTAS:
  finishCreateTable(...)

INSERT:
  finishInsert(...)
```


and the Iceberg commit is still an append:


```text
new data files
  -> AppendFiles
  -> new snapshot
```


INSERT does not modify existing rows. It writes new files and commits new
metadata that makes those files part of the current table snapshot.


## What To Check In Iceberg


Iceberg's metadata tables let us confirm each step. After CTAS, check that the
table exists:


```sql
SHOW CREATE TABLE iceberg.write_trace.orders_delta;
```


Then look at the snapshots:


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


After CTAS, there should be one snapshot for the initial append. After INSERT,
there should be another.


Finally, list the current files:


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


For the scope of this post, these are the checks that matter:


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


Each of these sources tells us something different:


| Evidence          | What it proves                                                 |
| ----------------- | -------------------------------------------------------------- |
| `EXPLAIN`         | The planned write shape: `TableWriter` below `TableCommit`.    |
| `EXPLAIN ANALYZE` | Runtime write stats for that execution. It executes the write. |
| `$snapshots`      | Iceberg committed a new snapshot.                              |
| `$files`          | Current snapshot points to data files.                         |


Be careful with `EXPLAIN ANALYZE`. For a read query, it reads data and reports
runtime stats. For a CTAS or INSERT, it actually runs the write, so only use it
against a disposable trace table.


## Code Anchors


Once the plan shape makes sense, these are the source files to read:


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


If you want to step through it in a debugger, a good first set of breakpoints is:


```text
BeginTableWrite
  -> IcebergMetadata.beginCreateTable(...) or beginInsert(...)
  -> TableWriterOperator.addInput(Page)
  -> IcebergPageSink.appendPage(Page)
  -> IcebergPageSink.finish()
  -> TableFinishOperator
  -> IcebergMetadata.finishCreateTable(...) or finishInsert(...)
```


That path covers CTAS and INSERT. DELETE and MERGE need more, because row-level
changes go through a merge sink and a `RowDelta` commit.


## CTAS vs INSERT


| Statement | Table state before write | Begin method            | Worker role          | Coordinator role                 | Iceberg commit idea                |
| --------- | ------------------------ | ----------------------- | -------------------- | -------------------------------- | ---------------------------------- |
| CTAS      | table does not exist     | `beginCreateTable(...)` | write new data files | create table and commit files    | append snapshot for new table      |
| INSERT    | table already exists     | `beginInsert(...)`      | write new data files | commit files into existing table | append snapshot for existing table |


Most of the path is shared:


```text
both use TableWriterOperator
both use IcebergPageSink
both return commit fragments
both finish on the coordinator
both make data visible through Iceberg snapshot metadata
```


## Summary

CTAS is a create-table step plus a data write, and INSERT reuses the same
append-style write against an existing table. In both cases, workers write the
physical data files through `IcebergPageSink` and return commit fragments
rather than table metadata. The coordinator then commits those fragments as a
new Iceberg snapshot with its manifest metadata. The durable table state is
that snapshot; files sitting in storage are not part of the table until a
snapshot points to them.

DELETE and MERGE are harder because they change existing rows. CTAS and INSERT
are the simpler foundation to understand first.

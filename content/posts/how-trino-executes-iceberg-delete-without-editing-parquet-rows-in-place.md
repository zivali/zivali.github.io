---
title: How Trino Executes Iceberg DELETE Without Editing Parquet Rows In Place
date: 2026-07-13T01:13:00.000Z
lastmod: 2026-07-13T01:27:00.000Z
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


# Iceberg DELETE Is Not An In-Place Row Update


A common assumption is that DELETE works like this:


```text
DELETE finds a row inside a Parquet file and edits that file in place.
```


That is not how Iceberg row-level DELETE works. What actually happens is:


```text
Iceberg DELETE changes table metadata.
```


Sometimes the metadata change can remove whole data files from the current
snapshot. Other times Trino has to read matching rows, carry row identity, write
delete information, and commit a new Iceberg row-change snapshot.


Side by side, the two paths look like this:


```text
metadata DELETE:
  remove whole matching files from Iceberg metadata

row-level DELETE:
  read matching rows
  identify file and row position
  write delete metadata
  commit a new RowDelta snapshot
```


This post walks through both paths using one small table. It builds on the
CTAS and INSERT write path from the previous note, but DELETE is harder because
it does more than append new data files.


## The Setup


We'll reuse the same small table shape as the write-path note:


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


Because the table is partitioned by `orderstatus`, it can show two different
DELETE shapes:


```sql
DELETE FROM iceberg.write_trace.orders_delta
WHERE orderstatus = 'P';
```


and:


```sql
DELETE FROM iceberg.write_trace.orders_delta
WHERE orderkey = 1;
```


The first query is a candidate for metadata DELETE because `orderstatus` is the
identity partition column. The second query is the better row-level DELETE
trace because `orderkey` is a regular data column.


## Path 1: Metadata DELETE


A metadata DELETE is the simpler path. If the predicate can be represented as whole-file or whole-partition removal,
Trino can let the Iceberg connector remove matching files from the current
table snapshot.


At the plan level, the flow is:


```text
DELETE predicate
  -> connector accepts file-level delete
  -> TableDelete
  -> Iceberg DeleteFiles commit
  -> new snapshot
```


This does not mean Trino edited rows inside a Parquet file. It also does not
mean the physical Parquet file is immediately deleted from storage. It means
the current Iceberg snapshot no longer points to files that match the delete
predicate.


For a table partitioned by `orderstatus`, this query may fit that shape:


```sql
DELETE FROM iceberg.write_trace.orders_delta
WHERE orderstatus = 'P';
```


If all rows in a matching data file belong to `orderstatus = 'P'`, Iceberg can
drop that file from the visible snapshot.


The difference between the snapshot and storage matters because Iceberg
supports snapshot-based reads. An older
snapshot may still reference the data file that the current snapshot no longer
uses. That is what makes time travel possible.


In the plan, this path shows up as:


```text
TableDelete
```


On the Iceberg side, we can check the snapshots and files metadata tables:


```sql
SELECT
    committed_at,
    snapshot_id,
    operation,
    summary
FROM iceberg.write_trace."orders_delta$snapshots"
ORDER BY committed_at DESC;
```


and:


```sql
SELECT
    content,
    file_path,
    record_count,
    partition
FROM iceberg.write_trace."orders_delta$files"
ORDER BY content, file_path;
```


The exact file names do not matter. What we want to see is whether the
current snapshot stopped referencing a matching data file.


To make the time-travel behavior concrete, capture the snapshot id before and
after the metadata DELETE:


```sql
SELECT
    committed_at,
    snapshot_id,
    parent_id,
    operation
FROM iceberg.write_trace."orders_delta$snapshots"
ORDER BY committed_at;
```


Then query the old snapshot by version:


```sql
SELECT *
FROM iceberg.write_trace.orders_delta
FOR VERSION AS OF 1234567890123456789
ORDER BY orderkey;
```


Use the real `snapshot_id` from the metadata table in place of
`1234567890123456789`. If the old snapshot is still retained, that query reads
the table as it existed at that snapshot. The current table no longer shows the
deleted rows, but the old snapshot may still need the old data file.


That is why physical cleanup is a separate maintenance concern. Once old
snapshots are expired and no retained snapshot references a file anymore,
Iceberg maintenance can remove files that are no longer reachable from the
table history.


## Path 2: Row-Level DELETE


The second query is the clearer row-level example:


```sql
DELETE FROM iceberg.write_trace.orders_delta
WHERE orderkey = 1;
```


`orderkey` is not the partition column. Iceberg cannot usually remove a whole
file just because one row inside the file has `orderkey = 1`.


So the write path has to carry row identity:


```text
which file?
which row position inside that file?
```


This is the main difference from CTAS and INSERT. Those statements write new
data rows, while row-level DELETE reads existing rows to produce delete
coordinates.


## The Row-Level Plan Shape


The compact distributed `EXPLAIN` shape is:


```text
Fragment 0 [COORDINATOR_ONLY]
  TableCommit[target = iceberg:write_trace.orders_delta$data@...]
    RemoteSource[sourceFragmentIds = [1]]

Fragment 1 [MERGE [insert = HASH]]
  MergeWriter[table = iceberg:write_trace.orders_delta$data@...]
    RemoteSource[sourceFragmentIds = [2]]

Fragment 2 [SOURCE]
  ScanFilterProject[
    table = iceberg:write_trace.orders_delta$data@...,
    filterPredicate = (orderkey = integer '1')]

    operation := tinyint '2'
    case_number := integer '0'
    insert_from_update := tinyint '0'
    field := $merge_row_id(
      _file,
      _pos,
      partition_spec_id,
      partition_data,
      source_row_id)
```


The exact symbols can vary, but five pieces are worth reading closely:


| Plan piece                 | Meaning                                                |
| -------------------------- | ------------------------------------------------------ |
| `ScanFilterProject`        | Reads rows matching `orderkey = 1`.                    |
| `operation := tinyint '2'` | Marks the row-change operation as DELETE.              |
| `$merge_row_id(...)`       | Carries the physical row identity.                     |
| `MergeWriter`              | Writes delete information through the row-change sink. |
| `TableCommit`              | Commits the final Iceberg row-change snapshot.         |


As in the published [EXPLAIN post]({{< relref "how-to-read-a-trino-distributed-explain-plan.md" >}}), the printed order starts at fragment `0`,
but the data flows the other way:


```text
Fragment 2
  scan matching rows and produce delete coordinates

Fragment 1
  write delete information

Fragment 0
  commit the Iceberg metadata update
```


## Fragment 2: Read And Identify Rows


Fragment 2 is the source side of the plan:


```text
ScanFilterProject[
  filterPredicate = (orderkey = integer '1')]
```


It reads the matching row, but its output is more than the user columns
`orderkey`, `custkey`, `orderstatus`, and `totalprice`. The row-level DELETE
plan also creates control columns:


```text
operation := tinyint '2'
case_number := integer '0'
insert_from_update := tinyint '0'
field := $merge_row_id(...)
```


The one that matters most for DELETE is the row id:


```text
field := $merge_row_id(...)
```


For Iceberg, that row id includes:


```text
_file:
  the data file that currently contains the row

_pos:
  the row position inside that data file

partition_spec_id and partition_data:
  partition context needed by the connector

source_row_id:
  source row identity carried through the row-change path
```


This is what shows that Trino is not editing the Parquet row directly. It reads
the row so the connector can later say:


```text
hide the row at this file and position
```


## Fragment 1: Write Delete Information


Fragment 1 receives these rows from fragment 2:


```text
RemoteSource[sourceFragmentIds = [2]]
```


and passes them to `MergeWriter`:


```text
MergeWriter[table = iceberg:write_trace.orders_delta$data@...]
```


The name `MergeWriter` can be confusing in a DELETE plan. It does not mean the
SQL statement is `MERGE`; it means Trino is using its row-change writer
machinery. DELETE, UPDATE, and MERGE all need a way to send row-change commands
to the connector.


For row-level DELETE, `MergeWriter` sends delete rows into the Iceberg
row-change sink. The connector groups deleted row positions by referenced data
file.


What gets written depends on the Iceberg table format version:


| Iceberg format | Row-level delete representation |
| -------------- | ------------------------------- |
| Format v2      | Position delete files.          |
| Format v3      | Deletion vectors can be used.   |


In both cases:


```text
the old Parquet data file is not rewritten in place
```


Instead, Iceberg records delete metadata that changes which rows are visible in
the table snapshot.


## Fragment 0: Commit The RowDelta


Fragment 0 runs only on the coordinator:


```text
Fragment 0 [COORDINATOR_ONLY]
  TableCommit
    RemoteSource[sourceFragmentIds = [1]]
```


The workers have already produced delete fragments. The coordinator gathers
those fragments and asks Iceberg to commit the row-level change.


For row-level DELETE, the Iceberg commit type is:


```text
RowDelta
```


The commit adds delete metadata to a new snapshot. After the commit, readers use
Iceberg metadata to understand that the old row is no longer visible.


Compared with CTAS and INSERT:


```text
CTAS / INSERT:
  write data files
  commit AppendFiles

row-level DELETE:
  identify existing row positions
  write delete metadata
  commit RowDelta
```


Both paths end in a coordinator-side Iceberg commit. The difference is what the
workers produce before that commit.


## What To Check In Iceberg


Check snapshots after the DELETE:


```sql
SELECT
    committed_at,
    snapshot_id,
    parent_id,
    operation,
    summary
FROM iceberg.write_trace."orders_delta$snapshots"
ORDER BY committed_at DESC;
```


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


For row-level DELETE, we expect:


```text
before DELETE:
  current snapshot points to data files

after DELETE:
  new snapshot exists
  delete-file or deletion-vector effects may appear
  the deleted row is no longer visible to normal queries
```


Finally, confirm the visible table result:


```sql
SELECT *
FROM iceberg.write_trace.orders_delta
ORDER BY orderkey;
```


The deleted `orderkey = 1` row should not appear after the row-level DELETE.


## What The Evidence Proves


| Evidence                     | What it proves                                                           |
| ---------------------------- | ------------------------------------------------------------------------ |
| `EXPLAIN` with `TableDelete` | Trino planned a connector metadata DELETE path.                          |
| `EXPLAIN` with `MergeWriter` | Trino planned a row-change DELETE path.                                  |
| `$merge_row_id` in the plan  | The writer needs physical row identity: file and position.               |
| `$snapshots`                 | Iceberg committed a new table snapshot.                                  |
| `$files`                     | Current snapshot file state, including delete-file effects when visible. |
| Final `SELECT`               | The row is no longer visible through the table.                          |


None of this shows Parquet bytes being edited in place. The plan points the
other way: Trino reads the row, carries its file
and position, writes delete metadata, and commits a new snapshot.


## Code Anchors


Once the plan shape is clear, these are the places to look in the Trino source:


| Concept                                          | Code area                                                                                                   |
| ------------------------------------------------ | ----------------------------------------------------------------------------------------------------------- |
| Build DELETE plan                                | `core/trino-main/src/main/java/io/trino/sql/planner/LogicalPlanner.java`                                    |
| Optimize whole-file DELETE into connector DELETE | `core/trino-main/src/main/java/io/trino/sql/planner/iterative/rule/PushMergeWriterDeleteIntoConnector.java` |
| Worker row-change writer                         | `core/trino-main/src/main/java/io/trino/operator/MergeWriterOperator.java`                                  |
| Iceberg row-change sink                          | `plugin/trino-iceberg/src/main/java/io/trino/plugin/iceberg/IcebergMergeSink.java`                          |
| Iceberg metadata commit                          | `plugin/trino-iceberg/src/main/java/io/trino/plugin/iceberg/IcebergMetadata.java`                           |


I do not need to start with these files. The plan shape comes first. The code
anchors are only for checking the handoff after the fragments make sense.


## Comparison Table


| Statement        | Main write idea                                       | Worker role                            | Coordinator role              | Iceberg commit idea   |
| ---------------- | ----------------------------------------------------- | -------------------------------------- | ----------------------------- | --------------------- |
| CTAS             | create table and append data                          | write new data files                   | create table and commit files | append snapshot       |
| INSERT           | append data to existing table                         | write new data files                   | commit files                  | append snapshot       |
| Metadata DELETE  | remove whole matching files from the current snapshot | little or no row-level writer work     | execute connector delete      | delete-files snapshot |
| Row-level DELETE | hide specific rows                                    | read row ids and write delete metadata | commit row-change fragments   | `RowDelta`            |
| MERGE            | read, classify, write inserts/deletes                 | write insert and delete outputs        | commit row-change fragments   | `RowDelta`            |


Putting the statements next to each other makes one thing clear:


```text
DELETE is not one physical behavior.
```


A metadata DELETE can remove whole files from the current snapshot without
physically deleting those files immediately. A row-level DELETE has to preserve
row identity and commit delete metadata. Neither path means “open the Parquet
file and edit the old row in place.”


## Summary

Iceberg tables are snapshot-based, and DELETE changes which files or rows are
visible in a new snapshot. When the predicate allows it, a metadata DELETE
removes whole matching files from the current snapshot, though the physical
file may remain while older snapshots can still time-travel to it.

Row-level DELETE reads matching rows and carries `$merge_row_id`, which includes
file and position information. Format v2 writes position delete files, and
format v3 can use deletion vectors. `MergeWriter` in a DELETE plan means the
row-change writer machinery, not necessarily a SQL `MERGE` statement. In
neither case is the old Parquet row edited in place.

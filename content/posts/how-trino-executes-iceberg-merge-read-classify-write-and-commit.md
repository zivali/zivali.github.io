---
title: "How Trino Executes Iceberg MERGE: Read, Classify, Write, And Commit"
date: 2026-07-19T00:48:00.000Z
lastmod: 2026-07-19T00:51:00.000Z
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


# How Trino Executes Iceberg MERGE


`MERGE` combines the two write paths covered in the previous notes in this series:

- From Page To Snapshot: How Trino Writes An Iceberg Table
- How Trino Executes Iceberg DELETE Without Editing Parquet Rows In Place

The CTAS and INSERT note followed the append path:


```text
write new data files
  -> return commit fragments
  -> commit an Iceberg snapshot
```


The DELETE note followed the row-level delete path:


```text
read existing row identity
  -> write delete metadata
  -> commit a RowDelta snapshot
```


A `MERGE` can insert new rows, delete old rows, and update existing rows, so it
needs both. In Iceberg, an update is not an in-place Parquet edit. It is stored
as delete metadata for the old row plus new data-file work for the new row
values.


In one line:


```text
MERGE is read, classify, write, and commit.
```


Trino reads the source and target rows, decides which `WHEN` branch each joined
row takes, converts those SQL actions into physical row-change rows, writes the
inserts and deletes through the connector, and then commits one Iceberg
`RowDelta`.


The shape I want to remember is:


```text
MERGE SQL
  -> target/source join
  -> merge_row
  -> duplicate-match check
  -> MergeProcessor
  -> operation rows
  -> MergeWriter
  -> IcebergMergeSink
  -> data files and delete metadata
  -> TableCommit
  -> Iceberg RowDelta snapshot
```


This post assumes the earlier notes and follows the row-change trace from there,
rather than introducing writes from scratch.


## The Example


We start with an Iceberg table that already has a few rows:


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


Then we run a `MERGE` with one matched update and one not-matched insert:


```sql
MERGE INTO iceberg.write_trace.orders_delta t
USING (
    VALUES
        (3, 2003, 'F', CAST(1234.50 AS DOUBLE)),
        (7, 2007, 'O', CAST(999.99 AS DOUBLE))
) s(orderkey, custkey, orderstatus, totalprice)
ON t.orderkey = s.orderkey
WHEN MATCHED THEN UPDATE SET totalprice = s.totalprice
WHEN NOT MATCHED THEN INSERT (orderkey, custkey, orderstatus, totalprice)
VALUES (s.orderkey, s.custkey, s.orderstatus, s.totalprice);
```


The intended result is:


| Source row     | Target match? | SQL action                       |
| -------------- | ------------- | -------------------------------- |
| `orderkey = 3` | yes           | update `totalprice` to `1234.50` |
| `orderkey = 7` | no            | insert a new row                 |


These two rows exercise both sides of the row-change writer:


```text
matched UPDATE:
  delete the old target row
  insert the new row values

not-matched INSERT:
  insert the new row values
```


In neither case does Iceberg edit an existing Parquet row in place.


The update is where this connects to the DELETE note:


```text
UPDATE orderkey = 3
  -> delete old row position for orderkey = 3
  -> write a new row version with totalprice = 1234.50
```


The insert is where it connects to the CTAS/INSERT note:


```text
INSERT orderkey = 7
  -> write new row values into data files
```


## The Plan Shape


Here is the relevant part of the distributed `EXPLAIN` output:


```text
Fragment 0 [COORDINATOR_ONLY]
  TableCommit[target = iceberg:write_trace.orders_delta$data@...]
    RemoteSource[sourceFragmentIds = [1]]

Fragment 1 [MERGE [insert = HASH]]
  MergeWriter[table = iceberg:write_trace.orders_delta$data@...]
    RemoteSource[sourceFragmentIds = [2]]

Fragment 2 [HASH]
  MergeProcessor[]
    target: iceberg:write_trace.orders_delta$data@...
    merge row column: merge_row
    row id column: field
    redistribution columns: [orderstatus]
    data columns: [orderkey, custkey, orderstatus, totalprice]
  Filter[duplicate target-row match check]
    MarkDistinct[distinct = [unique_id, case_number], marker = is_distinct]
      RemoteSource[sourceFragmentIds = [3]]

Fragment 3 [SINGLE]
  Project[]
    unique_id := COALESCE(target_unique_id, source_unique_id)
    case_number := merge_row.6
  Project[]
    merge_row := CASE
      WHEN present THEN ROW(..., tinyint '3', integer '0')
      WHEN present IS NULL THEN ROW(..., tinyint '1', integer '1')
  LeftJoin[criteria = (field_0 = orderkey), distribution = REPLICATED]
    Values[]
      (3, 2003, 'F', 1234.5)
      (7, 2007, 'O', 999.99)
    RemoteSource[sourceFragmentIds = [4]]

Fragment 4 [SOURCE]
  ScanFilter[
    table = iceberg:write_trace.orders_delta$data@...,
    filterPredicate = (orderkey IN (3, 7))]
  field := $merge_row_id(
    _file, _pos, partition_spec_id, partition_data, source_row_id)
```


Trino prints the root fragment first, which is not the most useful reading
order. As in the
[EXPLAIN post]({{< relref "how-to-read-a-trino-distributed-explain-plan.md" >}}),
we follow the `RemoteSource` links instead of the printed order:


```text
Fragment 0 reads Fragment 1.
Fragment 1 reads Fragment 2.
Fragment 2 reads Fragment 3.
Fragment 3 reads Fragment 4.
Fragment 4 scans the target table.
```


Read bottom-up, the data flow is:


```text
Fragment 4
  scan target rows and expose row ids

Fragment 3
  read source VALUES
  join source rows to target rows
  build merge_row

Fragment 2
  check duplicate target matches
  convert SQL actions into physical row-change rows

Fragment 1
  write insert and delete pages through MergeWriter

Fragment 0
  commit one Iceberg RowDelta
```


Fragment 4 reads the target, Fragment 3 compares source with target and picks
the `MERGE` branch, Fragment 2 prepares validated row-change records, Fragment 1
writes them through `MergeWriter`, and Fragment 0 commits the Iceberg snapshot
and metadata. The rest of the post walks through the fragments in that order.


## Fragment 4: Read Target Rows


The target scan sits at the bottom of the table side:


```text
Fragment 4 [SOURCE]
  ScanFilter[
      table = iceberg:write_trace.orders_delta$data@...,
      filterPredicate = (orderkey IN (3, 7))]
```


The source values contain only `orderkey = 3` and `orderkey = 7`, so Trino only
needs target rows that can match those keys.


The scan also produces a hidden column:


```text
field := $merge_row_id(...)
```


For Iceberg, this row id includes the old row's file and position:


```text
"_file" varchar
"_pos" bigint
"partition_spec_id" integer
"partition_data" varchar
"source_row_id" bigint
```


This row id is what later lets the connector delete the old physical row, and it
is why the row-level write path needs more than the normal table columns.


Fragment 4 also adds:


```text
target_unique_id
present := true
```


`present` marks that a target row exists. Later, when `present IS NULL`, the
source row did not match any target row and takes the insert branch.


## Fragment 3: Join Source And Target


Fragment 3 reads the source rows:


```text
Values[]
  (3, 2003, 'F', 1234.5)
  (7, 2007, 'O', 999.99)
```


and joins them to the target rows:


```text
LeftJoin[criteria = (field_0 = orderkey), distribution = REPLICATED]
```


Here `field_0` is the source `orderkey`. The target side contributes the target
columns, the row id, `target_unique_id`, and `present`.


The output we care about is `merge_row`:


```text
merge_row := CASE
```


`merge_row` packs the SQL-level row-change instruction into one value. In this
plan it is built as:


```text
WHEN present THEN ROW(..., tinyint '3', integer '0')
WHEN present IS NULL THEN ROW(..., tinyint '1', integer '1')
```


At this stage, the operation numbers still carry SQL `MERGE` branch meanings:


| Operation number | SQL meaning |
| ---------------- | ----------- |
| `1`              | `INSERT`    |
| `2`              | `DELETE`    |
| `3`              | `UPDATE`    |


For our query that gives:


```text
source orderkey = 3 matched target row
  -> merge_row operation = 3

source orderkey = 7 did not match target row
  -> merge_row operation = 1
```


Fragment 3 also creates two more symbols:


```text
case_number := merge_row.6
unique_id := COALESCE(target_unique_id, source_unique_id)
```


These feed the duplicate-match check in the next fragment. Fragment 3 creates
the values but does not run `MarkDistinct` itself.


## Fragment 2: Check And Classify


Fragment 2 is where the plan moves from SQL `MERGE` semantics to physical
row-change rows.


It starts with the duplicate-match check:


```text
MarkDistinct[distinct = [unique_id, case_number], marker = is_distinct]
Filter[
  CASE WHEN NOT is_distinct
  THEN fail('One MERGE target table row matched more than one source row')
  ELSE true
  END]
```


This enforces the SQL rule that a target table row cannot be modified by more
than one source row.


Then `MergeProcessor` classifies the rows:


```text
MergeProcessor[]
  merge row column: merge_row
  row id column: field
  redistribution columns: [orderstatus]
  data columns: [orderkey, custkey, orderstatus, totalprice]
```


The two fragments answer different questions:


```text
Fragment 3:
  Which SQL MERGE branch matched?

Fragment 2:
  What physical row-change rows does the connector writer need?
```


On this Iceberg path, a logical update becomes a delete plus an insert:


```text
SQL UPDATE
  -> UPDATE_DELETE operation number = 5
  -> UPDATE_INSERT operation number = 4
```


The unmatched insert stays an insert:


```text
SQL INSERT
  -> INSERT operation number = 1
```


As a result, the output of Fragment 2 carries both table data columns and
control columns:


```text
[orderkey, custkey, orderstatus, totalprice,
 operation, case_number_4, field, insert_from_update]
```


`field` carries the old target row id for deletes, and the data columns carry
the values for newly inserted rows. `insert_from_update` is bookkeeping: an
update physically becomes one delete row plus one insert row, and this flag lets
it still be counted as one SQL update.


## Fragment 1: Write Inserts And Deletes


Fragment 1 is the worker-side write fragment:


```text
Fragment 1 [MERGE [insert = HASH]]
  MergeWriter[table = iceberg:write_trace.orders_delta$data@...]
```


It receives the already-classified rows from Fragment 2:


```text
RemoteSource[sourceFragmentIds = [2]]
```


and arranges them for the merge writer:


```text
LocalExchange[
  partitioning = MERGE [insert = HASH],
  arguments = [operation::tinyint, orderstatus::varchar]]
```


The partitioning label is easy to misread:


```text
MERGE [insert = HASH]
```


It does not mean the statement only inserts. Insert-like rows are
hash-partitioned by the insert layout, while delete rows still carry the target
row id the connector needs. In this table `orderstatus` is the partition column,
so it shows up in the write partitioning.


By this point, `MergeWriter` is no longer deciding which SQL branch matched. It
only writes rows that have already been classified.


In the Iceberg connector, the handoff looks like this:


```text
MergeWriter
  -> IcebergMergeSink.storeMergedRows(...)
  -> MergePage.createDeleteAndInsertPages(...)
```


The connector splits each incoming page into two parts:


```text
delete rows:
  old target row ids grouped by file and row position

insert rows:
  new row values sent to the inner Iceberg page sink
```


With format version 2, deleted rows become position delete information. With
format version 3, Iceberg can use deletion-vector information instead. Either
way, the old Parquet row is hidden by delete metadata rather than edited in
place.


## Fragment 0: Commit Once


Fragment 0 runs on the coordinator:


```text
Fragment 0 [COORDINATOR_ONLY]
  TableCommit[target = iceberg:write_trace.orders_delta$data@...]
```


It receives writer fragments from Fragment 1:


```text
RemoteSource[sourceFragmentIds = [1]]
```


and gathers them into one stream:


```text
LocalExchange[partitioning = SINGLE]
```


This is the commit boundary. The workers have written their physical output and
returned fragments, and the coordinator now asks the connector to finish the
write.


For an Iceberg `MERGE`, the commit looks like this:


```text
IcebergMetadata.finishMerge(...)
  -> finishWrite(...)
  -> transaction.newRowDelta()
  -> rowDelta.addRows(...)       for inserted data files
  -> rowDelta.addDeletes(...)    for position delete files
  -> commit
```


So the final snapshot can contain both kinds of output:


```text
new data files:
  inserted rows and the new version of updated rows

delete metadata:
  old target row positions removed from the visible table state
```


Put together, an update in an Iceberg `MERGE` is:


```text
MERGE UPDATE in Iceberg
  = delete old row metadata
  + write new row values
  + commit both in one RowDelta
```


## What To Check In Iceberg


After running the `MERGE`, two system tables give a quick sanity check.


First, confirm that a new snapshot exists:


```sql
SELECT
    committed_at,
    snapshot_id,
    operation,
    summary
FROM iceberg.write_trace."orders_delta$snapshots"
ORDER BY committed_at DESC;
```


Then look at the current files:


```sql
SELECT
    content,
    file_path,
    file_format,
    record_count
FROM iceberg.write_trace."orders_delta$files"
ORDER BY content, file_path;
```


The exact rows depend on the local run, the format version, and whether delete
files or deletion vectors are used. What you should expect to see is:


```text
insert output:
  new data-file work for orderkey = 7 and the new version of orderkey = 3

delete output:
  delete metadata for the old target row where orderkey = 3
```


## Code Anchors


Once the plan shape is clear, these are the places to look in the source:


| Plan idea                                      | Code area                                                                          |
| ---------------------------------------------- | ---------------------------------------------------------------------------------- |
| Build target/source merge plan                 | `core/trino-main/src/main/java/io/trino/sql/planner/QueryPlanner.java`             |
| Convert SQL actions into row-change rows       | `core/trino-main/src/main/java/io/trino/operator/MergeProcessorOperator.java`      |
| Send classified rows into connector merge sink | `core/trino-main/src/main/java/io/trino/operator/MergeWriterOperator.java`         |
| Split delete and insert pages for Iceberg      | `plugin/trino-iceberg/src/main/java/io/trino/plugin/iceberg/IcebergMergeSink.java` |
| Finish the Iceberg row-change commit           | `plugin/trino-iceberg/src/main/java/io/trino/plugin/iceberg/IcebergMetadata.java`  |


## Summary

Read a `MERGE` plan by following `RemoteSource`, not the printed fragment order.
Fragment 4 scans the target rows and exposes `$merge_row_id`. Fragment 3 joins
source rows to target rows and builds `merge_row`. Fragment 2 runs the
duplicate-match check and `MergeProcessor`, which turns SQL operation `3`
(logical `UPDATE`) into `UPDATE_DELETE` plus `UPDATE_INSERT` rows. Fragment 1
runs `MergeWriter` and hands the classified rows to `IcebergMergeSink`, and
Fragment 0 commits the final Iceberg `RowDelta`. At no point is a Parquet row
edited in place.

## Self-Check


Questions to answer without looking back:

- Why is `MERGE` read, classify, write, and commit?
- Which fragment scans the target table?
- What does `$merge_row_id` let Iceberg do later?
- What does Fragment 3 build before `MergeProcessor` runs?
- Why does `MarkDistinct` belong in Fragment 2, not Fragment 3?
- What is the difference between SQL operation `3` and physical
`UPDATE_DELETE` / `UPDATE_INSERT` rows?
- Why does `MERGE [insert = HASH]` not mean the statement only inserts?
- Which fragment contains `TableCommit`?
- What should `$snapshots` and `$files` prove after the merge?

## References

- Trino `MERGE`: https://trino.io/docs/current/sql/merge.html
- Trino Iceberg connector: https://trino.io/docs/current/connector/iceberg.html
- Apache Iceberg row-level deletes: https://iceberg.apache.org/spec/#row-level-deletes

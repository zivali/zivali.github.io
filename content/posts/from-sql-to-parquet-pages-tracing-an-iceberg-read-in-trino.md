---
title: |-
  
  From SQL To Parquet Pages: Tracing An Iceberg Read In Trino
date: 2026-06-17T03:06:00.000Z
lastmod: 2026-07-04T21:19:00.000Z
draft: false
series:
  - Learning Trino by Tracing One Iceberg Table from SQL to Files
tags:
  - Trino
  - Iceberg
  - Parquet
categories: []
authors:
  - Ziva Li
---
<link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/katex@0.16.2/dist/katex.min.css" integrity="sha384-bYdxxUwYipFNohQlHt0bjN/LCpueqWz13HufFEV1SUatKs1cm4L6fFgCi1jT643X" crossorigin="anonymous">


# From SQL To Parquet Pages


The read path is where the Trino engine, the Iceberg connector, Iceberg
metadata, Parquet files, and Trino `Page` objects all meet. You could follow it
as one long chain of classes, but I find it easier to follow by asking one
question at each step:


```text
Where does the query cross a boundary?
```


For an Iceberg read, the boundaries are:


```text
SQL
  -> plan
  -> Iceberg table handle
  -> predicate pushdown
  -> Iceberg metadata scan
  -> IcebergSplit
  -> worker split assignment
  -> connector page source
  -> Parquet reader
  -> Trino Page
  -> engine operators
```


In this note we trace one small query through each of those handoffs.


## The Setup


The table is an Iceberg table built from TPC-H data:


```sql
CREATE TABLE iceberg.tpch.orders
WITH (
    format = 'PARQUET',
    partitioning = ARRAY['o_orderstatus']
) AS
SELECT *
FROM tpch.tiny.orders;
```


The rows originally come from TPC-H, but the table we trace is:


```text
iceberg.tpch.orders
```


The read path therefore goes through the Iceberg connector, Iceberg split
generation, and the Parquet reader. Querying `tpch.tiny.orders` directly would
trace the TPC-H connector instead.


The query is:


```sql
SELECT *
FROM iceberg.tpch.orders
WHERE o_orderstatus = 'F';
```


Because the table is partitioned by `o_orderstatus`, this makes a good first
trace: the filter is simple to follow, and it should show up as an Iceberg
partition constraint.


## The Plan Evidence


Here is the compact `EXPLAIN` output:


```text
Fragment 0 [SOURCE]
  Output
    TableScan[
      table = iceberg:tpch.orders$data@...
      constraint on [o_orderstatus]]

      o_orderstatus := 3:o_orderstatus:varchar
          :: [[F]]
```


On its own, this shows only one thing:


```text
Trino planned a scan of the Iceberg table with a constraint on o_orderstatus.
```


It says nothing about how many bytes were read, how many splits ran, or how
many rows came back at runtime. `EXPLAIN` gives plan shape and estimates. For
runtime numbers we need `EXPLAIN ANALYZE`:


```text
TableScan[table = iceberg:tpch.orders$data@... constraint on [o_orderstatus]]
  Output: 7304 rows
  Input: 7304 rows (1012.76kB)
  Physical input: 169.66kB
  Splits: 1
  Splits generation wait time: 3.63ms
```


Side by side, the two commands answer different questions:


```text
EXPLAIN:
  planned scan shape and connector constraint

EXPLAIN ANALYZE:
  actual input rows, physical input bytes, and split count
```


For this query the plan is a single source fragment. Since the query is a
simple scan that returns rows, there is no `RemoteSource` chain to rebuild. The
fragment-reading habit from the previous post will matter again later, but this
post stays at the scan boundary.


## The Table Handle


Before workers read any bytes, the coordinator has to resolve the table. That
happens here:


```text
MetadataManager.getTableHandle(...)
  -> IcebergMetadata.getTableHandle(...)
  -> IcebergTableHandle
```


The table handle does not hold table data. It is the connector-specific object
Trino carries through planning and execution to represent this scan. For
Iceberg, the handle is where I expect to find facts such as:


```text
schema and table name
snapshot id
projected columns
enforced predicate
unenforced predicate
remaining predicate
table location
```


The snapshot id matters because Iceberg tables are snapshot-based. The query
reads the files that belong to the selected snapshot, not “whatever files exist
under a directory.” The metadata chain behind that snapshot looks like this:


```text
catalog
  -> current metadata.json
    -> current snapshot
      -> manifest list
        -> manifest files
          -> data files
```


All of this is Iceberg metadata work, and it happens before the Parquet reader
starts decoding row data.


## Predicate Pushdown


The SQL filter:


```sql
WHERE o_orderstatus = 'F'
```


becomes a predicate that the optimizer tries to push into the table scan,
through this path:


```text
PushPredicateIntoTableScan
  -> MetadataManager.applyFilter(...)
  -> IcebergMetadata.applyFilter(...)
```


Inside Iceberg, the predicate is sorted into three buckets:


```text
enforced predicate:
  the connector can guarantee this from metadata or partition knowledge

unenforced predicate:
  the connector can use it for pruning or reader work, but cannot fully
  guarantee it as the only correctness check

remaining predicate:
  the engine must still evaluate this above the scan
```


In this table, `o_orderstatus` is an identity partition column, so Iceberg
partition metadata can enforce `o_orderstatus = 'F'`. The plan agrees:


```text
TableScan[... constraint on [o_orderstatus]]
o_orderstatus := 3:o_orderstatus:varchar
    :: [[F]]
```


A more interesting example, which I'll come back to later, adds a second filter:


```sql
SELECT o_orderkey, o_totalprice
FROM iceberg.tpch.orders
WHERE o_orderstatus = 'F'
  AND o_totalprice > 1000;
```


The two predicates likely land in different buckets:


| Predicate             | Likely bucket           | Why                                                                                                           |
| --------------------- | ----------------------- | ------------------------------------------------------------------------------------------------------------- |
| `o_orderstatus = 'F'` | enforced                | It is the identity partition column.                                                                          |
| `o_totalprice > 1000` | unenforced or remaining | Iceberg may use file statistics, but partition metadata alone does not prove every returned row satisfies it. |


So when reading a plan, keep this in mind:


```text
"Pushed down" does not always mean "fully enforced by the connector."
```


## From Metadata To Splits


Once planning is done, the scheduler needs source work to hand to workers. For a
table scan, that means generating splits. On the engine side, the path is:


```text
SplitSourceFactory.visitTableScan(...)
  -> SplitSourceFactory.createSplitSource(...)
  -> SplitManager.getSplits(...)
  -> connector split manager
```


For Iceberg, that becomes:


```text
IcebergSplitManager.getSplits(...)
  -> IcebergSplitManager.getScan(...)
  -> IcebergSplitSource.getNextBatch(...)
  -> IcebergSplitSource.processFileScanTask(...)
  -> IcebergSplitSource.toIcebergSplit(...)
```


This is where the table handle turns into concrete read work.
`IcebergSplitManager.getScan(...)` builds an Iceberg scan from the selected
snapshot and projected columns. `IcebergSplitSource` plans file tasks and turns
those tasks into Trino connector splits.


Each `IcebergSplit` is one unit of read work. It can carry:


```text
file path
byte range
file size
record count
file format
partition values
delete files
file statistics domain
```


For this query, `EXPLAIN ANALYZE` reported:


```text
Splits: 1
```


So in the traced run, this simple partition read executed one split. The
division of labor between the two systems is:


```text
Iceberg metadata chooses which files or file ranges are worth scanning.
Trino splits make that selected work schedulable on workers.
```


## Worker Read Path


When a worker receives a split, it needs a connector page source. The engine
asks for one through:


```text
PageSourceManager.createPageSource(...)
```


For Iceberg, the connector path is:


```text
IcebergPageSourceProvider.createPageSource(...)
  -> IcebergPageSourceProvider.createDataPageSource(...)
  -> IcebergPageSourceProvider.createParquetPageSource(...)
```


At this point we are past metadata and splits. The worker opens the selected
data file and creates a reader for its file format. This table uses Parquet, so
the path continues into:


```text
MetadataReader.readFooter(...)
  -> getFilteredRowGroups(...)
  -> ParquetReader
  -> ParquetPageSource
```


Parquet adds its own pruning layer. Iceberg can prune manifests and files before
any data file is opened, and Parquet can then use row group, column chunk, and
page metadata inside a selected file. The two layers work on different things:


```text
Iceberg:
  table snapshot, manifests, data files, partition values, file metrics

Parquet:
  row groups, column chunks, encoded pages, footer statistics
```


Keeping them separate avoids a common confusion. Iceberg did not “read a Parquet
row group” during manifest pruning; it selected data files. The Parquet reader
deals with row groups after a file is selected.


## From Parquet To Trino Page


On the worker, the operator that pulls pages is usually either:


```text
TableScanOperator.getOutput()
```


or:


```text
ScanFilterAndProjectOperator
```


Which one you get depends on whether the optimizer leaves filter or project work
above the scan. Either way, a page travels like this:


```text
ParquetReader.nextPage()
  -> ParquetPageSource.getNextSourcePage()
  -> ConnectorPageSource
  -> TableScanOperator or ScanFilterAndProjectOperator
  -> Trino Page
  -> next engine operator
```


Despite the shared name, a Trino `Page` is not a Parquet page. It is an
in-memory columnar batch:


```text
Parquet page:
  encoded and compressed storage unit inside a Parquet column chunk

Trino Page:
  in-memory batch of rows in Block form, passed between operators
```


I consider the trace complete when I can point to one returned page and say:


```text
This page came from an IcebergSplit.
That split came from Iceberg snapshot and manifest planning.
The file format was Parquet.
The page then entered the engine operator pipeline.
```


## What The Query Proves


Putting it together, the query


```sql
SELECT *
FROM iceberg.tpch.orders
WHERE o_orderstatus = 'F';
```


shows this read shape:


```text
SQL:
  one filtered Iceberg table read

Plan:
  TableScan with constraint on o_orderstatus

Metadata:
  Iceberg table handle records snapshot and pushed predicate state

Splits:
  Iceberg metadata scan plans file tasks and emits IcebergSplit objects

Worker:
  PageSourceManager asks IcebergPageSourceProvider for a page source

Reader:
  Parquet reader reads selected columns/row groups/pages from selected files

Engine:
  Trino Page objects flow through scan/filter/project/output operators
```


## Minimal Trace Note Template


For future read traces, I want to fill in a template like this one:


```text
Query:
  SELECT * FROM iceberg.tpch.orders WHERE o_orderstatus = 'F'

Plan evidence:
  TableScan node:
  Constraint:
  EXPLAIN ANALYZE rows:
  Physical input:
  Splits:

Metadata:
  table handle:
  snapshot id:
  enforced predicate:
  unenforced predicate:
  remaining predicate:

Splits:
  split manager:
  effective predicate:
  planned file tasks:
  emitted split count:
  sample split path:
  sample split format:

Page source:
  page source provider:
  file format:
  delete filtering path used:

Reader and pages:
  reader class:
  first SourcePage position count:
  first Trino Page position count:
  operator class:

Conclusion:
  What did Iceberg prune?
  What did Parquet prune?
  What did Trino still evaluate?
```


## What To Remember

- The catalog name chooses the connector. `iceberg.tpch.orders` uses the Iceberg
connector even if the rows originally came from TPC-H.
- The table handle is connector-specific planning state, not row data.
- Iceberg reads a snapshot through metadata JSON, manifest lists, manifest
files, and data files.
- Predicate pushdown has buckets: enforced, unenforced, and remaining.
- Splits are schedulable file/range read units, not table rows.
- `PageSourceManager` is the engine bridge into the connector page source.
- A Parquet page is a storage unit. A Trino `Page` is an execution batch.

## Self-Check


Questions to answer without looking back:

- Why does this query go through the Iceberg connector instead of the TPC-H
connector?
- What does the `TableScan` constraint prove, and what does it not prove?
- What is stored in an `IcebergTableHandle`?
- What is the difference between enforced, unenforced, and remaining
predicates?
- Where does Iceberg metadata pruning happen?
- What is an `IcebergSplit`?
- Which class bridges the engine to the connector page source?
- Why is a Parquet page different from a Trino `Page`?
- Why do I need `EXPLAIN ANALYZE` in addition to `EXPLAIN`?

## References

- Trino `EXPLAIN`: https://trino.io/docs/current/sql/explain.html
- Trino `EXPLAIN ANALYZE`: https://trino.io/docs/current/sql/explain-analyze.html
- Trino Iceberg connector: https://trino.io/docs/current/connector/iceberg.html

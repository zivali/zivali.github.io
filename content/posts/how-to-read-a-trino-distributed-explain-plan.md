---
title: How To Read A Trino Distributed EXPLAIN Plan
date: 2026-06-10T02:20:00.000Z
lastmod: 2026-06-14T19:57:00.000Z
draft: false
series:
  - Learning Trino by Tracing One Iceberg Table from SQL to Files
tags:
  - Trino
  - Iceberg
categories: []
summary: Reading Trino distributed EXPLAIN output by following RemoteSource dependencies from source fragments to commit.
authors:
  - Ziva Li
---
<link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/katex@0.16.2/dist/katex.min.css" integrity="sha384-bYdxxUwYipFNohQlHt0bjN/LCpueqWz13HufFEV1SUatKs1cm4L6fFgCi1jT643X" crossorigin="anonymous">


The main rule:


```text
Trino usually prints the root fragment first.
Data usually flows from source fragments toward fragment 0.
```


So the first pass over a distributed plan should not be:


```text
Fragment 0, then Fragment 1, then Fragment 2
```


The better habit is:


```text
Find each RemoteSource.
Use it to rebuild the dependency chain.
Read the plan from the source fragments back to the root.
```


For this note, the example is a row-level Iceberg `DELETE`. It is more useful
than a one-fragment read query because it shows a real distributed shape:


```text
source scan
  -> worker-side writer
  -> coordinator-side commit
```


The Iceberg details matter, but they are not the main lesson. The main lesson
is how to read fragments, `RemoteSource`, `LocalExchange`, estimates, and
runtime facts without mixing them together.


If I can read this delete plan, a normal `SELECT` plan should be easier. A
`SELECT` uses the same fragment and exchange model, but it usually ends in
result-producing operators instead of `MergeWriter` and `TableCommit`.


## The Query


The example query is:


```sql
EXPLAIN (TYPE DISTRIBUTED)
DELETE FROM iceberg.write_trace.orders_delta
WHERE orderkey = 1;
```


The table is an Iceberg table. The predicate is on `orderkey`, not the partition
column, so this is the row-level delete path rather than a simple metadata
delete.


The plan shape can be summarized as:


```text
Fragment 0 [COORDINATOR_ONLY]
  TableCommit
    LocalExchange[SINGLE]
      RemoteSource[sourceFragmentIds = [1]]

Fragment 1 [MERGE [insert = HASH]]
  MergeWriter
    LocalExchange[MERGE ...]
      RemoteSource[sourceFragmentIds = [2]]

Fragment 2 [SOURCE]
  ScanFilterProject
    table = iceberg.write_trace.orders_delta
    filterPredicate = orderkey = 1
    operation := DELETE
    field := $merge_row_id(...)
```


Printed order:


```text
Fragment 0
Fragment 1
Fragment 2
```


Data-flow order:


```text
Fragment 2
  -> Fragment 1
  -> Fragment 0
```


That reversal is the first thing to notice.


## Fragment Means Planned Distributed Work


A fragment is one distributed section of the query plan. At runtime, it becomes
stage work. That stage can run tasks, drivers, and operators.


A fragment is not a file, a row group, a single worker or a single thread.


It is a plan section with an input shape and an output shape.


Trino creates fragment boundaries when pages need to move between parts of the
plan. Common reasons include:

- source scan output needs to feed another stage
- rows need to be repartitioned for a join, aggregation, or writer
- distributed writer output needs to be gathered for a final commit
- final results need to be gathered for the client

In this example:


```text
Fragment 2:
  reads table data

Fragment 1:
  writes delete information from matching rows

Fragment 0:
  commits the Iceberg metadata change
```


The labels help:


```text
[SOURCE]:
  starts from connector splits and table scan work

[MERGE [insert = HASH]]:
  row-change writer fragment with merge-style partitioning

[COORDINATOR_ONLY]:
  work that runs once on the coordinator
```


## RemoteSource Is The Fragment Bridge


`RemoteSource` is the most important line when reading a distributed plan.


This line:


```text
RemoteSource[sourceFragmentIds = [2]]
```


means:


```text
the current fragment reads pages produced by fragment 2
```


In the example:


```text
Fragment 1 has RemoteSource[sourceFragmentIds = [2]]
```


So fragment 1 depends on fragment 2.


And:


```text
Fragment 0 has RemoteSource[sourceFragmentIds = [1]]
```


So fragment 0 depends on fragment 1.


That gives the actual dependency chain:


```text
Fragment 2
  -> Fragment 1
  -> Fragment 0
```


This is the habit to keep:


```text
1. Find Fragment 0.
2. Find its RemoteSource.
3. Jump to that source fragment.
4. Repeat until reaching SOURCE fragments.
5. Read the data flow from the deepest source back to Fragment 0.
```


Fragment numbers are useful labels. They are not the execution story by
themselves.


## LocalExchange Is Inside One Fragment


`RemoteSource` crosses fragment boundaries.


`LocalExchange` reshapes pages inside a fragment.


In fragment 0:


```text
LocalExchange[partitioning = SINGLE]
```


This gathers writer output into one stream before `TableCommit`.


That makes sense because an Iceberg commit is a single metadata update. Workers
can write data or delete artifacts in parallel, but the final table metadata
commit should happen once.


In fragment 1:


```text
LocalExchange[partitioning = MERGE [insert = HASH],
              arguments = [operation::tinyint, orderstatus::varchar]]
```


This arranges pages for the merge writer. The details are writer-specific, but
the general pattern is:


```text
RemoteSource:
  bring pages from another fragment

LocalExchange:
  gather, repartition, or arrange pages locally before the next operator
```


So when I see both together, I read them as two different movement layers:


```text
RemoteSource:
  inter-fragment page movement

LocalExchange:
  intra-fragment page movement
```


## Start From The Source Fragment


For this plan, the source fragment is:


```text
Fragment 2 [SOURCE]
  ScanFilterProject[
      table = iceberg:write_trace.orders_delta$data@...,
      filterPredicate = (orderkey = integer '1')]
```


This is where table data enters the plan.


`ScanFilterProject` tells me three things are fused together in the source
work:


```text
scan:
  read rows from the connector

filter:
  keep rows where orderkey = 1

project:
  output the symbols needed by the rest of the plan
```


For a row-level delete, the output is not only user table columns. Trino also
needs control columns:


```text
operation := tinyint '2'
case_number := integer '0'
insert_from_update := tinyint '0'
field := $merge_row_id(...)
```


The important ones here:


```text
operation := tinyint '2'
  marks the row as a delete operation

field := $merge_row_id(...)
  carries the row identity needed by the Iceberg connector
```


The `$merge_row_id` contains fields such as:


```text
"_file"
"_pos"
"partition_spec_id"
"partition_data"
"source_row_id"
```


That is the clue that this is not an in-place Parquet edit. Trino reads the
matching row, carries the file and row-position identity forward, and lets the
Iceberg write path commit delete metadata.


For the purpose of reading `EXPLAIN`, the bigger point is:


```text
Fragment 2 produces pages with both user columns and hidden control columns.
Fragment 1 consumes those pages through RemoteSource[sourceFragmentIds = [2]].
```


## Then Read The Writer Fragment


Fragment 1 consumes the output of fragment 2:


```text
Fragment 1 [MERGE [insert = HASH]]
  MergeWriter
    LocalExchange[partitioning = MERGE [insert = HASH],
                  arguments = [operation::tinyint, orderstatus::varchar]]
      RemoteSource[sourceFragmentIds = [2]]
```


The operator to notice is:


```text
MergeWriter
```


For this delete, `MergeWriter` receives pages containing:


```text
the operation code
the row id
the columns needed by the writer
```


It sends that information into the connector’s row-change write path.


For Iceberg:


```text
format v2:
  row-level deletes are represented with position delete files

format v3:
  row-level deletes can use deletion vectors
```


That storage detail belongs to the connector. From the Trino plan-reading
angle, I only need this summary:


```text
Fragment 1 is the worker-side write fragment.
It does not commit the table by itself.
It produces writer output for the root commit fragment.
```


## End At The Root Fragment


Fragment 0 is printed first because it is the root:


```text
Fragment 0 [COORDINATOR_ONLY]
  Output[columnNames = [rows]]
    TableCommit[target = iceberg:write_trace.orders_delta$data@...]
      LocalExchange[partitioning = SINGLE]
        RemoteSource[sourceFragmentIds = [1]]
```


This fragment depends on fragment 1:


```text
RemoteSource[sourceFragmentIds = [1]]
```


Then it gathers the writer output:


```text
LocalExchange[partitioning = SINGLE]
```


Then it runs:


```text
TableCommit
```


This is the final connector commit. For the Iceberg row-level delete path, the
commit is a `RowDelta`. Iceberg records delete metadata in a new snapshot
instead of modifying the original Parquet file in place.


The useful write-query mental model:


```text
workers:
  write data files, delete files, or other connector artifacts

coordinator:
  commits the final table metadata update
```


That is why write plans often have a worker-side writer fragment and a
coordinator-only commit fragment.


## Estimates Are Not Runtime Facts


`EXPLAIN` shows the planned shape. It does not read the data.


Lines like this are planner estimates:


```text
Estimates: {rows: ..., cpu: ..., memory: ..., network: ...}
```


They are useful for understanding what the optimizer thinks might happen, but
they are not proof of actual work performed.


The separation to remember:


```text
EXPLAIN:
  planned operators, fragments, exchanges, symbols, estimates

EXPLAIN ANALYZE:
  runs the query and adds runtime measurements
```


For a read query, `EXPLAIN ANALYZE` can show facts such as:


```text
Input rows
Physical input bytes
CPU time
Scheduled time
Split count
Splits generation wait time
```


So this line in a plan:


```text
TableScan[... constraint on [o_orderstatus]]
```


means the planned scan has a connector constraint.


It does not by itself prove:


```text
how many bytes were read
how many splits ran
how long the scan took
how many rows actually flowed through the operator
```


Use `EXPLAIN` for plan shape. Use `EXPLAIN ANALYZE` for runtime evidence.


For writes, be careful: `EXPLAIN ANALYZE` executes the write. It is not a dry
run.


## Quick Translation Table


| Plan text                                      | How to read it                                                       |
| ---------------------------------------------- | -------------------------------------------------------------------- |
| `Fragment N [...]`                             | One distributed plan section that becomes stage work at runtime.     |
| `[SOURCE]`                                     | Fragment starts from connector scan/split work.                      |
| `[COORDINATOR_ONLY]`                           | Fragment runs only on the coordinator. Common for final commit work. |
| `RemoteSource[sourceFragmentIds = [N]]`        | Current fragment reads pages produced by fragment `N`.               |
| `LocalExchange[partitioning = SINGLE]`         | Gather pages into one local stream.                                  |
| `LocalExchange[partitioning = HASH/MERGE/...]` | Repartition or arrange pages inside the fragment.                    |
| `TableScan`                                    | Connector table data enters the engine.                              |
| `ScanFilterProject`                            | Scan, filter, and projection are represented together.               |
| `MergeWriter`                                  | Worker-side row-change writer path.                                  |
| `TableCommit`                                  | Final connector commit path.                                         |


## Common SELECT Operators


The delete example is intentionally a harder plan because it includes a write
path and a final commit. A normal `SELECT` plan usually uses the same reading
routine, but the operators are more familiar:


| SELECT operator                    | How to read it                                                  |
| ---------------------------------- | --------------------------------------------------------------- |
| `TableScan`                        | Data enters from a connector table.                             |
| `ScanFilterProject`                | Scan, filter, and projection are combined near the source.      |
| `Filter`                           | Rows are removed by a predicate.                                |
| `Project`                          | Columns or expressions are selected or computed.                |
| `Aggregation` / `HashAggregation`  | Rows are grouped and aggregate state is built.                  |
| `Join` / `LookupJoin` / `HashJoin` | Rows from two inputs are matched.                               |
| `Sort`                             | Rows are ordered.                                               |
| `TopN`                             | Ordered rows are limited, often for `ORDER BY ... LIMIT`.       |
| `Limit`                            | The output row count is capped.                                 |
| `Window`                           | Window functions are evaluated over partitions or ordered rows. |
| `Exchange` / `RemoteSource`        | Pages move between fragments.                                   |
| `LocalExchange`                    | Pages are gathered or repartitioned inside a fragment.          |
| `Output`                           | Final result symbols are returned to the client.                |


So the comparison is:


```text
DELETE:
  scan rows -> write delete artifacts -> commit table metadata

SELECT:
  scan rows -> filter/project/join/aggregate/sort -> return result rows
```


The fragment-reading habit is the same. The write-specific operators disappear.


## The Reading Routine


When I read a distributed `EXPLAIN`, I want to do this:


```text
1. Identify the root fragment, usually Fragment 0.
2. Follow each RemoteSource[sourceFragmentIds = [...]] dependency.
3. Reconstruct the source-to-root data flow.
4. Find where table data enters: TableScan or ScanFilterProject.
5. Find where pages move between fragments: RemoteSource.
6. Find where pages are rearranged inside a fragment: LocalExchange.
7. Use Layout and symbol mappings to understand the data flowing between operators.
8. Treat Estimates as planner guesses.
9. Switch to EXPLAIN ANALYZE only when runtime evidence is needed.
```


For the delete example, the compact reading is:


```text
Fragment 2:
  scan Iceberg table rows where orderkey = 1
  create delete operation rows with $merge_row_id

Fragment 1:
  receive those rows through RemoteSource[2]
  arrange them for MergeWriter
  write delete information through the connector

Fragment 0:
  receive writer output through RemoteSource[1]
  gather it into one stream
  commit the Iceberg RowDelta
```


That is the part worth remembering. The printed plan starts at fragment 0, but
the data story starts at the source fragment.


## Self-Check


Questions to answer without looking back:

- Why is fragment `0` often printed before the source fragment?
- What does `RemoteSource[sourceFragmentIds = [2]]` mean?
- What is the difference between `RemoteSource` and `LocalExchange`?
- Why is `Fragment 0 [COORDINATOR_ONLY]` common in write plans?
- What does `ScanFilterProject` tell me?
- Why does a row-level Iceberg delete need `$merge_row_id`?
- Why are `Estimates` not runtime proof?
- When is `EXPLAIN ANALYZE` useful, and why is it dangerous for write queries?

## References

- Trino `EXPLAIN`: https://trino.io/docs/current/sql/explain.html

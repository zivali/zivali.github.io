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


A distributed `EXPLAIN` plan in Trino is printed in the opposite order from the
way data moves through it. Most of the confusion I had reading these plans came
from that one fact:


```text
Trino usually prints the root fragment first.
Data usually flows from source fragments toward fragment 0.
```


So the first pass over a distributed plan should not be:


```text
Fragment 0, then Fragment 1, then Fragment 2
```


Instead, I read it like this:


```text
Find each RemoteSource.
Use it to rebuild the dependency chain.
Read the plan from the source fragments back to the root.
```


The example in this note is a row-level Iceberg `DELETE`. It is more useful
than a one-fragment read query because it has a real distributed shape:


```text
source scan
  -> worker-side writer
  -> coordinator-side commit
```


The Iceberg details matter, but they are not the main point. The goal is to
read fragments, `RemoteSource`, `LocalExchange`, estimates, and runtime facts
without mixing them together.


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


Trimmed down, the plan looks like this:


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


The fragments are printed in this order:


```text
Fragment 0
Fragment 1
Fragment 2
```


but data flows in this order:


```text
Fragment 2
  -> Fragment 1
  -> Fragment 0
```


That reversal is the first thing to notice.


## Fragment Means Planned Distributed Work


A fragment is one distributed section of the query plan. At runtime it becomes
stage work, and that stage runs tasks, drivers, and operators. A fragment is
not a file, a row group, a single worker, or a single thread. It is a plan
section with an input shape and an output shape.


Trino creates fragment boundaries when pages need to move between parts of the
plan. Common reasons include:

- source scan output needs to feed another stage
- rows need to be repartitioned for a join, aggregation, or writer
- distributed writer output needs to be gathered for a final commit
- final results need to be gathered for the client

In this example, each fragment has one job:


```text
Fragment 2:
  reads table data

Fragment 1:
  writes delete information from matching rows

Fragment 0:
  commits the Iceberg metadata change
```


The bracketed labels on each fragment hint at that job:


```text
[SOURCE]:
  starts from connector splits and table scan work

[MERGE [insert = HASH]]:
  row-change writer fragment with merge-style partitioning

[COORDINATOR_ONLY]:
  work that runs once on the coordinator
```


## RemoteSource Is The Fragment Bridge


`RemoteSource` is the line I look for first in a distributed plan, because it
is how one fragment refers to another. This line:


```text
RemoteSource[sourceFragmentIds = [2]]
```


means:


```text
the current fragment reads pages produced by fragment 2
```


In the example, fragment 1 contains:


```text
Fragment 1 has RemoteSource[sourceFragmentIds = [2]]
```


so fragment 1 depends on fragment 2. Fragment 0 contains:


```text
Fragment 0 has RemoteSource[sourceFragmentIds = [1]]
```


so fragment 0 depends on fragment 1. Putting those together gives the actual
dependency chain:


```text
Fragment 2
  -> Fragment 1
  -> Fragment 0
```


The routine that falls out of this is:


```text
1. Find Fragment 0.
2. Find its RemoteSource.
3. Jump to that source fragment.
4. Repeat until reaching SOURCE fragments.
5. Read the data flow from the deepest source back to Fragment 0.
```


Fragment numbers are useful labels, but on their own they don't tell you the
execution order.


## LocalExchange Is Inside One Fragment


`RemoteSource` crosses fragment boundaries. `LocalExchange` reshapes pages
inside a single fragment.


Fragment 0 has:


```text
LocalExchange[partitioning = SINGLE]
```


This gathers writer output into one stream before `TableCommit`. That makes
sense because an Iceberg commit is a single metadata update. Workers can write
data or delete artifacts in parallel, but the final table metadata commit
should happen once.


Fragment 1 has a different one:


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


So when I see both in one fragment, I read them as two separate layers of page
movement:


```text
RemoteSource:
  inter-fragment page movement

LocalExchange:
  intra-fragment page movement
```


## Start From The Source Fragment


With the dependency chain in hand, we can walk the plan in data-flow order,
starting with the source fragment:


```text
Fragment 2 [SOURCE]
  ScanFilterProject[
      table = iceberg:write_trace.orders_delta$data@...,
      filterPredicate = (orderkey = integer '1')]
```


This is where table data enters the plan. `ScanFilterProject` means three
steps are fused together in the source work:


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


Two of these matter most here:


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


This tells us the delete is not an in-place Parquet edit. Trino reads the
matching row, carries the file and row-position identity forward, and lets the
Iceberg write path commit delete metadata.


For reading `EXPLAIN`, what matters is the handoff to the next fragment:


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


and sends that information into the connector’s row-change write path. How
Iceberg stores the result depends on the table format version:


```text
format v2:
  row-level deletes are represented with position delete files

format v3:
  row-level deletes can use deletion vectors
```


That storage detail belongs to the connector. For reading the Trino plan, this
summary is enough:


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


Reading it bottom-up, it depends on fragment 1:


```text
RemoteSource[sourceFragmentIds = [1]]
```


gathers the writer output:


```text
LocalExchange[partitioning = SINGLE]
```


and then runs:


```text
TableCommit
```


This is the final connector commit. For the Iceberg row-level delete path, the
commit is a `RowDelta`. Iceberg records delete metadata in a new snapshot
instead of modifying the original Parquet file in place.


The mental model I use for write queries is:


```text
workers:
  write data files, delete files, or other connector artifacts

coordinator:
  commits the final table metadata update
```


That is why write plans often have a worker-side writer fragment and a
coordinator-only commit fragment.


## Estimates Are Not Runtime Facts


`EXPLAIN` shows the planned shape. It does not read the data. Lines like this
are planner estimates:


```text
Estimates: {rows: ..., cpu: ..., memory: ..., network: ...}
```


They show what the optimizer expects to happen, but they are not a record of
work actually performed. The two commands answer different questions:


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


So a line like this in a plan:


```text
TableScan[... constraint on [o_orderstatus]]
```


means the planned scan has a connector constraint. It does not by itself tell
you:


```text
how many bytes were read
how many splits ran
how long the scan took
how many rows actually flowed through the operator
```


Use `EXPLAIN` for plan shape and `EXPLAIN ANALYZE` for runtime evidence. For
writes, be careful: `EXPLAIN ANALYZE` executes the write. It is not a dry run.


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


I picked the delete example on purpose because it is harder: it has a write
path and a final commit. A normal `SELECT` plan is read the same way, but the
operators are more familiar:


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


Side by side, the two shapes are:


```text
DELETE:
  scan rows -> write delete artifacts -> commit table metadata

SELECT:
  scan rows -> filter/project/join/aggregate/sort -> return result rows
```


The way you follow fragments is the same. Only the write-specific operators
go away.


## The Reading Routine


When I read a distributed `EXPLAIN`, I go through these steps:


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


Applied to the delete example, that gives:


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


The printed plan starts at fragment 0, but the data starts at the source
fragment. Following `RemoteSource` links is how you get from one to the other.


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

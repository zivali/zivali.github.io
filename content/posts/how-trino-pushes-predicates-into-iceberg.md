---
title: How Trino Pushes Predicates Into Iceberg
date: 2026-06-21T03:50:00.000Z
lastmod: 2026-06-21T03:51:00.000Z
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


# What Predicate Pushdown Means In Trino


Predicate pushdown means Trino tries to move part of a `WHERE` filter closer to
the data source. Instead of reading all rows first and filtering only inside the
engine, Trino asks the connector whether it can use the predicate while planning
or reading the table.


That can reduce work, but it is not one yes/no switch.


The useful mental model is:


```text
pushed down:
  the connector can use the predicate during scan planning or reading

enforced:
  the connector guarantees rows returned by the scan satisfy that predicate

remaining:
  Trino still evaluates the predicate above the scan for correctness
```


So “pushed down” does not mean “the engine no longer checks it.”


## The Setup


The table is the same Iceberg table from the read trace:


```sql
CREATE TABLE iceberg.tpch.orders
WITH (
    format = 'PARQUET',
    partitioning = ARRAY['o_orderstatus']
) AS
SELECT *
FROM tpch.tiny.orders;
```


The important part is the partitioning:


```text
partitioning = ARRAY['o_orderstatus']
```


That makes `o_orderstatus` an identity partition column. A predicate on that
column can be enforced from Iceberg partition metadata.


The first query is:


```sql
SELECT *
FROM iceberg.tpch.orders
WHERE o_orderstatus = 'F';
```


The second query adds a regular data-column predicate:


```sql
SELECT o_orderkey, o_totalprice
FROM iceberg.tpch.orders
WHERE o_orderstatus = 'F'
  AND o_totalprice > 1000;
```


This second query is the useful one for understanding pushdown. It has two
predicate shapes:


| Predicate             | Kind                          | Expected behavior                                                         |
| --------------------- | ----------------------------- | ------------------------------------------------------------------------- |
| `o_orderstatus = 'F'` | identity partition predicate  | Iceberg can enforce it from partition metadata.                           |
| `o_totalprice > 1000` | regular data-column predicate | Iceberg can use it for pruning, but Trino still needs a remaining filter. |


## Plan Evidence For The Partition Predicate


For the simple partition query, `EXPLAIN` shows the predicate attached to the
scan:


```text
TableScan[table = iceberg:tpch.orders$data@... constraint on [o_orderstatus]]
    o_orderstatus := 3:o_orderstatus:varchar
        :: [[F]]
```


This proves:


```text
The plan contains an Iceberg table scan with a constraint on o_orderstatus.
```


It does not prove how many bytes were read. It does not prove row-group pruning.
It does not prove page-level filtering. It only proves the planned scan shape.


`EXPLAIN ANALYZE` adds runtime evidence:


```text
TableScan[table = iceberg:tpch.orders$data@... constraint on [o_orderstatus]]
    Output: 7304 rows
    Input: 7304 rows (1012.76kB)
    Physical input: 169.66kB
    Splits: 1
```


This proves that, in this run, the scan returned `7304` rows and executed one
split with `169.66kB` physical input.


The metadata tables match that shape:


```text
orders$partitions:
  {o_orderstatus=F}
  record_count = 7304
  file_count = 1

orders$files where partition.o_orderstatus = 'F':
  one PARQUET file
  record_count = 7304
```


So for this table, `o_orderstatus = 'F'` selects one Iceberg partition and one
data file.


## What Each Evidence Type Proves


Before going deeper into the code path, it helps to separate what each piece of
evidence can and cannot prove:


| Evidence          | What it proves                                                              | What it does not prove                                            |
| ----------------- | --------------------------------------------------------------------------- | ----------------------------------------------------------------- |
| `EXPLAIN`         | Planned scan shape and visible scan constraints.                            | Runtime rows, bytes read, or exact pruning done during execution. |
| `EXPLAIN ANALYZE` | Runtime rows, physical input, split count, and operator stats for that run. | Which Java branch classified each predicate.                      |
| `$partitions`     | Partition metadata, record counts, file counts, and partition-level stats.  | Parquet row-group reads inside a selected file.                   |
| `$files`          | Which Iceberg data files match metadata filters.                            | Which Parquet pages were read from that file.                     |


## Where Predicate Pushdown Happens


Predicate pushdown is not one method call. It happens in layers:


| Layer                    | Trino component                                                           | Where it happens                                                      | What happens                                                                                                          |
| ------------------------ | ------------------------------------------------------------------------- | --------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------- |
| Planner optimization     | Logical planner / iterative optimizer                                     | Coordinator: `PushPredicateIntoTableScan`                             | Trino turns the `WHERE` expression into a connector `Constraint` and asks the connector to apply it.                  |
| Connector metadata       | `ConnectorMetadata` implementation in the Iceberg connector               | Coordinator: `IcebergMetadata.applyFilter(...)`                       | Iceberg classifies predicate domains into enforced, unenforced, unsupported, and remaining pieces.                    |
| Split planning           | `ConnectorSplitManager` / `ConnectorSplitSource` in the Iceberg connector | Coordinator: `IcebergSplitManager` and `IcebergSplitSource`           | Iceberg uses the enforced and unenforced scan state to plan matching files and create `IcebergSplit` objects.         |
| File-format reader setup | `ConnectorPageSourceProvider` in the Iceberg connector                    | Worker: `IcebergPageSourceProvider`                                   | The worker refines the unenforced predicate with split statistics and dynamic filters before opening the file reader. |
| Engine filtering         | Worker execution operators                                                | Worker: filter above the scan, such as `ScanFilterAndProjectOperator` | Trino evaluates the remaining predicate for correctness.                                                              |


The full flow is:


```text
SQL WHERE predicate
  -> coordinator planner extracts a TupleDomain
  -> coordinator planner asks ConnectorMetadata.applyFilter(...)
  -> coordinator IcebergMetadata.applyFilter(...) classifies the predicate
  -> new IcebergTableHandle carries enforced and unenforced predicates
  -> remaining predicate stays above the TableScan
  -> coordinator IcebergSplitSource uses the scan predicates to plan files
  -> IcebergSplit carries file path, partition values, and file stats
  -> worker IcebergPageSourceProvider refines the predicate for one split
  -> Parquet or ORC reader may prune row groups or pages
  -> worker Trino operator evaluates any remaining filter
```


So the first pushdown decision is coordinator-side planning. The later pruning
work also uses pushed predicate information, but it happens when Trino plans
Iceberg splits and when a worker builds the page source for a concrete split.


## Where Pushdown Enters The Planner


The planner rule is:


```text
PushPredicateIntoTableScan
```


The shape is:


```text
SQL WHERE predicate
  -> DomainTranslator extracts a TupleDomain
  -> PushPredicateIntoTableScan builds a Constraint
  -> MetadataManager.applyFilter(...)
  -> ConnectorMetadata.applyFilter(...)
  -> IcebergMetadata.applyFilter(...)
```


In Trino source, the key planner step is:


```text
core/trino-main/src/main/java/io/trino/sql/planner/iterative/rule/PushPredicateIntoTableScan.java
```


The rule extracts a tuple domain from the deterministic predicate, maps symbols
to connector column handles, builds a `Constraint`, and calls:


```text
plannerContext.getMetadata().applyFilter(session, node.getTable(), constraint)
```


The connector then returns a `ConstraintApplicationResult`. The planner reads:


```text
new table handle
remaining filter
remaining connector expression
```


That is the first sign that pushdown is negotiated. The engine asks the
connector what it can use. The connector answers with both new scan state and
the part Trino must keep.


## How Iceberg Classifies The Predicate


Inside Iceberg, the important method is:


```text
IcebergMetadata.applyFilter(...)
```


For each predicate domain, Iceberg classifies it into one of these buckets:


```text
newEnforcedConstraint:
  Iceberg can guarantee this predicate from metadata or partition knowledge

newUnenforcedConstraint:
  Iceberg can use this predicate in scan planning or reader setup, but it is not
  a correctness guarantee

remainingConstraint:
  Trino must still evaluate this predicate above the scan
```


The mixed query:


```sql
WHERE o_orderstatus = 'F'
  AND o_totalprice > 1000
```


should split like this:


| Predicate piece       | Iceberg bucket            | Why                                                                                                                               |
| --------------------- | ------------------------- | --------------------------------------------------------------------------------------------------------------------------------- |
| `o_orderstatus = 'F'` | `newEnforcedConstraint`   | `o_orderstatus` is an identity partition column.                                                                                  |
| `o_totalprice > 1000` | `newUnenforcedConstraint` | Iceberg can push it into scan planning, but this regular data-column predicate is not classified as connector-enforced filtering. |
| `o_totalprice > 1000` | `remainingConstraint`     | Trino still applies it above the scan for correctness.                                                                            |


The code path matches that mental model:


```text
if the domain is not convertible:
  unsupported

else if Iceberg can enforce it with the partition spec:
  newEnforced

else if it is an enforceable metadata column:
  newEnforced

else:
  newUnenforced
```


Here `unsupported` does not mean the SQL query is unsupported. It means Trino
has a predicate domain, but Iceberg cannot safely translate that domain into an
Iceberg filter expression for scan planning. Examples include structural types
such as arrays, maps, and rows, geospatial types, and most UUID or variant
comparisons. Those predicates stay in the remaining filter so Trino can evaluate
them after the scan.


Then Iceberg returns a new `IcebergTableHandle` with:


```text
newUnenforcedConstraint
newEnforcedConstraint
newConstraintColumns
```


and returns `remainingConstraint` separately to the engine.


## Why The Partition Predicate Is Enforced


The helper to remember is:


```text
canEnforceColumnConstraintInSpecs(...)
```


It checks whether a column predicate can be enforced by the Iceberg partition
specs for the selected snapshot.


The simplest case is identity partitioning:


```text
if the partition field transform is identity:
  a predicate on that column can always be enforced
```


That is why `o_orderstatus = 'F'` is strong. If the table is identity
partitioned by `o_orderstatus`, a file in the `F` partition already has that
partition value. Iceberg can remove files from other partitions before Trino
workers read them.


This is metadata-level pruning:


```text
Iceberg metadata:
  partition values
  manifest entries
  data file records

Result:
  non-F partitions do not become scan work
```


## Why The Price Predicate Is Not Fully Enforced


`o_totalprice` is a table column inside the data file. It is not the identity
partition column in this setup.


That means this predicate:


```sql
o_totalprice > 1000
```


can still be useful, but it is a weaker kind of pushdown.


Iceberg can use file statistics and scan planning to skip work when metadata
proves a file cannot match. This happens in a few steps.


First, the split source builds an effective predicate from:


```text
data column predicate
tableHandle.getUnenforcedPredicate()
pushed-down dynamic filter predicate
```


A dynamic filter is a runtime predicate that Trino learns while the query is
already running. The common case is a join: if the build side produces only
`status = F`, Trino can use that runtime fact as a dynamic filter on the probe
side scan, such as `o_orderstatus IN ('F')`. In this single-table example, the
dynamic filter does not add much; in a join, it can give `IcebergSplitSource` and
`IcebergPageSourceProvider` extra values for file and reader pruning.


For this example, the useful part is still:


```text
o_totalprice > 1000
```


That predicate is not enforced by Iceberg, but it can still be used as a file
selection hint. The split source converts the effective predicate to an Iceberg
expression and gives it to Iceberg’s table scan:


```text
toIcebergExpression(effectivePredicate)
scan.planFiles()
```


At this point, Iceberg is still working with metadata. It can look at manifest
entries and data-file statistics before any worker opens a Parquet file. If a
file says:


```text
o_totalprice max = 900
```


then that file cannot contain rows where:


```text
o_totalprice > 1000
```


so it does not need to become scan work for this predicate.


Second, if predicated columns exist, the scan includes column stats so each
planned split can carry file-level statistics:


```text
fileStatisticsDomain
```


For a selected file, that domain is built from metadata such as:


```text
lower_bounds
upper_bounds
null_value_counts
```


So a split might carry a rough fact like:


```text
this file has o_totalprice values between 800 and 1500
```


That does not prove every row matches `o_totalprice > 1000`. It only says this
file may contain matching rows.


Third, when a worker opens the split, the page source can intersect:


```text
unenforced predicate
fileStatisticsDomain
dynamic filter
```


This is a second chance to refine the read for one concrete split. The dynamic
filter may be more selective by then, and the file statistics are already
attached to the split. If the intersection is empty, the page source can return
no rows for that split. If the intersection is not empty, the refined predicate
can still help the Parquet reader with row-group or page pruning.


This is useful, but it is still not the same as the connector returning the
predicate as enforced. File statistics can often prove:


```text
no rows in this file can match
```


They cannot always prove:


```text
every row returned by this scan satisfies the predicate
```


So Trino keeps the remaining filter. A later reader or split-level check may
discover that a specific file or row group is fully inside the predicate, but
that is separate from Iceberg classifying the original predicate as enforced
during `applyFilter(...)`.


## The Row-Group Min/Max Version


Parquet adds another pruning layer after Iceberg has already selected data
files.


A simple row-group example:


```text
predicate:
  o_totalprice > 1000

row group 1:
  min = 100
  max = 900
  skip, because max <= 1000

row group 2:
  min = 800
  max = 1500
  read, because some rows may match

row group 3:
  min = 1200
  max = 2000
  read, because the min/max range is inside this predicate
```


The important case for the remaining-filter mental model is row group `2`. Its
stats cannot prove the final answer.
It may contain rows like:


```text
900
950
1200
1300
```


The row group is worth reading, but the filter still has to remove the rows
below or equal to `1000`.


That is why I need to keep these layers separate:


```text
Iceberg partition/file pruning:
  happens before opening selected Parquet files

Parquet row-group/page pruning:
  happens inside selected Parquet files

Trino remaining filter:
  happens on decoded Trino Page objects for correctness
```


This is also where the word “page” can mislead:


```text
Parquet page:
  encoded and compressed storage unit inside a Parquet column chunk

Trino Page:
  in-memory batch of rows in columnar Block form
```


A pushed predicate may help the Parquet reader avoid some storage pages or row
groups. The remaining filter is applied to Trino pages after the connector has
decoded data into engine batches.


## What To Remember

- Predicate pushdown is a negotiation between the engine and the connector.
- `PushPredicateIntoTableScan` asks the connector to apply a filter.
- `IcebergMetadata.applyFilter(...)` classifies predicate domains into
enforced, unenforced, and remaining buckets.
- Identity partition predicates, such as `o_orderstatus = 'F'` in this table,
can be enforced by Iceberg partition metadata.
- Regular data-column predicates, such as `o_totalprice > 1000`, can still help
prune files, row groups, or pages, but they may remain as Trino filters.
- Dynamic filters are runtime predicates, usually learned from joins, and can
refine split planning or page-source setup.
- Iceberg metadata pruning and Parquet row-group/page pruning are different
layers.
- A Parquet page is a storage-format unit. A Trino `Page` is an in-memory
execution batch.
- “Pushed down” does not automatically mean “fully enforced.”

## Self-Check


Questions to answer without looking back:

- What is the difference between pushed down and enforced?
- Why can Iceberg enforce `o_orderstatus = 'F'` for this table?
- Why does `o_totalprice > 1000` remain a Trino filter?
- What does `EXPLAIN` prove for a scan constraint?
- What does `EXPLAIN ANALYZE` add?
- When does a dynamic filter become useful?
- What is the difference between Iceberg metadata pruning and Parquet row-group
pruning?
- Why is a Parquet page different from a Trino `Page`?

## Source Anchors For Debugging


These are the Trino source points I would use to verify the trace with
breakpoints. The links are pinned to the Trino commit used for this note:
`f865b4a444eacf871de4d1fefceedf292c7f6cc6`.


| Boundary                                     | Trino source anchor                  | What to inspect                                                                             |
| -------------------------------------------- | ------------------------------------ | ------------------------------------------------------------------------------------------- |
| Planner extracts a tuple domain              | PushPredicateIntoTableScan.java#L161 | `decomposedPredicate`, `newDomain`, and symbol-to-column-handle mapping.                    |
| Planner asks connector to apply the filter   | PushPredicateIntoTableScan.java#L226 | `Constraint` passed into metadata and returned `ConstraintApplicationResult`.               |
| Planner keeps remaining filter               | PushPredicateIntoTableScan.java#L240 | `remainingFilter` from the connector response.                                              |
| Iceberg classifies domains                   | IcebergMetadata.java#L3590           | `predicate`, `newEnforcedConstraint`, `newUnenforcedConstraint`, and `remainingConstraint`. |
| Iceberg checks partition enforcement         | IcebergUtil.java#L621                | Whether the column constraint can be enforced by the active Iceberg partition specs.        |
| Identity partition is enforceable            | IcebergUtil.java#L655                | Identity partition transform returns enforceable.                                           |
| Split planning uses the unenforced predicate | IcebergSplitSource.java#L274         | `effectivePredicate`, `toIcebergExpression(...)`, and `scan.planFiles()`.                   |
| Split carries file statistics                | IcebergSplitSource.java#L613         | `fileStatisticsDomain` built from lower bounds, upper bounds, and null counts.              |
| Page source refines reader predicate         | IcebergPageSourceProvider.java#L467  | Intersection of unenforced predicate, file statistics, and dynamic filter.                  |


## References

- Trino source code: https://github.com/trinodb/trino
- Trino `EXPLAIN`: https://trino.io/docs/current/sql/explain.html
- Trino `EXPLAIN ANALYZE`: https://trino.io/docs/current/sql/explain-analyze.html

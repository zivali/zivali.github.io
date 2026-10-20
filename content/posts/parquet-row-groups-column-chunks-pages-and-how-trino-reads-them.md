---
title: Parquet Row Groups, Column Chunks, Pages, And How Trino Reads Them
date: 2026-06-16T02:39:00.000Z
lastmod: 2026-06-16T20:05:00.000Z
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


Once Iceberg has picked the data files for a scan, Parquet decides how those
files are actually read. It sits between Iceberg's selected data files and
Trino's in-memory execution batches.


At a high level, a Trino scan of a Parquet file looks like this:


```text
read file metadata
  -> choose useful row groups
  -> read useful columns
  -> read encoded and compressed Parquet pages
  -> decode batches into Trino blocks
  -> pass Trino Page objects to operators
```


The previous note in this series covered how Iceberg metadata points to data
files. This one picks up after a selected data file is opened as Parquet and
walks through how row groups, column chunks, pages, encodings, and Trino `Page`
objects fit together. The next note uses this vocabulary to trace a real Iceberg
read query.


## Row-Based vs Columnar Storage


We will use a tiny table as the running example:


| id | name  | age | country |
| -- | ----- | --- | ------- |
| 1  | Alice | 20  | US      |
| 2  | Bob   | 35  | CA      |
| 3  | Chris | 28  | US      |


A row-based file stores complete rows together:


```text
1, Alice, 20, US
2, Bob, 35, CA
3, Chris, 28, US
```


A columnar file stores values by column:


```text
id:      1, 2, 3
name:    Alice, Bob, Chris
age:     20, 35, 28
country: US, CA, US
```


Now consider this query:


```sql
SELECT avg(age)
FROM users;
```


With a columnar format, the reader only has to touch the `age` column. It does
not need to read `id`, `name`, or `country` to compute the average.


This is the main reason Parquet suits Trino. Analytical queries usually need a
few columns, not every column of every row.


## The Parquet File Layout


A Parquet file is organized as a nested structure:


```text
Parquet file
  row group 1
    column chunk: id
      data pages
    column chunk: name
      data pages
    column chunk: age
      data pages
    column chunk: country
      optional dictionary page
      data pages

  row group 2
    column chunk: id
    column chunk: name
    column chunk: age
    column chunk: country

  footer metadata
```


The terms used in the rest of this note:


| Term            | Meaning                                                                    |
| --------------- | -------------------------------------------------------------------------- |
| Parquet file    | One physical data file.                                                    |
| Row group       | A horizontal batch of rows inside the file.                                |
| Column chunk    | Values for one column inside one row group.                                |
| Parquet page    | Smaller encoded and compressed storage unit inside a column chunk.         |
| Footer metadata | File metadata, schema, row group metadata, column statistics, and offsets. |


Row groups and column chunks cut the data in two different directions:


```text
row group:
  horizontal slice of rows

column chunk:
  vertical slice for one column inside that row group
```


Because of that, Parquet supports two kinds of pruning:


```text
row-group pruning:
  skip row batches that cannot match a filter

column pruning:
  avoid reading columns the query does not need
```


## Row Groups


A row group is a batch of rows. A file with one million rows might be laid out
like this:


```text
users.parquet
  row group 1: rows 1 - 100,000
  row group 2: rows 100,001 - 200,000
  row group 3: rows 200,001 - 300,000
  ...
```


Inside each row group, values are stored by column:


```text
row group 1
  id column chunk
  name column chunk
  age column chunk
  country column chunk

row group 2
  id column chunk
  name column chunk
  age column chunk
  country column chunk
```


A row group is large enough for its metadata to be useful, but smaller than the
whole file, which makes it a practical boundary for skipping data.


## Row Group Skipping


Parquet row groups often carry per-column statistics. The simplest example is
min/max. Suppose the `age` column has these row group statistics:


```text
row group 1: age min=10, max=25
row group 2: age min=26, max=45
row group 3: age min=46, max=80
```


For this query:


```sql
SELECT *
FROM users
WHERE age > 50;
```


Trino can reason about each row group from the statistics alone:


```text
row group 1:
  max age is 25, so skip

row group 2:
  max age is 45, so skip

row group 3:
  max age is 80, so rows may match
```


Parquet does not know the answer to the query. The statistics only let the
reader avoid work for row groups that cannot produce matching rows. The proof
only goes one way: statistics can show that no row in a row group can match, but
they usually cannot show that every row matches.


So after reading a row group that might contain matches, Trino may still need to
evaluate the filter on its rows.


## Column Chunks And Parquet Pages


A row group contains one column chunk per column. For this row group:


```text
row group 1: rows 1 - 100,000
```


the file has separate column chunks:


```text
id column chunk:      id values for rows 1 - 100,000
name column chunk:    name values for rows 1 - 100,000
age column chunk:     age values for rows 1 - 100,000
country column chunk: country values for rows 1 - 100,000
```


Each column chunk is split into smaller Parquet pages:


```text
country column chunk
  dictionary page: unique country values, if dictionary encoding is used
  data page 1: encoded values for some rows
  data page 2: encoded values for more rows
  data page 3: encoded values for more rows
```


The row group is the larger pruning unit. The Parquet page is the smaller
encoded and compressed storage unit inside a column chunk.


Keep the word `page` in mind, because Trino uses it too. A Parquet page and a
Trino `Page` are different things, and we will come back to that below.


## Encoding vs Compression


Encoding and compression are related but separate steps. Encoding changes how
values are represented; compression then shrinks the encoded bytes.


On the write side:


```text
original values
  -> encode values into a compact representation
  -> compress the encoded bytes
  -> write bytes to the Parquet file
```


And on the read side, the same steps run in reverse:


```text
read compressed bytes
  -> decompress bytes
  -> decode values or ids
  -> produce in-memory batches for the engine
```


Two common encodings are dictionary encoding and run-length encoding.


## Dictionary Encoding


Dictionary encoding helps when a column has many repeated values, for example:


```text
country:
US
US
CA
US
CA
MX
US
```


Instead of storing each string again and again, Parquet can store the unique
values once:


```text
dictionary:
0 -> US
1 -> CA
2 -> MX
```


and then store ids in the data:


```text
0, 0, 1, 0, 1, 2, 0
```


The column is now stored as the dictionary values plus a list of integer ids.


This works well for string-like columns with a small set of repeated values:


```text
country
status
event_type
tenant_id
category
```


## Run-Length Encoding


Run-length encoding (RLE) helps when the same value, or the same small id,
appears many times in a row. For example:


```text
status:
ACTIVE
ACTIVE
ACTIVE
ACTIVE
ACTIVE
INACTIVE
INACTIVE
ACTIVE
ACTIVE
ACTIVE
```


A simple RLE representation is:


```text
ACTIVE x 5
INACTIVE x 2
ACTIVE x 3
```


In Parquet, RLE is mostly used for compact streams of small integers, such as
dictionary ids and the levels used for null or nested data.


Dictionary encoding and RLE can be combined:


```text
country:
US
US
US
US
CA
CA
CA
MX
MX

dictionary:
0 -> US
1 -> CA
2 -> MX

dictionary ids:
0, 0, 0, 0, 1, 1, 1, 2, 2

RLE over ids:
0 x 4
1 x 3
2 x 2
```


The exact low-level encoding details matter less here than the general effect:
Parquet can store repeated values compactly, and Trino can often keep those
compact shapes while it processes batches.


## Nulls And Nested Data


Parquet also has to represent nulls and nested structures. For a nullable
column:


```text
nickname:
Alice
null
Chris
```


the file needs to record:


```text
position 0: value exists
position 1: value is null
position 2: value exists
```


Parquet stores this as definition levels. A definition level is a small number
that says how much of the value path exists.


For repeated nested data such as arrays and maps, Parquet also uses repetition
levels, which describe whether a value starts a new parent row or continues a
repeated field.


In short:


```text
definition level:
  is the value present, or is some part null?

repetition level:
  for arrays and maps, does this continue the same parent row?
```


These levels are usually small, repeated numbers, so they compress well with
RLE and bit-packed encoding.


## How Trino Fits


Here are the Trino-side terms alongside the Parquet ones:


```text
split:
  a unit of read work assigned to a worker; often covers a file or file range

connector page source:
  the connector object that reads data and produces pages for the engine

Parquet reader:
  the format reader that reads Parquet metadata and page data

Parquet row group:
  a storage-format batch of rows with column chunks and statistics

Parquet page:
  encoded and compressed storage unit inside a column chunk

Trino Block:
  in-memory column-shaped batch of values

Trino Page:
  in-memory batch of rows; one block per selected column
```


As mentioned earlier, `page` means two different things:


```text
Parquet page:
  encoded and compressed bytes inside a file

Trino Page:
  in-memory batch passed between operators
```


The reader turns one into the other, but they are not the same object:


```text
Parquet pages
  -> decompression
  -> decoding
  -> Trino blocks
  -> Trino Page
```


## A Simple Trino Read Shape


To make this concrete, take this query:


```sql
SELECT name
FROM users
WHERE country = 'US';
```


Trino only needs two of the four columns:


```text
country:
  needed for the filter

name:
  needed for the output

id:
  not needed

age:
  not needed
```


The scan flow looks roughly like this:


```text
SQL query
  -> split assigned to a worker
  -> connector page source reads Parquet metadata
  -> skip impossible row groups
  -> read needed column chunks
  -> read compressed Parquet pages
  -> decompress page bytes
  -> decode values into Trino blocks
  -> build Trino Page objects
  -> apply operators such as filter, project, join, aggregation
  -> output pages
```


If `country` is dictionary-encoded, the file might contain:


```text
dictionary:
0 -> US
1 -> CA
2 -> MX

ids:
0, 0, 1, 0, 2, 1
```


Trino may keep a similar compact shape in memory:


```text
dictionary block:
  dictionary values: US, CA, MX
  ids:               0, 0, 1, 0, 2, 1
```


The filter then works out which positions match:


```text
position 0: id 0 -> US -> keep
position 1: id 0 -> US -> keep
position 2: id 1 -> CA -> drop
position 3: id 0 -> US -> keep
position 4: id 2 -> MX -> drop
position 5: id 1 -> CA -> drop
```


Since the query returns `name`, Trino uses those kept positions to select the
matching values from the `name` block.


Some operations can keep compact block shapes, while others materialize new
values. Either way, the processing model is the same: Trino works on columnar
batches, not on one row object at a time.


## A Footer Check From The Demo File


For the demo file, I exported Parquet footer metadata as CSV from a Parquet
metadata viewer. This is better evidence than a row-preview screenshot because
it shows the storage layout behind the decoded rows.


The CSV has one row per row-group column chunk:


```text
100 row groups * 4 columns = 400 metadata rows
```


The inspected columns are:


```text
orderkey
custkey
orderstatus
totalprice
```


The main facts from the footer:


| Fact                   | Observed value                      |
| ---------------------- | ----------------------------------- |
| Row groups             | `100`                               |
| Columns per row group  | `4`                                 |
| Rows per row group     | mostly `48` to `55`                 |
| Compression            | `ZSTD`                              |
| `orderstatus` encoding | `PLAIN_DICTIONARY, BIT_PACKED, RLE` |


Side note: I can inspect the same kind of footer metadata from the command line
too. For a local file, `parquet-tools inspect <file.parquet>` shows the schema,
row group count, columns, physical/logical types, compression, and related file
metadata without uploading the file anywhere.


Here is a narrowed excerpt from the CSV export. It includes every column chunk
row for row groups `0` and `1`, then keeps only the `totalprice` rows for row
groups `2` through `4`. The `...` rows stand in for the other column chunks in
those row groups.


| Row group | Rows | Column        | Type         | Min       | Max       | Compression | Encodings                           |
| --------- | ---- | ------------- | ------------ | --------- | --------- | ----------- | ----------------------------------- |
| 0         | 50   | `orderkey`    | `INT64`      | `1`       | `50`      | `ZSTD`      | `PLAIN, BIT_PACKED, RLE`            |
| 0         | 50   | `custkey`     | `INT64`      | `1000`    | `1009`    | `ZSTD`      | `PLAIN_DICTIONARY, BIT_PACKED, RLE` |
| 0         | 50   | `orderstatus` | `BYTE_ARRAY` | `F`       | `P`       | `ZSTD`      | `PLAIN_DICTIONARY, BIT_PACKED, RLE` |
| 0         | 50   | `totalprice`  | `DOUBLE`     | `57.25`   | `412.5`   | `ZSTD`      | `PLAIN, BIT_PACKED, RLE`            |
| 1         | 50   | `orderkey`    | `INT64`      | `51`      | `100`     | `ZSTD`      | `PLAIN, BIT_PACKED, RLE`            |
| 1         | 50   | `custkey`     | `INT64`      | `1000`    | `1009`    | `ZSTD`      | `PLAIN_DICTIONARY, BIT_PACKED, RLE` |
| 1         | 50   | `orderstatus` | `BYTE_ARRAY` | `F`       | `P`       | `ZSTD`      | `PLAIN_DICTIONARY, BIT_PACKED, RLE` |
| 1         | 50   | `totalprice`  | `DOUBLE`     | `419.75`  | `775.0`   | `ZSTD`      | `PLAIN, BIT_PACKED, RLE`            |
| 2         | …    | `...`         | `...`        | `...`     | `...`     | `...`       | `...`                               |
| 2         | 50   | `totalprice`  | `DOUBLE`     | `782.25`  | `1137.5`  | `ZSTD`      | `PLAIN, BIT_PACKED, RLE`            |
| 3         | …    | `...`         | `...`        | `...`     | `...`     | `...`       | `...`                               |
| 3         | 50   | `totalprice`  | `DOUBLE`     | `1144.75` | `1500.0`  | `ZSTD`      | `PLAIN, BIT_PACKED, RLE`            |
| 4         | …    | `...`         | `...`        | `...`     | `...`     | `...`       | `...`                               |
| 4         | 55   | `totalprice`  | `DOUBLE`     | `1507.25` | `1898.75` | `ZSTD`      | `PLAIN, BIT_PACKED, RLE`            |


The `orderstatus` rows line up with the dictionary encoding section above. It is
a repeated string column with values like `F`, `O`, and `P`, and the footer
shows dictionary-style encoding:


```text
path_in_schema: orderstatus
type: BYTE_ARRAY
compression: ZSTD
encodings: PLAIN_DICTIONARY, BIT_PACKED, RLE
stats_min_value: F
stats_max_value: P
```


The numeric columns show row-group min/max ranges. For `totalprice`, the first
few row groups look like this:


| Row group | Min `totalprice` | Max `totalprice` |
| --------- | ---------------- | ---------------- |
| 0         | `57.25`          | `412.5`          |
| 1         | `419.75`         | `775.0`          |
| 2         | `782.25`         | `1137.5`         |
| 3         | `1144.75`        | `1500.0`         |
| 4         | `1507.25`        | `1898.75`        |


A reader can use this metadata for pruning. Given the filter:


```sql
WHERE totalprice > 1000
```


the first two row groups cannot match, based on their max values. Later row
groups may match, so Trino still has to read and evaluate rows from those
groups.


Each concept from earlier in the note shows up somewhere in the export:


```text
row groups:
  visible through row_group_id and row_group_num_rows

column chunks:
  one metadata row per column inside each row group

statistics:
  visible through stats_min_value and stats_max_value

encoding and compression:
  visible through encodings and compression
```


## Where This Sits With Iceberg


The previous post described the Iceberg metadata chain:


```text
catalog
  -> current metadata.json
    -> current snapshot
      -> manifest list
        -> manifest file
          -> data file
```


That chain chooses data files. Parquet takes over once a selected data file is
opened:


```text
Parquet file
  -> footer metadata
  -> row groups
  -> column chunks
  -> Parquet pages
```


Each layer skips or processes data at a different level:


```text
Iceberg metadata pruning:
  skip manifests or whole data files

Parquet pruning:
  skip row groups, column chunks, or page ranges inside a selected file

Trino execution:
  process decoded batches as Page and Block objects
```


This matters when reading `EXPLAIN` and `EXPLAIN ANALYZE`. `EXPLAIN` can show
that a constraint reached the table scan, but on its own it does not prove how
many Parquet row groups or pages were skipped at runtime.


For runtime proof, I need to combine:


```text
EXPLAIN:
  planned scan shape and constraints

EXPLAIN ANALYZE:
  runtime rows, physical input, splits, and operator stats

Iceberg metadata tables:
  which files and partitions were candidates

Parquet/file-level evidence:
  row group and page-level pruning behavior, when needed
```


That proof pattern fits better in the next read-trace note. This note just sets
up the vocabulary for it.


## Summary

Parquet is columnar, so Trino can read only the columns a query needs. A file is
split into row groups, each row group into one column chunk per column, and each
column chunk into encoded and compressed Parquet pages. Row-group min/max
statistics can prove that a row group cannot match a predicate, but not that
every row matches, so filters are still evaluated. Encoding changes how values
are represented and compression shrinks the bytes; dictionary encoding and RLE
keep repeated values compact. A Parquet page is not a Trino `Page`: the reader
decodes Parquet pages into `Block` objects, which are grouped into in-memory
`Page` batches for operators. Finally, Iceberg metadata pruning, which skips
whole files, and Parquet row-group/page pruning, which skips data inside a
file, are separate layers.

## Self-Check


Questions to answer without looking back:

- Why is Parquet useful for analytical queries?
- What is the difference between a row group and a column chunk?
- What is stored in a Parquet page?
- Why can min/max statistics let Trino skip a row group?
- Why does row-group skipping not replace SQL filter evaluation in every case?
- What is the difference between encoding and compression?
- Why is dictionary encoding useful for repeated string-like columns?
- What is RLE good at representing?
- Why is a Parquet page different from a Trino `Page`?
- Which layer skips whole data files: Iceberg metadata or Parquet metadata?
- Which layer produces `Block` and `Page` objects for Trino operators?

## References

- Apache Parquet concepts
- Trino concepts
- Trino Iceberg connector
- Parquet Viewer
- Chatdb Parquet Metadata Reader

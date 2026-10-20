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


Parquet is the storage-format layer that sits between Iceberg’s selected data
files and Trino’s in-memory execution batches.


For a Trino scan, the useful read shape is:


```text
read file metadata
  -> choose useful row groups
  -> read useful columns
  -> read encoded and compressed Parquet pages
  -> decode batches into Trino blocks
  -> pass Trino Page objects to operators
```


This note is the storage-format bridge before tracing a real Iceberg read query.
The previous note explained how Iceberg metadata points to data files. This one
explains what happens after a selected data file is Parquet: how row groups,
column chunks, pages, encodings, and Trino `Page` objects fit together.


## Row-Based vs Columnar Storage


Start with a tiny table:


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


For this query:


```sql
SELECT avg(age)
FROM users;
```


a columnar format can focus on the `age` column. It does not need to read
`id`, `name`, or `country` just to compute the average.


That is the first Parquet idea to keep:


```text
Parquet is useful to Trino because analytical queries often need some columns,
not every column in every row.
```


## The Parquet File Layout


A Parquet file has a nested layout:


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


The vocabulary:


| Term            | Meaning                                                                    |
| --------------- | -------------------------------------------------------------------------- |
| Parquet file    | One physical data file.                                                    |
| Row group       | A horizontal batch of rows inside the file.                                |
| Column chunk    | Values for one column inside one row group.                                |
| Parquet page    | Smaller encoded and compressed storage unit inside a column chunk.         |
| Footer metadata | File metadata, schema, row group metadata, column statistics, and offsets. |


The two dimensions matter:


```text
row group:
  horizontal slice of rows

column chunk:
  vertical slice for one column inside that row group
```


That is why Parquet can support both:


```text
row-group pruning:
  skip row batches that cannot match a filter

column pruning:
  avoid reading columns the query does not need
```


## Row Groups


A row group is a batch of rows.


For a file with one million rows, the layout might be:


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


A row group is large enough to make metadata useful, but smaller than the whole
file. That makes it a practical skipping boundary.


## Row Group Skipping


Parquet row groups often have statistics for columns. The most useful beginner
example is min/max.


Imagine the `age` column has these row group statistics:


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


Trino can reason:


```text
row group 1:
  max age is 25, so skip

row group 2:
  max age is 45, so skip

row group 3:
  max age is 80, so rows may match
```


This does not mean Parquet knows the final SQL answer. It means the reader can
avoid work for row groups that cannot possibly produce matching rows.


The useful distinction:


```text
statistics can prove "no rows here can match"

statistics often cannot prove "all rows here match"
```


So Trino may still evaluate a filter after reading a row group that might
contain matching rows.


## Column Chunks And Parquet Pages


A row group contains one column chunk per column.


For this row group:


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
encoded/compressed storage unit inside a column chunk.


That matters because the word `page` will appear again in Trino. A Parquet page
and a Trino `Page` are different things.


## Encoding vs Compression


Encoding and compression are related, but they are not the same.


Encoding changes how values are represented.


Compression shrinks the encoded bytes.


The write-side shape is:


```text
original values
  -> encode values into a compact representation
  -> compress the encoded bytes
  -> write bytes to the Parquet file
```


The read-side shape is:


```text
read compressed bytes
  -> decompress bytes
  -> decode values or ids
  -> produce in-memory batches for the engine
```


Two common encoding ideas are dictionary encoding and run-length encoding.


## Dictionary Encoding


Dictionary encoding is useful when a column has repeated values.


Example:


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


Instead of storing every string repeatedly, Parquet can store unique values:


```text
dictionary:
0 -> US
1 -> CA
2 -> MX
```


Then the data can store ids:


```text
0, 0, 1, 0, 1, 2, 0
```


The compact shape is:


```text
dictionary values + integer ids
```


This is useful for string-like columns with repeated values:


```text
country
status
event_type
tenant_id
category
```


## Run-Length Encoding


Run-length encoding, or RLE, is useful when the same value or same small id
appears many times in a row.


Example:


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


In Parquet, RLE is especially useful for compact streams of small integers,
such as dictionary ids and the levels used for null or nested data.


Dictionary encoding and RLE can work together:


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


The important point is not the exact low-level encoding. The useful point is
that Parquet can store repeated values compactly, and Trino can often preserve
compact shapes while processing batches.


## Nulls And Nested Data


Parquet also needs to represent nulls and nested structures.


For a nullable column:


```text
nickname:
Alice
null
Chris
```


the file needs to distinguish:


```text
position 0: value exists
position 1: value is null
position 2: value exists
```


Parquet uses definition levels for this kind of information. A definition level
is a small number that says how much of the value path exists.


For repeated nested data, such as arrays and maps, Parquet also uses repetition
levels. Repetition levels help describe whether a value starts a new parent row
or continues a repeated field.


The compact mental model:


```text
definition level:
  is the value present, or is some part null?

repetition level:
  for arrays and maps, does this continue the same parent row?
```


These levels are usually small repeated numbers, so they are good candidates for
compact RLE and bit-packed encoding.


## How Trino Fits


In Trino terms:


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


The word `page` is overloaded:


```text
Parquet page:
  encoded and compressed bytes inside a file

Trino Page:
  in-memory batch passed between operators
```


They are related by the reader path, but they are not the same object.


```text
Parquet pages
  -> decompression
  -> decoding
  -> Trino blocks
  -> Trino Page
```


## A Simple Trino Read Shape


For this query:


```sql
SELECT name
FROM users
WHERE country = 'US';
```


Trino mainly needs:


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


The scan flow is closer to:


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


For a dictionary-encoded `country` column, the file might contain:


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


The filter can identify matching positions:


```text
position 0: id 0 -> US -> keep
position 1: id 0 -> US -> keep
position 2: id 1 -> CA -> drop
position 3: id 0 -> US -> keep
position 4: id 2 -> MX -> drop
position 5: id 1 -> CA -> drop
```


If the query also returns `name`, Trino uses those kept positions to keep the
matching names from the `name` block.


Some operations can preserve compact block shapes. Other operations materialize
new values. The important rule is:


```text
Trino works in columnar batches, not row-by-row objects.
```


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


The useful footer facts:


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


The `orderstatus` line is a good concrete example for this note. It is a
repeated string column with values like `F`, `O`, and `P`, and the footer shows
dictionary-style encoding:


```text
path_in_schema: orderstatus
type: BYTE_ARRAY
compression: ZSTD
encodings: PLAIN_DICTIONARY, BIT_PACKED, RLE
stats_min_value: F
stats_max_value: P
```


The numeric columns show row-group min/max ranges. For `totalprice`, the first
few row groups look like:


| Row group | Min `totalprice` | Max `totalprice` |
| --------- | ---------------- | ---------------- |
| 0         | `57.25`          | `412.5`          |
| 1         | `419.75`         | `775.0`          |
| 2         | `782.25`         | `1137.5`         |
| 3         | `1144.75`        | `1500.0`         |
| 4         | `1507.25`        | `1898.75`        |


That is the kind of metadata a reader can use for pruning. If a query asks for:


```sql
WHERE totalprice > 1000
```


then the first two row groups cannot match based on their max value. Later row
groups may match, so Trino still needs to read and evaluate rows from those
groups.


This is enough evidence for the post:


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


That chain chooses data files.


Parquet starts after a selected data file is opened:


```text
Parquet file
  -> footer metadata
  -> row groups
  -> column chunks
  -> Parquet pages
```


The layers are separate:


```text
Iceberg metadata pruning:
  skip manifests or whole data files

Parquet pruning:
  skip row groups, column chunks, or page ranges inside a selected file

Trino execution:
  process decoded batches as Page and Block objects
```


This distinction matters when reading `EXPLAIN` and `EXPLAIN ANALYZE`.
`EXPLAIN` can show that a constraint reached the table scan. It does not, by
itself, prove exactly how many Parquet row groups or pages were skipped at
runtime.


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


That proof pattern belongs more naturally in the next read-trace note. This
note is just the vocabulary needed before that trace.


## What To Remember

- Parquet is columnar, so Trino can read useful columns instead of every column.
- A Parquet file is organized into row groups, column chunks, and Parquet pages.
- Row groups can carry statistics such as min/max values.
- Row-group statistics can prove that a row group cannot match a predicate.
- Column chunks let the reader avoid unneeded columns.
- Parquet pages are encoded and compressed storage units.
- Encoding changes representation; compression shrinks bytes.
- Dictionary encoding and RLE help repeated values stay compact.
- A Parquet page is not a Trino `Page`.
- Trino `Page` objects are in-memory batches made of `Block` objects.
- Iceberg metadata pruning and Parquet row-group/page pruning are different
layers.

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

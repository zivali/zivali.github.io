---
title: Best practice for cloning a partitioned hive table
date: 2023-10-27T08:09:00.000Z
lastmod: 2026-06-14T19:54:00.000Z
draft: false
series: []
tags:
  - Hive
categories: []
summary: Step-by-step notes for cloning a partitioned Hive table by recreating schema, copying files, and repairing metadata.
authors:
  - Ziva Li
---
<link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/katex@0.16.2/dist/katex.min.css" integrity="sha384-bYdxxUwYipFNohQlHt0bjN/LCpueqWz13HufFEV1SUatKs1cm4L6fFgCi1jT643X" crossorigin="anonymous">

Cloning a partitioned Hive table takes three steps: recreate the schema on the target, copy the data files, and then tell the metastore about the partitions. The examples below use a table called `mycart`.

1. Create the schema on the target cluster

	Run `show create table` on the source table to get its DDL.


	```bash
	> show create table mycart;
	CREATE TABLE `mycart`(
	  `id` string,
	  `gd_id` int,
	  `create_day_id` int,
	  `remove_day_id` int,
	  `data_date` string)
	ROW FORMAT SERDE
	  'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
	STORED AS INPUTFORMAT
	  'org.apache.hadoop.mapred.TextInputFormat'
	OUTPUTFORMAT
	  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
	LOCATION
	  'hdfs:///{original_directory}/mycart'
	TBLPROPERTIES (
	  'last_partition_modified_time'='0',
	  'numFiles'='0',
	  'totalSize'='0',
	  'transient_lastDdlTime'='1698400311')
	```


	Remove the `LOCATION` clause so the new table gets its own directory, and decide whether the new table needs the same `TBLPROPERTIES`. Without either, the statement looks like this:


	```bash
	CREATE TABLE `mycart`(
	  `id` string,
	  `gd_id` int,
	  `create_day_id` int,
	  `remove_day_id` int,
	  `data_date` string)
	ROW FORMAT SERDE
	  'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
	STORED AS INPUTFORMAT
	  'org.apache.hadoop.mapred.TextInputFormat'
	OUTPUTFORMAT
	  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
	```

1. Copy the partitions you want to clone to the target cluster

	Within the same cluster, use `hadoop fs -cp` to copy the table directory to the new location. The original directory is the `LOCATION` from the DDL in step 1.


	```bash
	hadoop fs -cp hdfs:///{original_directory}/mycart hdfs:///{new_directory}
	
	```


	To copy between clusters, use `hadoop distcp` instead.


	```bash
	# -p <arg> can be specified to preserve status (replication, block-size, user, group, permission ...)
	hadoop distcp hdfs:///{original_directory}/mycart hdfs:///{new_directory}
	```

1. Sync partition info to table metadata

	The copied files are not visible to queries until the metastore knows about their partitions. `msck repair table` reads the directory structure, creates partitions from it, and updates the Hive metastore. It also removes partitions from the metastore whose directories no longer exist.

	On a large table this can be slow, for a few reasons:
	- `msck repair` does a full traversal of every sub-directory under the table directory. It parses the file names, checks that they are valid, checks whether each partition already exists in the metastore, and adds only the ones that are missing.
	- Each filesystem listing is an RPC to the namenode (HDFS) or a web-service call (S3), and these add up.
	- To know which partitions already exist, it also has to list every partition the metastore has for the table.

	```bash
	msck repair table mycart;
	
	```


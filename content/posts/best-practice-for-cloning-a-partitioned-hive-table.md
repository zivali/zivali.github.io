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

1. Create schema on target cluster

	We can use `show create table` to get the create table sql of the table we would like to clone.


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


	The LOCATION property should be removed.


	You can determine whether you need the same TBLPROPERTIES in your new table. e.g.


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

1. Copy the partitions you would like to clone to your target cluster

	Same cluster: 


	Use `hadoop fs -cp` to copy the table to targeted location.


	The original directory can be found in the create table sql we got in step 1.


	```bash
	hadoop fs -cp hdfs:///{original_directory}/mycart hdfs:///{new_directory}
	
	```


	Intra-cluster copying


	Use `hadoop distcp` to perform Intra-cluster copying.


	```bash
	# -p <arg> can be specified to preserve status (replication, block-size, user, group, permission ...)
	hadoop distcp hdfs:///{original_directory}/mycart hdfs:///{new_directory}
	```

1. Sync partition info to table metadata

	`msck repair table` Reads the directory structure, creates partitions out of it and then updates the hive metastore. 

	- This command removes non-existing partitions from metastore as well.
	- This may be slow because:
		- `msck repair` will have to do a full-tree traversal of all the sub-directories under the table directory, parse the file names, make sure that the file names are valid, check if the partition is already existing in the metastore and then add the only partitions which are not present in the metastore.
		- Each listing on the filesystem is a RPC to the namenode (HDFS) or a web-service (S3) which can add to significant amount of time.
		- Additionally, in order to figure out if the partition is already present in metastore or not, it needs to do a full listing of all the partitions which metastore knows of for the table.

	```bash
	msck repair table mycart;
	
	```


This demo shows the schema tracking but no the schema evolution flag set in the SparkSession.

Demo scenario:
1. Run `DemoRunner`. It should run with the following output:
```
Creating table...
...table created
Starting streaming reader
-------------------------------------------
Batch: 0
-------------------------------------------
+------+------+
|number|letter|
+------+------+
|     1|     a|
|     2|     b|
|     3|     c|
|     4|     d|
+------+------+

Stream has started. We can now do the schema evolution.
Renaming an existing column...
-------------------------------------------
Batch: 1
-------------------------------------------
+------+------+
|number|letter|
+------+------+
+------+------+
```
2. Next, it should automatically fail with:
```
The schema or metadata tracking log has been updated.
Please restart the stream to continue processing using the updated metadata.
Updated schema: root
-- number: integer (nullable = true)
-- letter2: string (nullable = true)
.
Updated table configurations: delta.enableChangeDataFeed:true, delta.columnMapping.mode:name, delta.columnMapping.maxColumnId:2.
Updated table protocol: 2,5
=== Streaming Query ===
Identifier: [id = 41fd6836-0204-461c-b9e5-8f6c84f3b06b, runId = 877b252e-83e4-4fba-995c-f599e32c9464]
Current Committed Offsets: {DeltaSource[file:/tmp/delta-lake-playground/014_schema_tracking/warehouse/numbers_with_letters]: {"sourceVersion":3,"reservoirId":"e4da2bbb-5251-4c38-ab98-3d4718af5579","reservoirVersion":2,"index":-20,"isStartingVersion":false}}
Current Available Offsets: {DeltaSource[file:/tmp/delta-lake-playground/014_schema_tracking/warehouse/numbers_with_letters]: {"sourceVersion":3,"reservoirId":"e4da2bbb-5251-4c38-ab98-3d4718af5579","reservoirVersion":2,"index":-19,"isStartingVersion":false}}

Current State: ACTIVE
Thread State: RUNNABLE

Logical Plan:
WriteToMicroBatchDataSource org.apache.spark.sql.execution.streaming.ConsoleTable$@3e9aa755, 41fd6836-0204-461c-b9e5-8f6c84f3b06b, [checkpointLocation=/tmp/delta-lake-playground/014_schema_tracking//demo2/checkpoint], Append
+- SubqueryAlias spark_catalog.default.numbers_with_letters
   +- StreamingExecutionRelation DeltaSource[file:/tmp/delta-lake-playground/014_schema_tracking/warehouse/numbers_with_letters], [number#1763, letter#1764], `spark_catalog`.`default`.`numbers_with_letters`, org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe
...
```
The failure is expected as the schema evolved and the error message asks you to confirm this evolution to 
avoid breaking your downstream consumers.
3. Restart the [StreamDeltaTable.scala](StreamDeltaTable.scala). It should fail once again, but this time ask you for 
enable the changes:
```
Exception in thread "main" org.apache.spark.sql.streaming.StreamingQueryException: [DELTA_STREAMING_CANNOT_CONTINUE_PROCESSING_POST_SCHEMA_EVOLUTION] We've detected one or more non-additive schema change(s) (RENAME COLUMN) between Delta version 1 and 2 in the Delta streaming source.
Please check if you want to manually propagate the schema change(s) to the sink table before we proceed with stream processing using the finalized schema at 2.
Once you have fixed the schema of the sink table or have decided there is no need to fix, you can set (one of) the following SQL configurations to unblock the non-additive schema change(s) and continue stream processing.
To unblock for this particular stream just for this series of schema change(s): set `spark.databricks.delta.streaming.allowSourceColumnRenameAndDrop.ckpt_-1180187956 = 2`.
To unblock for this particular stream: set `spark.databricks.delta.streaming.allowSourceColumnRenameAndDrop.ckpt_-1180187956 = always`
To unblock for all streams: set `spark.databricks.delta.streaming.allowSourceColumnRenameAndDrop = always`.
Alternatively if applicable, you may replace the `allowSourceColumnRenameAndDrop` with `allowSourceColumnRename` in the SQL conf to unblock stream for just this schema change type.
=== Streaming Query ===
Identifier: [id = 41fd6836-0204-461c-b9e5-8f6c84f3b06b, runId = c4efcc45-88f7-447b-8a30-17c498e26a5d]
Current Committed Offsets: {}
Current Available Offsets: {}

Current State: INITIALIZING
Thread State: RUNNABLE
```
4. Configure the `SparkSession` in [DemoRunner2.scala](DemoRunner2.scala):
```
val sparkSession = getOrCreateDeltaLakeSparkSession(extraConfig = 
    Map("spark.databricks.delta.streaming.allowSourceColumnRenameAndDrop.ckpt_-1180187956" -> "always"))
```
⚠️ Your checkpoint hash may be different. Please take it from the error message.
5. Run the `DemoRunner2`. It should process the _letter2_ column correctly:
```
Starting streaming reader
-------------------------------------------
Batch: 2
-------------------------------------------
+------+-------+
|number|letter2|
+------+-------+
+------+-------+

Stream has started. Modifying the table again.
Renaming an existing column...
-------------------------------------------
Batch: 3
-------------------------------------------
+------+-------+
|number|letter2|
+------+-------+
|     1|      a|
|     2|      b|
|     3|      c|
|     4|      d|
+------+-------+

-------------------------------------------
Batch: 4
-------------------------------------------
+------+-------+
|number|letter2|
+------+-------+
+------+-------+
```
6. But it should fail soon after, at processing the renamed _number_:
```
The schema or metadata tracking log has been updated.
Please restart the stream to continue processing using the updated metadata.
Updated schema: root
-- number2: integer (nullable = true)
-- letter2: string (nullable = true)
.
Updated table configurations: delta.enableChangeDataFeed:true, delta.columnMapping.mode:name, delta.columnMapping.maxColumnId:2.
Updated table protocol: 2,5
=== Streaming Query ===
Identifier: [id = 41fd6836-0204-461c-b9e5-8f6c84f3b06b, runId = 5c6fa498-23db-4cb8-9970-35b416652829]
Current Committed Offsets: {DeltaSource[file:/tmp/delta-lake-playground/014_schema_tracking/warehouse/numbers_with_letters]: {"sourceVersion":3,"reservoirId":"e4da2bbb-5251-4c38-ab98-3d4718af5579","reservoirVersion":4,"index":-20,"isStartingVersion":false}}
Current Available Offsets: {DeltaSource[file:/tmp/delta-lake-playground/014_schema_tracking/warehouse/numbers_with_letters]: {"sourceVersion":3,"reservoirId":"e4da2bbb-5251-4c38-ab98-3d4718af5579","reservoirVersion":4,"index":-19,"isStartingVersion":false}}
```
7. Run the `StreamDeltaTable`. Once again, the stream fails with the same _allowSourceColumn..._ error message:
```
Please check if you want to manually propagate the schema change(s) to the sink table before we proceed with stream processing using the finalized schema at 4.
Once you have fixed the schema of the sink table or have decided there is no need to fix, you can set (one of) the following SQL configurations to unblock the non-additive schema change(s) and continue stream processing.
To unblock for this particular stream just for this series of schema change(s): set `spark.databricks.delta.streaming.allowSourceColumnRenameAndDrop.ckpt_-1180187956 = 4`.
To unblock for this particular stream: set `spark.databricks.delta.streaming.allowSourceColumnRenameAndDrop.ckpt_-1180187956 = always`
To unblock for all streams: set `spark.databricks.delta.streaming.allowSourceColumnRenameAndDrop = always`.
Alternatively if applicable, you may replace the `allowSourceColumnRenameAndDrop` with `allowSourceColumnRename` in the SQL conf to unblock stream for just this schema change type.
```
8. Add the general configuration for the stream renaming in the `SparkSession` from the `DemoRunner3`:
```
val sparkSession = getOrCreateDeltaLakeSparkSession(extraConfig =
  Map("spark.databricks.delta.streaming.allowSourceColumnRenameAndDrop" -> "always"))
```
9. Run the `DemoRunner3`. Yet again, it should fail with the schema evolution error, and so despite the configuration
applied to all future schema changes (_number2_ renamed to _number3_):
```
Starting streaming reader
-------------------------------------------
Batch: 5
-------------------------------------------
+-------+-------+
|number2|letter2|
+-------+-------+
+-------+-------+

Stream has started. Modifying the table again.
Renaming an existing column...
-------------------------------------------
Batch: 6
-------------------------------------------
+-------+-------+
|number2|letter2|
+-------+-------+
|      1|      a|
|      2|      b|
|      3|      c|
|      4|      d|
+-------+-------+

-------------------------------------------
Batch: 7
-------------------------------------------
+-------+-------+
|number2|letter2|
+-------+-------+
+-------+-------+

The schema or metadata tracking log has been updated.
Please restart the stream to continue processing using the updated metadata.
Updated schema: root
-- number3: integer (nullable = true)
-- letter2: string (nullable = true)
.
Updated table configurations: delta.enableChangeDataFeed:true, delta.columnMapping.mode:name, delta.columnMapping.maxColumnId:2.
Updated table protocol: 2,5
```
💡 As you can see, the configuration property is only a convenience method to not have to enable changes at each renamed 
or dropped column. It doesn't help in running the stream continuously.

❓ Defining the schema tracking alone is not enough. But what happens if we do a non-additive schema change without specifying it?
  It's the topic of the next demo.
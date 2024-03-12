# Streaming a Delta table with overwritten schema

1. Explain the [DemoRunner.scala](DemoRunner.scala):
* the job starts by creating a table with two columns
* next it starts the streaming reader that simply takes the table and prints it to the console sink
* finally, the main job evolves the schema by renaming _number_ to _id_number_, hence it does a _non additive_ schema change
2. Run the `DemoRunner`. It should fail with the following exception:
```
Exception in thread "Thread-43" org.apache.spark.sql.streaming.StreamingQueryException: 
[DELTA_STREAMING_INCOMPATIBLE_SCHEMA_CHANGE_USE_SCHEMA_LOG] Streaming read is not supported on tables with read-incompatible schema changes (e.g. rename or drop or datatype changes).

Please provide a 'schemaTrackingLocation' to enable non-additive schema evolution for Delta stream processing.
See https://docs.delta.io/latest/versioning.html#column-mapping for more details.
Read schema: {"type":"struct","fields":[
{"name":"number","type":"integer","nullable":true,"metadata":{"delta.columnMapping.id":1,"delta.columnMapping.physicalName":"number"}},
{"name":"letter","type":"string","nullable":true,"metadata":{"delta.columnMapping.id":2,"delta.columnMapping.physicalName":"letter"}}]}.
 
Incompatible data schema: {"type":"struct","fields":[
{"name":"id_number","type":"integer","nullable":true,"metadata":{"delta.columnMapping.id":1,"delta.columnMapping.physicalName":"number"}},
{"name":"letter","type":"string","nullable":true,"metadata":{"delta.columnMapping.id":2,"delta.columnMapping.physicalName":"letter"}}]}.
```

3. Even though the error message asks us to use the _schema tracking_ feature, we're going to use a hack 
and allow this evolution with `spark.databricks.delta.streaming.unsafeReadOnIncompatibleColumnMappingSchemaChanges.enabled`.
To do so, uncomment the following line in the `StreamingReader`:
```
val sparkSession = getOrCreateSparkSessionWithDeltaLake(extraConfig = Map(
  "spark.databricks.delta.streaming.unsafeReadOnIncompatibleColumnMappingSchemaChanges.enabled" -> true,
))
```
4. Start the `StreamingReader`. You should see the rows from the changed table correctly processed.
```
-------------------------------------------
Batch: 1
-------------------------------------------
+---------+------+
|id_number|letter|
+---------+------+
|        1|     a|
|        2|     b|
|        3|     c|
|        4|     d|
+---------+------+
``` 
💡 Please notice that the flag doesn't let you streaming a Delta table without interruption when the schema changes.
Structured Streaming job will always fail whenever that happens and the flag lets you restart the query 
without managing the schema tracking. I'm not introducing the schema tracking here as it's the topic presented
in the [/014_schema_tracking](014_schema_tracking)
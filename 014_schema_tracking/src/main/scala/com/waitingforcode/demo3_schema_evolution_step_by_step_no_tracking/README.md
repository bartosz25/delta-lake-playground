This demo shows the schema evolution without the schema tracking enabled.

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
```
2. Next, it should automatically fail with:
```
[DELTA_STREAMING_INCOMPATIBLE_SCHEMA_CHANGE_USE_SCHEMA_LOG] Streaming read is not supported on tables with read-incompatible schema changes (e.g. rename or drop or datatype changes).
Please provide a 'schemaTrackingLocation' to enable non-additive schema evolution for Delta stream processing.
See https://docs.delta.io/latest/versioning.html#column-mapping for more details.
Read schema: {"type":"struct","fields":[{"name":"number","type":"integer","nullable":true,"metadata":{"delta.columnMapping.id":1,"delta.columnMapping.physicalName":"number"}},{"name":"letter","type":"string","nullable":true,"metadata":{"delta.columnMapping.id":2,"delta.columnMapping.physicalName":"letter"}}]}. Incompatible data schema: {"type":"struct","fields":[{"name":"number","type":"integer","nullable":true,"metadata":{"delta.columnMapping.id":1,"delta.columnMapping.physicalName":"number"}},{"name":"letter2","type":"string","nullable":true,"metadata":{"delta.columnMapping.id":2,"delta.columnMapping.physicalName":"letter"}}]}.
=== Streaming Query ===
Identifier: [id = 8e4c79b1-3258-4ee7-845c-d52e51a5c9db, runId = d6d325d4-aa1d-43ba-b004-587d979fe91b]
Current Committed Offsets: {DeltaSource[file:/tmp/delta-lake-playground/014_schema_tracking/warehouse/numbers_with_letters]: {"sourceVersion":1,"reservoirId":"aad1af2d-ae58-4659-82ef-30ba9eb7a19d","reservoirVersion":2,"index":-1,"isStartingVersion":false}}
Current Available Offsets: {DeltaSource[file:/tmp/delta-lake-playground/014_schema_tracking/warehouse/numbers_with_letters]: {"sourceVersion":1,"reservoirId":"aad1af2d-ae58-4659-82ef-30ba9eb7a19d","reservoirVersion":2,"index":-1,"isStartingVersion":false}}

Current State: ACTIVE
Thread State: RUNNABLE
```
The error message is now different. In other words you can:
* restart the stream from the most recent checkpoint location
* define the schema tracking location, as stated in the error message; we have done it already, so we're going to try the
  first approach
3. Change the checkpoint location in the [StreamDeltaTable.scala](StreamDeltaTable.scala):
```
val checkpointDir = s"${DemoDir}/checkpoint-2"
```
4. Run the `StreamDeltaTable`. You should correctly process the records now:
```
-------------------------------------------
Batch: 0
-------------------------------------------
+------+-------+
|number|letter2|
+------+-------+
|     1|      a|
|     2|      b|
|     3|      c|
|     4|      d|
|     1|      a|
|     2|      b|
|     3|      c|
|     4|      d|
+------+-------+
```

The drawback is the cost of the backfill and increased latency as you're restarting from the beginning.
Demo scenario:
1. Run `DemoRunner`. It should run with the following output:
```
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
2. It should fail with:
```
Caused by: org.apache.spark.sql.delta.DeltaRuntimeException: [DELTA_STREAMING_METADATA_EVOLUTION] The schema, table configuration or protocol of your Delta table has changed during streaming.
The schema or metadata tracking log has been updated.
Please restart the stream to continue processing using the updated metadata.
Updated schema: root
 |-- number: integer (nullable = true)
 |-- letter2: string (nullable = true)
.
Updated table configurations: delta.enableChangeDataFeed:true, delta.columnMapping.mode:name, delta.columnMapping.maxColumnId:2.
Updated table protocol: 2,5
```
The failure is expected as the schema evolved and the error message asks you to confirm this evolution to 
avoid breaking your downstream consumers.
3. Restart the [StreamDeltaTable.scala](StreamDeltaTable.scala). It should correctly process the new column:
```
-------------------------------------------
Batch: 2
-------------------------------------------
+------+-------+
|number|letter2|
+------+-------+
+------+-------+

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
```
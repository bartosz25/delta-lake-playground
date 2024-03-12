# Streaming a Delta table from an arbitrary past version with schema changes

1. Explain the [DemoRunner.scala](DemoRunner.scala):
* the job starts by creating a table with 4 versions; letters in the given version are prefixed with a unique number
  * there is also one column rename operation, just after the version the streaming job starts to read
* finally, the job  starts the streaming reader that simply takes the table and prints it to the console sink
2. Run the `DemoRunner`. It should correctly set the table up but the streaming reader should fail:
```
Generating input table versions...
...version 0
...version 1 (metadata)
...version 2
...version 3
...version 4 (metadata)
...version 5
...generated all the versions. Checking the table history.
+-------+-----------------------+------+--------+---------------------------------+-------------------------------------------------------------------------------------------------------------+----+--------+---------+-----------+--------------+-------------+----------------------------------------------------------+------------+-----------------------------------+
|version|timestamp              |userId|userName|operation                        |operationParameters                                                                                          |job |notebook|clusterId|readVersion|isolationLevel|isBlindAppend|operationMetrics                                          |userMetadata|engineInfo                         |
+-------+-----------------------+------+--------+---------------------------------+-------------------------------------------------------------------------------------------------------------+----+--------+---------+-----------+--------------+-------------+----------------------------------------------------------+------------+-----------------------------------+
|5      |2024-03-09 06:49:45.411|NULL  |NULL    |WRITE                            |{mode -> Append, partitionBy -> []}                                                                          |NULL|NULL    |NULL     |4          |Serializable  |true         |{numFiles -> 1, numOutputRows -> 2, numOutputBytes -> 885}|NULL        |Apache-Spark/3.5.0 Delta-Lake/3.1.0|
|4      |2024-03-09 06:49:41.907|NULL  |NULL    |RENAME COLUMN                    |{oldColumnPath -> number, newColumnPath -> id_number}                                                        |NULL|NULL    |NULL     |3          |Serializable  |true         |{}                                                        |NULL        |Apache-Spark/3.5.0 Delta-Lake/3.1.0|
|3      |2024-03-09 06:49:37.871|NULL  |NULL    |WRITE                            |{mode -> Append, partitionBy -> []}                                                                          |NULL|NULL    |NULL     |2          |Serializable  |true         |{numFiles -> 1, numOutputRows -> 2, numOutputBytes -> 885}|NULL        |Apache-Spark/3.5.0 Delta-Lake/3.1.0|
|2      |2024-03-09 06:49:30.767|NULL  |NULL    |WRITE                            |{mode -> Append, partitionBy -> []}                                                                          |NULL|NULL    |NULL     |1          |Serializable  |true         |{numFiles -> 1, numOutputRows -> 2, numOutputBytes -> 885}|NULL        |Apache-Spark/3.5.0 Delta-Lake/3.1.0|
|1      |2024-03-09 06:49:20.955|NULL  |NULL    |SET TBLPROPERTIES                |{properties -> {"delta.minReaderVersion":"2","delta.minWriterVersion":"5","delta.columnMapping.mode":"name"}}|NULL|NULL    |NULL     |0          |Serializable  |true         |{}                                                        |NULL        |Apache-Spark/3.5.0 Delta-Lake/3.1.0|
|0      |2024-03-09 06:49:04.911|NULL  |NULL    |CREATE OR REPLACE TABLE AS SELECT|{isManaged -> true, description -> NULL, partitionBy -> [], properties -> {}}                                |NULL|NULL    |NULL     |NULL       |Serializable  |false        |{numFiles -> 1, numOutputRows -> 2, numOutputBytes -> 727}|NULL        |Apache-Spark/3.5.0 Delta-Lake/3.1.0|
+-------+-----------------------+------+--------+---------------------------------+-------------------------------------------------------------------------------------------------------------+----+--------+---------+-----------+--------------+-------------+----------------------------------------------------------+------------+-----------------------------------+
```

And for the failure:
```
Starting the streaming from the version 2
Exception in thread "main" org.apache.spark.sql.streaming.StreamingQueryException: 
[DELTA_STREAMING_INCOMPATIBLE_SCHEMA_CHANGE_USE_SCHEMA_LOG] Streaming read is not supported on tables with read-incompatible
 schema changes (e.g. rename or drop or datatype changes).
Please provide a 'schemaTrackingLocation' to enable non-additive schema evolution for Delta stream processing.
See https://docs.delta.io/latest/versioning.html#column-mapping for more details.
Read schema: {"type":"struct","fields":[{"name":"number","type":"integer","nullable":true,"metadata":{"delta.columnMapping.id":1,"delta.columnMapping.physicalName":"number"}},{"name":"letter","type":"string","nullable":true,"metadata":{"delta.columnMapping.id":2,"delta.columnMapping.physicalName":"letter"}}]}. Incompatible data schema: {"type":"struct","fields":[{"name":"id_number","type":"integer","nullable":true,"metadata":{"delta.columnMapping.id":1,"delta.columnMapping.physicalName":"number"}},{"name":"letter","type":"string","nullable":true,"metadata":{"delta.columnMapping.id":2,"delta.columnMapping.physicalName":"letter"}}]}.
```
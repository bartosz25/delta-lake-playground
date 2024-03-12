# Streaming a Delta table from an arbitrary past version

1. Explain the [DemoRunner.scala](DemoRunner.scala):
* the job starts by creating a table with 4 versions; letters in the given version are prefixed with a unique number
* next it displays the table history, just for the record
* finally, the job  starts the streaming reader that simply takes the table and prints it to the console sink
2. Run the `DemoRunner`. It prints:
```
Generating input table versions...
...version 0
...version 1
...version 2
....version 3
...generated all the versions. Checking the table history.
+-------+-----------------------+------+--------+---------------------------------+-----------------------------------------------------------------------------+----+--------+---------+-----------+--------------+-------------+----------------------------------------------------------+------------+-----------------------------------+
|version|timestamp              |userId|userName|operation                        |operationParameters                                                          |job |notebook|clusterId|readVersion|isolationLevel|isBlindAppend|operationMetrics                                          |userMetadata|engineInfo                         |
+-------+-----------------------+------+--------+---------------------------------+-----------------------------------------------------------------------------+----+--------+---------+-----------+--------------+-------------+----------------------------------------------------------+------------+-----------------------------------+
|3      |2024-03-09 06:37:23.89 |NULL  |NULL    |WRITE                            |{mode -> Append, partitionBy -> []}                                          |NULL|NULL    |NULL     |2          |Serializable  |true         |{numFiles -> 1, numOutputRows -> 2, numOutputBytes -> 699}|NULL        |Apache-Spark/3.5.0 Delta-Lake/3.1.0|
|2      |2024-03-09 06:37:19.03 |NULL  |NULL    |WRITE                            |{mode -> Append, partitionBy -> []}                                          |NULL|NULL    |NULL     |1          |Serializable  |true         |{numFiles -> 1, numOutputRows -> 2, numOutputBytes -> 699}|NULL        |Apache-Spark/3.5.0 Delta-Lake/3.1.0|
|1      |2024-03-09 06:37:11.642|NULL  |NULL    |WRITE                            |{mode -> Append, partitionBy -> []}                                          |NULL|NULL    |NULL     |0          |Serializable  |true         |{numFiles -> 1, numOutputRows -> 2, numOutputBytes -> 699}|NULL        |Apache-Spark/3.5.0 Delta-Lake/3.1.0|
|0      |2024-03-09 06:36:56.11 |NULL  |NULL    |CREATE OR REPLACE TABLE AS SELECT|{isManaged -> true, description -> NULL, partitionBy -> [], properties -> {}}|NULL|NULL    |NULL     |NULL       |Serializable  |false        |{numFiles -> 1, numOutputRows -> 2, numOutputBytes -> 727}|NULL        |Apache-Spark/3.5.0 Delta-Lake/3.1.0|
+-------+-----------------------+------+--------+---------------------------------+-----------------------------------------------------------------------------+----+--------+---------+-----------+--------------+-------------+----------------------------------------------------------+------------+-----------------------------------+

Starting the streaming from the version 2
-------------------------------------------
Batch: 0
-------------------------------------------
+------+------+
|number|letter|
+------+------+
|    20|   2-a|
|    40|   2-b|
|    30|   3-a|
|    60|   3-b|
+------+------+
```
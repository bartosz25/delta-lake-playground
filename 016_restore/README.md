# Restore command

1. Run [SetupJob.scala](src/main/scala/com/waitingforcode/demo1_restore_table/SetupJob.scala)
* it creates a new table with 3 commits

2. Run [TimeTravelExample.scala](src/main/scala/com/waitingforcode/demo1_restore_table/TimeTravelExample.scala)
* it's an example of the time-travel where the job first reads all available versions and later reads the table from
the version 1
* in the end, the job prints the most recent version yet again

The expected outcome is:
```
+-------+-----------------------+------+--------+---------------------------------+----------------------------------------------------------------------------------------------+----+--------+---------+-----------+--------------+-------------+-----------------------------------------------------------+------------+-----------------------------------+
|version|timestamp              |userId|userName|operation                        |operationParameters                                                                           |job |notebook|clusterId|readVersion|isolationLevel|isBlindAppend|operationMetrics                                           |userMetadata|engineInfo                         |
+-------+-----------------------+------+--------+---------------------------------+----------------------------------------------------------------------------------------------+----+--------+---------+-----------+--------------+-------------+-----------------------------------------------------------+------------+-----------------------------------+
|2      |2024-10-09 04:07:04.819|NULL  |NULL    |WRITE                            |{mode -> Append, partitionBy -> []}                                                           |NULL|NULL    |NULL     |1          |Serializable  |true         |{numFiles -> 2, numOutputRows -> 2, numOutputBytes -> 1368}|NULL        |Apache-Spark/3.5.1 Delta-Lake/3.2.0|
|1      |2024-10-09 04:07:03.763|NULL  |NULL    |WRITE                            |{mode -> Append, partitionBy -> []}                                                           |NULL|NULL    |NULL     |0          |Serializable  |true         |{numFiles -> 4, numOutputRows -> 4, numOutputBytes -> 2736}|NULL        |Apache-Spark/3.5.1 Delta-Lake/3.2.0|
|0      |2024-10-09 04:07:00.877|NULL  |NULL    |CREATE OR REPLACE TABLE AS SELECT|{partitionBy -> [], clusterBy -> [], description -> NULL, isManaged -> true, properties -> {}}|NULL|NULL    |NULL     |NULL       |Serializable  |false        |{numFiles -> 3, numOutputRows -> 3, numOutputBytes -> 2052}|NULL        |Apache-Spark/3.5.1 Delta-Lake/3.2.0|
+-------+-----------------------+------+--------+---------------------------------+----------------------------------------------------------------------------------------------+----+--------+---------+-----------+--------------+-------------+-----------------------------------------------------------+------------+-----------------------------------+

--- Printing all records ---
+------+------+
|number|letter|
+------+------+
|1     |a     |
|2     |b     |
|3     |c     |
|4     |d     |
|5     |e     |
|6     |f     |
|7     |g     |
|8     |h     |
|9     |i     |
+------+------+

--- Printing records for the version 1 ---
+------+------+
|number|letter|
+------+------+
|1     |a     |
|2     |b     |
|3     |c     |
|4     |d     |
|5     |e     |
|6     |f     |
|7     |g     |
+------+------+

--- Printing all records after time travel ---
+------+------+
|number|letter|
+------+------+
|1     |a     |
|2     |b     |
|3     |c     |
|4     |d     |
|5     |e     |
|6     |f     |
|7     |g     |
|8     |h     |
|9     |i     |
+------+------+
```


3. Run [RestoreExample.scala](src/main/scala/com/waitingforcode/demo1_restore_table/RestoreExample.scala)
* it's an example of the `RESTORE` command that performs the same actions as the time-travel but in the end, it 
persists the content of the version 1 by making it the new recent version

The expected outcome is:
```
+-------+-----------------------+------+--------+---------------------------------+----------------------------------------------------------------------------------------------+----+--------+---------+-----------+--------------+-------------+-----------------------------------------------------------+------------+-----------------------------------+
|version|timestamp              |userId|userName|operation                        |operationParameters                                                                           |job |notebook|clusterId|readVersion|isolationLevel|isBlindAppend|operationMetrics                                           |userMetadata|engineInfo                         |
+-------+-----------------------+------+--------+---------------------------------+----------------------------------------------------------------------------------------------+----+--------+---------+-----------+--------------+-------------+-----------------------------------------------------------+------------+-----------------------------------+
|2      |2024-10-09 04:07:04.819|NULL  |NULL    |WRITE                            |{mode -> Append, partitionBy -> []}                                                           |NULL|NULL    |NULL     |1          |Serializable  |true         |{numFiles -> 2, numOutputRows -> 2, numOutputBytes -> 1368}|NULL        |Apache-Spark/3.5.1 Delta-Lake/3.2.0|
|1      |2024-10-09 04:07:03.763|NULL  |NULL    |WRITE                            |{mode -> Append, partitionBy -> []}                                                           |NULL|NULL    |NULL     |0          |Serializable  |true         |{numFiles -> 4, numOutputRows -> 4, numOutputBytes -> 2736}|NULL        |Apache-Spark/3.5.1 Delta-Lake/3.2.0|
|0      |2024-10-09 04:07:00.877|NULL  |NULL    |CREATE OR REPLACE TABLE AS SELECT|{partitionBy -> [], clusterBy -> [], description -> NULL, isManaged -> true, properties -> {}}|NULL|NULL    |NULL     |NULL       |Serializable  |false        |{numFiles -> 3, numOutputRows -> 3, numOutputBytes -> 2052}|NULL        |Apache-Spark/3.5.1 Delta-Lake/3.2.0|
+-------+-----------------------+------+--------+---------------------------------+----------------------------------------------------------------------------------------------+----+--------+---------+-----------+--------------+-------------+-----------------------------------------------------------+------------+-----------------------------------+

--- Printing all records ---
+------+------+
|number|letter|
+------+------+
|1     |a     |
|2     |b     |
|3     |c     |
|4     |d     |
|5     |e     |
|6     |f     |
|7     |g     |
|8     |h     |
|9     |i     |
+------+------+

--- Restoring the table to the version 1 ---
--- Printing all records after the restore ---
+------+------+
|number|letter|
+------+------+
|1     |a     |
|2     |b     |
|3     |c     |
|4     |d     |
|5     |e     |
|6     |f     |
|7     |g     |
+------+------+

--- Printing the history ---
+-------+-----------------------+------+--------+---------------------------------+----------------------------------------------------------------------------------------------+----+--------+---------+-----------+--------------+-------------+-----------------------------------------------------------------------------------------------------------------------------------------------------------+------------+-----------------------------------+
|version|timestamp              |userId|userName|operation                        |operationParameters                                                                           |job |notebook|clusterId|readVersion|isolationLevel|isBlindAppend|operationMetrics                                                                                                                                           |userMetadata|engineInfo                         |
+-------+-----------------------+------+--------+---------------------------------+----------------------------------------------------------------------------------------------+----+--------+---------+-----------+--------------+-------------+-----------------------------------------------------------------------------------------------------------------------------------------------------------+------------+-----------------------------------+
|3      |2024-10-09 04:16:53.617|NULL  |NULL    |RESTORE                          |{version -> 1, timestamp -> NULL}                                                             |NULL|NULL    |NULL     |2          |Serializable  |false        |{numRestoredFiles -> 0, removedFilesSize -> 1368, numRemovedFiles -> 2, restoredFilesSize -> 0, numOfFilesAfterRestore -> 7, tableSizeAfterRestore -> 4788}|NULL        |Apache-Spark/3.5.1 Delta-Lake/3.2.0|
|2      |2024-10-09 04:07:04.819|NULL  |NULL    |WRITE                            |{mode -> Append, partitionBy -> []}                                                           |NULL|NULL    |NULL     |1          |Serializable  |true         |{numFiles -> 2, numOutputRows -> 2, numOutputBytes -> 1368}                                                                                                |NULL        |Apache-Spark/3.5.1 Delta-Lake/3.2.0|
|1      |2024-10-09 04:07:03.763|NULL  |NULL    |WRITE                            |{mode -> Append, partitionBy -> []}                                                           |NULL|NULL    |NULL     |0          |Serializable  |true         |{numFiles -> 4, numOutputRows -> 4, numOutputBytes -> 2736}                                                                                                |NULL        |Apache-Spark/3.5.1 Delta-Lake/3.2.0|
|0      |2024-10-09 04:07:00.877|NULL  |NULL    |CREATE OR REPLACE TABLE AS SELECT|{partitionBy -> [], clusterBy -> [], description -> NULL, isManaged -> true, properties -> {}}|NULL|NULL    |NULL     |NULL       |Serializable  |false        |{numFiles -> 3, numOutputRows -> 3, numOutputBytes -> 2052}                                                                                                |NULL        |Apache-Spark/3.5.1 Delta-Lake/3.2.0|
+-------+-----------------------+------+--------+---------------------------------+----------------------------------------------------------------------------------------------+----+--------+---------+-----------+--------------+-------------+-----------------------------------------------------------------------------------------------------------------------------------------------------------+------------+-----------------------------------+
```
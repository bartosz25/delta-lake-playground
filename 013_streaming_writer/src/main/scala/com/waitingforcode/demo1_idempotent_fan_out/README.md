# Idempotent writer

1. To understand the idempotency, let's begin with the [IdempotentForeachBatchWriter.scala](IdempotentForeachBatchWriter.scala)
* this is the `foreachBatch` sink implementation that depending on the idempotency flag, will either set the _txnVersion_
  and _txnAppId_ or skip them; also, it will fail after writing the first table if the `shouldFail` is set to true
2. Explain the [NotIdempotentFanOutProducerFailedRun.scala](NotIdempotentFanOutProducerFailedRun.scala)
* it simply consumes our rate data source and writes the output to the two tables from the foreachBatch sink
* it's configured not to use the idempotency and to fail
3. Explain the [NotIdempotentFanOutProducerSucceededRun.scala](NotIdempotentFanOutProducerSucceededRun.scala)
* as before but it's configured to succeed
4. Run `NotIdempotentFanOutProducerFailedRun`. It should fail with:
```
Exception in thread "main" org.apache.spark.sql.streaming.StreamingQueryException: An error occurred before writing table 2
=== Streaming Query ===
Identifier: [id = f70de4ac-3d32-404e-9e4c-44bd29b602ea, runId = fc10a28d-bb8d-4d99-a0db-ebd8c6c203cb]
Current Committed Offsets: {}
Current Available Offsets: {RatePerMicroBatchStream[rowsPerBatch=500, numPartitions=10, startTimestamp=1653474300000, advanceMsPerBatch=1000]: {"offset":500,"timestamp":1653474301000}}

Current State: ACTIVE
```
5. Run `NotIdempotentFanOutProducerSucceededRun`. It should succeed.
6. Run `FanOutChecker`. It counts the duplicated rows in the output tables. You should see:
```
+-------------------+-----+-----+
|timestamp          |value|count|
+-------------------+-----+-----+
|2022-05-25 12:25:00|0    |2    |
|2022-05-25 12:25:00|1    |2    |
|2022-05-25 12:25:00|2    |2    |
|2022-05-25 12:25:00|3    |2    |
|2022-05-25 12:25:00|4    |2    |
|2022-05-25 12:25:00|5    |2    |
|2022-05-25 12:25:00|6    |2    |
|2022-05-25 12:25:00|7    |2    |
|2022-05-25 12:25:00|8    |2    |
|2022-05-25 12:25:00|9    |2    |
+-------------------+-----+-----+
only showing top 10 rows
```
5. Let's move now to the idempotent writer. Run the `IdempotentFanOutProducerFailedRun` first. It should fail with:
```
Exception in thread "main" org.apache.spark.sql.streaming.StreamingQueryException: An error occurred before writing table 2
=== Streaming Query ===
Identifier: [id = 58b5795c-e583-4410-aae3-cdad99941aa5, runId = e9ef1b88-b250-44f5-9033-008c1bf7aedd]
Current Committed Offsets: {}
Current Available Offsets: {RatePerMicroBatchStream[rowsPerBatch=500, numPartitions=10, startTimestamp=1653474300000, advanceMsPerBatch=1000]: {"offset":500,"timestamp":1653474301000}}
```

6. Despite the error, the first table's write succeeded. Run the `IdempotentFanOutProducerSucceededRun`.
7. Check the outcome with the `FanOutChecker`. Both tables should be empty.
```
+---------+-----+-----+
|timestamp|value|count|
+---------+-----+-----+
+---------+-----+-----+

All records in the /tmp/delta-lake-playground/013_streaming_writer/demo1/table1=2000

+---------+-----+-----+
|timestamp|value|count|
+---------+-----+-----+
+---------+-----+-----+
```

The idempotent writer adds a special metadata in the _txn_ attribute for each commit, as below:
```
$ cat /tmp/delta-lake-playground/013_streaming_writer/demo1/table1/_delta_log/00000000000000000001.json 
{"commitInfo":{"timestamp":1710242682796,"operation":"WRITE","operationParameters":{"mode":"Append","partitionBy":"[]"},"readVersion":0,"isolationLevel":"Serializable","isBlindAppend":true,"operationMetrics":{"numFiles":"10","numOutputRows":"500","numOutputBytes":"9898"},"engineInfo":"Apache-Spark/3.5.0 Delta-Lake/3.1.0","txnId":"b8dccc86-3a0f-4d87-8b99-462915ec6f10"}}
{"txn":{"appId":"wfc-v1","version":1,"lastUpdated":1710242682791}}
```
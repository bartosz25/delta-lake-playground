# Streaming writer and schema overwriting

1. Explain [StreamingWriterSchemaV1.scala](StreamingWriterSchemaV1.scala)
* it's the simplest streaming writer possible that streams the rate input source and writes all column to table
2. Run the `StreamingWriterSchemaV1`.
3. Run the `TableReader`. It reads the created table. You should see a dataset similar to this one (columns matter):
```
+-------------------+-----+
|timestamp          |value|
+-------------------+-----+
|2022-05-25 12:25:00|0    |
|2022-05-25 12:25:00|1    |
|2022-05-25 12:25:00|2    |
|2022-05-25 12:25:00|3    |
|2022-05-25 12:25:00|4    |
+-------------------+-----+
only showing top 5 rows
```

4. Explain [StreamingWriterSchemaV2StreamingSink.scala](StreamingWriterSchemaV2StreamingSink.scala)
* it's also this kind of simple job but which rewrites the schema by replacing the numerical _value_ column by a struct
5. Run the `StreamingWriterSchemaV2StreamingSink`. Despite the `.option("overwriteSchema", true)`, the job should fail
with this error:
```
Exception in thread "main" org.apache.spark.sql.streaming.StreamingQueryException: Failed to merge fields 
'value' and 'value'. Failed to merge incompatible data types LongType and ArrayType(StringType,true)
=== Streaming Query ===
Identifier: [id = 65e259d5-fdcc-407a-8b95-057c0d7b51f8, runId = 4c9d82de-2a66-40fc-b7c3-5c9810f11175]
Current Committed Offsets: {RatePerMicroBatchStream[rowsPerBatch=500, numPartitions=10, startTimestamp=1653474300000, advanceMsPerBatch=1000]: {"offset":4000,"timestamp":1653474308000}}
Current Available Offsets: {RatePerMicroBatchStream[rowsPerBatch=500, numPartitions=10, startTimestamp=1653474300000, advanceMsPerBatch=1000]: {"offset":4500,"timestamp":1653474309000}}
```
 
6. Explain the [StreamingWriterSchemaV2ForeachBatch.scala](StreamingWriterSchemaV2ForeachBatch.scala)
* it does the same thing as the previous job but uses the batch writer

7. Run the `StreamingWriterSchemaV2ForeachBatch`. It fails too, this time with a slightly different error message (but the reason is the same):
```
Exception in thread "main" org.apache.spark.sql.streaming.StreamingQueryException: [DATATYPE_MISMATCH.CAST_WITHOUT_SUGGESTION] Cannot resolve "value" due to data type mismatch: cannot cast "ARRAY<STRING>" to "BIGINT".;
'AppendData RelationV2[timestamp#50, value#51L] spark_catalog.default.demo3_table spark_catalog.default.demo3_table, [overwriteSchema=true], false
+- 'Project [timestamp#0 AS timestamp#52, cast(value#4 as bigint) AS value#53]
   +- LogicalRDD [timestamp#0, value#4], false

```

8. Uncomment the `.mode(SaveMode.Overwrite)` with `.saveAsTable(TableName)`, and comment the `.insertInto(TableName)` in the `StreamingWriterSchemaV2ForeachBatch`. 
Rerun the job. This time it should work.

9. Run the `TableReader`. It reads the updated table. You should see a dataset similar to this one (columns matter):
```
+-------------------+------+
|timestamp          |value |
+-------------------+------+
|2022-05-25 12:25:09|[a, b]|
|2022-05-25 12:25:09|[a, b]|
|2022-05-25 12:25:09|[a, b]|
|2022-05-25 12:25:09|[a, b]|
|2022-05-25 12:25:09|[a, b]|
+-------------------+------+
only showing top 5 rows
```

⚠️ Beware, overwriting the schema with the batch API involves deleting all data present previously in the table!

⚠️ As for the `mergeSchema`, use the overwriting wisely. Think about the impact on your downstream consumers.

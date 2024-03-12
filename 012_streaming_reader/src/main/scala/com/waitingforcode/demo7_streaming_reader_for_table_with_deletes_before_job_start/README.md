# Streaming a Delta table with deletes

1. Explain the [DemoRunner.scala](DemoRunner.scala)
* the job starts by setting up the context, i.e. creating the table with some data and starting the 
  streaming query that only prints the content to the console
* next, the job performs a delete that should fail the streaming query
  * the streaming query starts without any option set
2. Run the `DemoRunner`. Without any options set, the job should pass:
```
Generating input table versions...
Generating data...
...done, starting streaming data
Deleting from the table...
...deleted.
Starting streaming reader
-------------------------------------------
Batch: 0
-------------------------------------------
+------+------+
|number|letter|
+------+------+
|     2|     b|
+------+------+
```

It's not the same behavior as for the update made after the query start. As you can see, streaming
starts here from the most recent _data_ snapshot.

3. Uncomment the `.option("startingVersion", 0)` in `StreamingReader` and run it. This time, you 
should have the same behavior as for the previous demo, i.e. a failure with a data update message:

```
Exception in thread "main" org.apache.spark.sql.streaming.StreamingQueryException: [DELTA_SOURCE_TABLE_IGNORE_CHANGES] 
Detected a data update (for example part-00000-1565b19b-5984-4e4e-b5e7-aa40c67d74b4-c000.snappy.parquet) in the source
table at version 1. This is currently not supported. If you'd like to ignore updates, set the option 'skipChangeCommits' 
to 'true'. If you would like the data update to be reflected, please restart this query with a fresh checkpoint 
directory. The source table can be found at path file:/tmp/delta-lake-playground/012_streaming_reader/warehouse/numbers_with_letters.
```

4. Uncomment the `.option("skipChangeCommits", true)` in `StreamingReader` and run it. This time the
query should return all rows as they were inserted:
```
-------------------------------------------
Batch: 0
-------------------------------------------
+------+------+
|number|letter|
+------+------+
|     1|     a|
|     2|     b|
|     3|     a|
|     4|     a|
+------+------+
```
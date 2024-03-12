# Streaming a Delta table with updates

1. Explain the [DemoRunner.scala](DemoRunner.scala)
* the job starts by setting up the context, i.e. creating the table with some data and starting the 
  streaming query that only prints the content to the console
* next, the job starts the streaming query
* finally, the job performs an update that should fail the streaming query
  * the streaming query starts without any option set
2. Run the `DemoRunner`. Without any options set, the job should fail with:
```
Exception in thread "Thread-33" org.apache.spark.sql.streaming.StreamingQueryException: 
[DELTA_SOURCE_TABLE_IGNORE_CHANGES] Detected a data update (for example part-00000-bd5609ad-66d0-431d-ae7e-af92cfd1da84-c000.snappy.parquet)
in the source table at version 1. This is currently not supported. 
If you'd like to ignore updates, set the option 'skipChangeCommits' to 'true'. 
If you would like the data update to be reflected, please restart this query with a fresh checkpoint directory. 
The source table can be found at path file:/tmp/delta-lake-playground/012_streaming_reader/warehouse/numbers_with_letters.
=== Streaming Query ===
Identifier: [id = 767a3711-f212-4c1b-a240-e2957b6382f0, runId = 53d2b175-da42-4200-a010-c78722d60e38]
Current Committed Offsets: {DeltaSource[file:/tmp/delta-lake-playground/012_streaming_reader/warehouse/numbers_with_letters]: {"sourceVersion":1,"reservoirId":"45842811-fc73-48bf-bb06-b28de3e3a53c","reservoirVersion":1,"index":-1,"isStartingVersion":false}}
Current Available Offsets: {DeltaSource[file:/tmp/delta-lake-playground/012_streaming_reader/warehouse/numbers_with_letters]: {"sourceVersion":1,"reservoirId":"45842811-fc73-48bf-bb06-b28de3e3a53c","reservoirVersion":1,"index":-1,"isStartingVersion":false}}
```

3. Run the `StreamingReader`; it uses a timestamped checkpoint directory meaning that the run will 
use the fresh location, as demanded in the error message:

```
-------------------------------------------
Batch: 0
-------------------------------------------
+------+------+
|number|letter|
+------+------+
|     1|     ?|
|     2|     b|
|     3|     ?|
|     4|     ?|
+------+------+
```

Here you can see that the update is returned but not the original value. It means that Apache Spark, while
creating the metadata `DataFrame`, filtered out all files marked as _deleted_ in the Delta Lake commit log. 

4. Change the `StreamingReader` by adding these options to the source:
```
sparkSession.readStream.format("delta")
  .option("startingVersion", 0)
  .option("skipChangeCommits", true) 
```

It uses the second solution given by the error message, i.e. ignoring commits with in-place changes.

5. Run the `DemoRunner`. This time the output contains only the initial value of the table:

```
Generating input table versions...
Generating data...
...done, starting streaming data
Waiting for the streaming reader to process the first micro-batch...
Starting streaming reader
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

...processed
Updating the table...
-------------------------------------------
Batch: 1
-------------------------------------------
+------+------+
|number|letter|
+------+------+
+------+------+

...updated.
```
6. This time, change the checkpoint location and the source options to respectively:

```
private val DemoDir = s"${OutputDir}/demo6-static"
```

and

```
.option("skipChangeCommits", false)
```

7. Run the `DemoRunner`. This time the job also failed with the same message as in the beginning: 

```
Exception in thread "Thread-33" org.apache.spark.sql.streaming.StreamingQueryException: 
[DELTA_SOURCE_TABLE_IGNORE_CHANGES] Detected a data update (for example part-00000-d68d85ec-d28d-4c63-80c0-819cfccc7869-c000.snappy.parquet) in the source table at version 1. This is currently not supported. 
If you'd like to ignore updates, set the option 'skipChangeCommits' to 'true'. If you would like the data update to be reflected, 
please restart this query with a fresh checkpoint directory. The source table can be found at path 
file:/tmp/delta-lake-playground/012_streaming_reader/warehouse/numbers_with_letters.
```

8. Just for the record, restart the `StreamingReader`. It still fails with the same message as previously
meaning that there is no other solution that the 2 already covered (skipping updates or using a fresh checkpoint location)
to handle in-place changes.
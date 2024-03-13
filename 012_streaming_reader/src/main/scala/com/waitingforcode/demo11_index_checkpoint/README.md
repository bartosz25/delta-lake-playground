# Checkpoint content

1. Explain the [DemoRunner.scala](DemoRunner.scala)
* the job simply processes a Delta table; the logic is not important here
* more important is the checkpoint location
  * as you can see, the job generates 12 rows in each commit and writes one row per data file
  * the reader accepts 3 files per trigger
2. Run the `DemoRunner`. The job should complete.
3. Check the checkpoint location:

```
$ cat  offsets/6
v1
{"batchWatermarkMs":0,"batchTimestampMs":1710305994254,"conf":{"spark.sql.streaming.stateStore.providerClass":"org.apache.spark.sql.execution.streaming.state.HDFSBackedStateStoreProvider","spark.sql.streaming.stateStore.rocksdb.formatVersion":"5","spark.sql.streaming.statefulOperator.useStrictDistribution":"true","spark.sql.streaming.flatMapGroupsWithState.stateFormatVersion":"2","spark.sql.streaming.multipleWatermarkPolicy":"min","spark.sql.streaming.aggregation.stateFormatVersion":"2","spark.sql.shuffle.partitions":"200","spark.sql.streaming.join.stateFormatVersion":"2","spark.sql.streaming.stateStore.compression.codec":"lz4"}}
{"sourceVersion":1,"reservoirId":"3bffe7c2-e334-4419-a150-f6f47b565c78",
"reservoirVersion":1,"index":8,"isStartingVersion":false}
```

💡 `isStartingVersion` is set to `false` because we are using the `.option("startingVersion", 0)` in the reader. If you remove 
    this part, you'll see the attribute changing to `true`.

💡 `isStartingVersion` doesn't refer to the `startingVersion` attribute, though! In the code base it's 
   called `isInitialSnapshot` which represents the real meaning of the field. The version-name is a legacy one.
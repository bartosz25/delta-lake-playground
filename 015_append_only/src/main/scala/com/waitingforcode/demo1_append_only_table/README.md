# Append-only table 
1. Explain the [DemoRunner.scala](DemoRunner.scala)
* the job starts by setting up the context, i.e. creating the table with some data
* next, the job starts the streaming query
* finally, the job performs an update that should fail
2. Run the `DemoRunner`. Without any options set, the job should fail with:
```
Exception in thread "main" org.apache.spark.sql.delta.DeltaUnsupportedOperationException: [DELTA_CANNOT_MODIFY_APPEND_ONLY]
 This table is configured to only allow appends. If you would like to permit updates or deletes, 
 use 'ALTER TABLE null SET TBLPROPERTIES (delta.appendOnly=false)'.
```

As you can see, the error doesn't come from the streaming job but from the main writer. In a way
it can be a protection against in-place changes that might fail the streaming consumers. On another hand,
it involves a different practice for the updates and deletes that should become inserts. As a writer,
you can rely here on:

* tombstone pattern to mark a row as deleted, i.e. with a flag column
* updating rows via inserts

As a result, your table will become a changelog which will be good for streaming consumers but might
require more effort on batch consumers to build the most recent view.
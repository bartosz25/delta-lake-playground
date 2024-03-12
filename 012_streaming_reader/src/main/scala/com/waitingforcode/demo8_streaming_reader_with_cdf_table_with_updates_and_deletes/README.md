# Streaming a Delta table with Change Data Feed (CDF)

1. Explain the [DemoRunner.scala](DemoRunner.scala)
* the job starts by setting up the context, i.e. creating the table with some data and deleting 
  one row
* next, the job starts a streaming query with the CDF configuration enabled
* finally, it performs an update
2. Run the `DemoRunner`. Without any options set, the job should print the CDF records:

```
Starting streaming reader
-------------------------------------------
Batch: 0
-------------------------------------------
+------+------+------------+---------------+--------------------+
|number|letter|_change_type|_commit_version|   _commit_timestamp|
+------+------+------------+---------------+--------------------+
|  1000|     A|      delete|              1|2024-03-12 03:45:...|
|     1|     a|      insert|              0|2024-03-12 03:45:...|
|     2|     b|      insert|              0|2024-03-12 03:45:...|
|     3|     a|      insert|              0|2024-03-12 03:45:...|
|     4|     a|      insert|              0|2024-03-12 03:45:...|
|  1000|     A|      insert|              0|2024-03-12 03:45:...|
+------+------+------------+---------------+--------------------+

...processed
Updating the table...
-------------------------------------------
Batch: 1
-------------------------------------------
+------+------+----------------+---------------+--------------------+
|number|letter|    _change_type|_commit_version|   _commit_timestamp|
+------+------+----------------+---------------+--------------------+
|     1|     a| update_preimage|              2|2024-03-12 03:46:...|
|     1|     ?|update_postimage|              2|2024-03-12 03:46:...|
|     3|     a| update_preimage|              2|2024-03-12 03:46:...|
|     3|     ?|update_postimage|              2|2024-03-12 03:46:...|
|     4|     a| update_preimage|              2|2024-03-12 03:46:...|
|     4|     ?|update_postimage|              2|2024-03-12 03:46:...|
+------+------+----------------+---------------+--------------------+

...updated.
```

3. Enable the `.option("skipChangeCommits", true)` in `StreamingReader` and run it. You should see
exactly the same output as before. The option doesn't apply when the CDF is enabled.
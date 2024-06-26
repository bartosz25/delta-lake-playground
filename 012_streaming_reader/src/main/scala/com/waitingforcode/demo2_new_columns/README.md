# Streaming a Delta table with new column
1. Explain the [DemoRunner.scala](DemoRunner.scala)
* it's a job simulating an _additive_ schema change, i.e. when the streamed table gets a new column
* the first part of the job creates our table composed of two columns, number and letter
* just after, the job starts the streaming reader that simply prints the output to the console
* finally, the job modifies the created table by adding a new column to it
2. Run the `DemoRunner`. It should fail with:
```
Exception in thread "Thread-31" org.apache.spark.sql.streaming.StreamingQueryException: 
[DELTA_SCHEMA_CHANGED_WITH_VERSION] Detected schema change in version 5:
streaming source schema: root
-- number: integer (nullable = true)
-- letter: string (nullable = true)


data file schema: root
-- number: integer (nullable = true)
-- letter: string (nullable = true)
-- upper_letter: string (nullable = true)


Please try restarting the query. If this issue repeats across query restarts without
making progress, you have made an incompatible schema change and need to start your
query from scratch using a new checkpoint directory.
```

3. Since our schema evolution is an _additive_ one, i.e. we just need to ensure that our downstream
consumers can safely read the data generated by the job with the new column. 
4. Restart the `StreamingRunner`. You should see the table with new column correctly processed:
```
-------------------------------------------
Batch: 1
-------------------------------------------
+------+------+------------+
|number|letter|upper_letter|
+------+------+------------+
|     1|     a|           A|
|     2|     b|           B|
|     3|     c|           C|
|     4|     d|           D|
+------+------+------------+
```
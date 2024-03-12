# Streaming writer with schema changes

1. Explain [StreamingWriterSchemaV1.scala](StreamingWriterSchemaV1.scala)
* it's the simplest streaming writer possible that streams the rate input source and writes all column to table
2. Run the `StreamingWriterSchemaV1`.
3. Run the `TableReader`. It reads the created table. You should see a dataset similar to this one (columns matter):

```
+-------------------+-----+
|timestamp          |value|
+-------------------+-----+
|2022-05-25 12:25:07|3500 |
|2022-05-25 12:25:07|3510 |
|2022-05-25 12:25:07|3520 |
|2022-05-25 12:25:07|3530 |
|2022-05-25 12:25:07|3540 |
+-------------------+-----+
only showing top 5 rows
```

4. Explain [StreamingWriterSchemaV2.scala](StreamingWriterSchemaV2.scala)
* the code does the same thing as before; the single difference is an extra column, hence a schema evolution
5. Run the `StreamingWriterSchemaV2`. It should fail with:
```
Exception in thread "main" org.apache.spark.sql.streaming.StreamingQueryException: A schema mismatch detected when writing to the Delta table (Table ID: 75c73665-cb28-4f70-b6c1-7b3273878c08).
To enable schema migration using DataFrameWriter or DataStreamWriter, please set:
'.option("mergeSchema", "true")'.
For other operations, set the session configuration
spark.databricks.delta.schema.autoMerge.enabled to "true". See the documentation
specific to the operation for details.

Table schema:
root
-- timestamp: timestamp (nullable = true)
-- value: long (nullable = true)


Data schema:
root
-- timestamp: timestamp (nullable = true)
-- value: long (nullable = true)
-- a: string (nullable = true)
```

6. Uncomment the `.option("mergeSchema", true)` in `StreamingWriterSchemaV2` and run it again. This time it
should pass.
7. Run the `TableReader`. It reads the created table. You should see a dataset similar to this one (columns matter):

```
+-------------------+-----+----+
|timestamp          |value|a   |
+-------------------+-----+----+
|2022-05-25 12:25:07|3500 |NULL|
|2022-05-25 12:25:07|3510 |NULL|
|2022-05-25 12:25:07|3520 |NULL|
|2022-05-25 12:25:07|3530 |NULL|
|2022-05-25 12:25:07|3540 |NULL|
+-------------------+-----+----+
only showing top 5 rows
```

⚠️ Although it enables you as a writer, do think about your consumers who will see their job failing because of the
schema changes.
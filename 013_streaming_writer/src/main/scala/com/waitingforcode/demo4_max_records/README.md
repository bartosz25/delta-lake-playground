# Writer with max records per file
1. Explain the [StreamingWriter.scala](StreamingWriter.scala)
* as in previous examples, it streams the rate data source
* the sink uses a new option which is the `.option("maxRecordsPerFile", 1)`
  * the option asks the writer to put one record in each data file; of course 1 is a highly inefficient number but
    it's the easiest one to use in the demo
2. Run the `StreamingWriter`
3. Run the `TableReader`. It should print the created files from the history table. You should see consistent size 
and most of the stats shared:
```
+---------------------------------------------------------------+---------------+----+----------------+----------+--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+----+--------------+---------+-----------------------+------------------+
|path                                                           |partitionValues|size|modificationTime|dataChange|stats                                                                                                                                                                                             |tags|deletionVector|baseRowId|defaultRowCommitVersion|clusteringProvider|
+---------------------------------------------------------------+---------------+----+----------------+----------+--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+----+--------------+---------+-----------------------+------------------+
|part-00000-0eeadb65-281b-4f01-ad51-ef4fe19ec6e5-c018.gz.parquet|{}             |826 |1710248365672   |false     |{"numRecords":1,"minValues":{"timestamp":"2022-05-25T12:25:00.000+02:00","value":180},"maxValues":{"timestamp":"2022-05-25T12:25:00.000+02:00","value":180},"nullCount":{"timestamp":0,"value":0}}|NULL|NULL          |NULL     |NULL                   |NULL              |
|part-00000-181f6777-7953-4f6d-957f-13b5f0335f29-c034.gz.parquet|{}             |827 |1710248365972   |false     |{"numRecords":1,"minValues":{"timestamp":"2022-05-25T12:25:00.000+02:00","value":340},"maxValues":{"timestamp":"2022-05-25T12:25:00.000+02:00","value":340},"nullCount":{"timestamp":0,"value":0}}|NULL|NULL          |NULL     |NULL                   |NULL              |
|part-00000-1fc10ae2-2a96-4ae7-8d60-7694e0b2ac69-c006.gz.parquet|{}             |826 |1710248365480   |false     |{"numRecords":1,"minValues":{"timestamp":"2022-05-25T12:25:00.000+02:00","value":60},"maxValues":{"timestamp":"2022-05-25T12:25:00.000+02:00","value":60},"nullCount":{"timestamp":0,"value":0}}  |NULL|NULL          |NULL     |NULL                   |NULL              |
|part-00000-2134fe06-36c5-4dae-9e9d-3be7f9ea4461-c039.gz.parquet|{}             |827 |1710248366044   |false     |{"numRecords":1,"minValues":{"timestamp":"2022-05-25T12:25:00.000+02:00","value":390},"maxValues":{"timestamp":"2022-05-25T12:25:00.000+02:00","value":390},"nullCount":{"timestamp":0,"value":0}}|NULL|NULL          |NULL     |NULL                   |NULL              |
|part-00000-24a41a83-f655-4bcd-a8df-2198d139b599-c036.gz.parquet|{}             |826 |1710248365996   |false     |{"numRecords":1,"minValues":{"timestamp":"2022-05-25T12:25:00.000+02:00","value":360},"maxValues":{"timestamp":"2022-05-25T12:25:00.000+02:00","value":360},"nullCount":{"timestamp":0,"value":0}}|NULL|NULL          |NULL     |NULL                   |NULL              |
+---------------------------------------------------------------+---------------+----+----------------+----------+--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+----+--------------+---------+-----------------------+------------------+
only showing top 5 rows

All files=500
```
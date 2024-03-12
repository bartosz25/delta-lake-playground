# Orphan files demo
1. Explain the [StreamingWriter.scala](StreamingWriter.scala)
* it's a streaming job that writes rate stream input rows to a Delta table
* besides writing, the job also has a thread that deletes any data file as soon as it's created
  * the goal here is to simulate an error on the storage and see whether Delta writer removes orphan files
2. Run the `StreamingWriter`. It should fail due to removed data file by the watcher:
```
Exception in thread "main" org.apache.spark.sql.streaming.StreamingQueryException: Job aborted due to stage failure: Task 7 in stage 4.0 failed 1 times, most recent failure: Lost task 7.0 in stage 4.0 (TID 60) (192.168.1.55 executor driver): 
ExitCodeException exitCode=1: chmod: cannot access 
'/tmp/delta-lake-playground/013_streaming_writer/warehouse/demo5_table/part-00007-84cabc83-a499-452b-a090-ba728bddfdb3-c000.gz.parquet': 
No such file or directory
```

Besides, you should see that some of the files created by the job:
```
$ ls /tmp/delta-lake-playground/013_streaming_writer/warehouse/demo5_table/ 
_delta_log                                                       part-00002-71bf1aea-af4c-47be-8f51-d7129937ca71-c004.gz.parquet  part-00005-38c4c880-2799-4d28-8ccc-c0913c10797e-c001.gz.parquet
part-00000-17089a76-005a-4ee8-8bf4-486e0ff4e8bd-c004.gz.parquet  part-00002-72deaf63-3d2c-43e3-8cea-ef404a888f3b-c003.gz.parquet  part-00005-78740802-8356-4662-98f0-c10ee37d6a18-c002.gz.parquet
part-00000-2005f596-4e25-4ebb-bb45-a43df3161f32-c000.gz.parquet  part-00002-90e46673-9363-48d0-ae05-cb4a544e981f-c000.gz.parquet  part-00005-d16c75af-c0d0-4780-a295-b92c96ebfd93-c004.gz.parquet
part-00000-a1208617-d10f-4c12-949d-24899eceec56-c003.gz.parquet  part-00003-0b500e82-978c-49b4-aded-f0768d2e9996-c002.gz.parquet  part-00005-f1526505-ad0f-4617-af59-a9821bdbb941-c000.gz.parquet
part-00000-a374a032-8b8e-4b6f-8cf1-8d53210b2434-c001.gz.parquet  part-00003-1e08e75c-26f3-40d2-9aab-240ce7af0a65-c003.gz.parquet  part-00005-f2b94e7a-51ed-487a-b414-ef07028978e3-c001.gz.parquet
part-00000-d1f929b4-ab99-494d-9592-f8e06e7d3165-c001.gz.parquet  part-00003-3eba7491-691a-4977-9e1e-8677f30f301f-c000.gz.parquet  part-00006-066333d4-2cf0-46a1-aa22-ab30885485ed-c001.gz.parquet
part-00000-d505305b-fc67-4339-832a-03972ac7fc30-c000.gz.parquet  part-00003-68657d2c-1e1b-460a-a85e-0e548912aa5f-c001.gz.parquet  part-00006-06e4e518-d0ba-4a36-b872-977526d56b12-c000.gz.parquet
part-00000-e099285e-4585-4623-8c6c-363723898129-c002.gz.parquet  part-00003-7a497a5c-fc07-4098-bba6-e01156b9b0c2-c000.gz.parquet  part-00006-4b835af0-6449-4dad-b484-238760f58819-c004.gz.parquet
part-00001-286937f3-7732-42d7-a1d9-fecdd2166487-c001.gz.parquet  part-00003-9e885fc4-d781-47ce-88b4-78b0642d8ab8-c004.gz.parquet  part-00006-637b2df8-423f-419f-b4f8-692d01c74b1c-c002.gz.parquet
```

And none of them belongs to the single committed version for the `CREATE TABLE` operation:

```
$ ls /tmp/delta-lake-playground/013_streaming_writer/warehouse/demo5_table/_delta_log/
00000000000000000000.json

$ cat /tmp/delta-lake-playground/013_streaming_writer/warehouse/demo5_table/_delta_log/00000000000000000000.json 
{"commitInfo":{"timestamp":1710249877043,"operation":"CREATE TABLE","operationParameters":{"isManaged":"true","description":null,"partitionBy":"[]","properties":"{}"},"isolationLevel":"Serializable","isBlindAppend":true,"operationMetrics":{},"engineInfo":"Apache-Spark/3.5.0 Delta-Lake/3.1.0","txnId":"d77818ae-c11e-4d64-87e3-6e6dfca1741c"}}
{"metaData":{"id":"e8858222-8a60-4c7b-b941-a2cba913cd34","format":{"provider":"parquet","options":{}},"schemaString":"{\"type\":\"struct\",\"fields\":[{\"name\":\"a\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}},{\"name\":\"b\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}},{\"name\":\"c\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}}]}","partitionColumns":[],"configuration":{},"createdTime":1710249876832}}
{"protocol":{"minReaderVersion":1,"minWriterVersion":2}}
```
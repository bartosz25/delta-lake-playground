# Excluding some files

1. Explain the [DemoRunner.scala](DemoRunner.scala)
* the job starts by setting up the context, i.e. creating the table with some data
* next, the job starts the streaming query
2. Run the `DemoRunner`. The job should count only 9 rows:
```
Generating data....
...data generated, starting streaming reader
All rows in the micro-batch 9
```

It's because of the `.option("excludeRegex", "part-00000-") ` that ignores the records written 
by the first of 4 tasks.
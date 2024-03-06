package com.waitingforcode.demo1_schema_tracking_example

import com.waitingforcode.getOrCreateDeltaLakeSparkSession

object DemoRunner {

  def main(args: Array[String]): Unit = {
    val sparkSession = getOrCreateDeltaLakeSparkSession(extraConfig = Map(
      "spark.databricks.delta.streaming.allowSourceColumnRenameAndDrop" -> "always"
    ))
    println("Creating table...")
    CreateTableWithColumnMapping.run(sparkSession)
    println("...table created")
    new Thread(() => {
      println("Starting streaming reader")
      StreamDeltaTable.main(args)
    }).start()

    while(!StreamDeltaTable.StreamStarted.get()) {}

    println("Stream has started. We can now do the schema evolution.")
    println("Renaming an existing column...")
    RenameColumn.run(sparkSession)
    println("...column renamed")
  }
}

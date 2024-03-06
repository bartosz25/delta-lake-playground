package com.waitingforcode.demo3_schema_evolution_step_by_step_no_tracking

import com.waitingforcode.getOrCreateDeltaLakeSparkSession

object DemoRunner {

  def main(args: Array[String]): Unit = {
    val sparkSession = getOrCreateDeltaLakeSparkSession()
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

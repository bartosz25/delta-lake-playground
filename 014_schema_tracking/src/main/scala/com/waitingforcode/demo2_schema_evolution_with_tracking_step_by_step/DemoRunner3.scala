package com.waitingforcode.demo2_schema_evolution_with_tracking_step_by_step

import com.waitingforcode.getOrCreateDeltaLakeSparkSession

object DemoRunner3 {

  def main(args: Array[String]): Unit = {
    val sparkSession = getOrCreateDeltaLakeSparkSession(extraConfig =
      Map("spark.databricks.delta.streaming.allowSourceColumnRenameAndDrop" -> "always"))
    new Thread(() => {
      println("Starting streaming reader")
      StreamDeltaTable.main(args)
    }).start()

    while(!StreamDeltaTable.StreamStarted.get()) {}
    println("Stream has started. Modifying the table again.")
    println("Renaming an existing column...")
    RenameColumn3.run(sparkSession)
    println("...column renamed")
  }
}

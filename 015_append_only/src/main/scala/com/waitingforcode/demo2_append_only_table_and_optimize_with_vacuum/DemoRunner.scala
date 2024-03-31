package com.waitingforcode.demo2_append_only_table_and_optimize_with_vacuum

import com.waitingforcode.{DataWarehouseBaseDir, NumbersWithLettersTable, OutputDir, getOrCreateSparkSessionWithDeltaLake}
import org.apache.commons.io.FileUtils
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.delta.DeltaLog
import org.apache.spark.sql.delta.commands.VacuumCommand

import java.io.File

object DemoRunner {

  def main(args: Array[String]): Unit = {
    val sparkSession = getOrCreateSparkSessionWithDeltaLake(extraConfig = Map(
      "spark.databricks.delta.properties.defaults.appendOnly" -> "true",
      "spark.databricks.delta.retentionDurationCheck.enabled" -> "false" // Disable to avoid the 168 min retention error
    ))
    import sparkSession.implicits._
    FileUtils.deleteDirectory(new File(OutputDir))
    println("Generating input table versions...")
    val numbersWithLetters = Seq(
      (1, "a"), (2, "b"), (3, "a"), (4, "a")
    ).toDF("number", "letter")

    println("Generating data...")
    numbersWithLetters.write.option("maxRecordsPerFile", 1).format("delta").mode(SaveMode.Overwrite)
      .saveAsTable(NumbersWithLettersTable)
    println("...done, starting streaming data")
    new Thread(() => {
      println("Starting streaming reader")
      StreamingReader.main(args)
    }).start()
    println("Waiting for the streaming reader to process the first micro-batch...")
    while(!StreamingReader.StreamStarted.get()) {}
    println("...processed")

    println("Optimizing the table...")
    val tablePath = s"${DataWarehouseBaseDir}/${NumbersWithLettersTable}"
    sparkSession.sql(s"OPTIMIZE '${tablePath}'")
    println("...optimized. Running VACUUM now...")
    VacuumCommand.gc(sparkSession, DeltaLog.forTable(sparkSession, tablePath), retentionHours = Some(0.01d), dryRun = false)
    println("...VACUUMED")
  }

}

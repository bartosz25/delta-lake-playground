package com.waitingforcode.demo1_restore_table

import com.waitingforcode.{DataWarehouseBaseDir, NumbersWithLettersTable, OutputDir, getOrCreateSparkSessionWithDeltaLake}
import org.apache.commons.io.FileUtils
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.delta.DeltaLog
import org.apache.spark.sql.delta.commands.VacuumCommand

import java.io.File

object SetupJob {

  def main(args: Array[String]): Unit = {
    val sparkSession = getOrCreateSparkSessionWithDeltaLake()
    import sparkSession.implicits._
    FileUtils.deleteDirectory(new File(OutputDir))

    println("Generating input table versions...")
    Seq((1, "a"), (2, "b"), (3, "c")).toDF("number", "letter").write.format("delta").mode(SaveMode.Overwrite)
      .saveAsTable(NumbersWithLettersTable)
    println("...done, inserting new data in 2 batches...")

    Seq((4, "d"), (5, "e"), (6, "f"), (7, "g")).toDF("number", "letter").write.insertInto(NumbersWithLettersTable)
    Seq((8, "h"), (9, "i")).toDF("number", "letter").write.insertInto(NumbersWithLettersTable)

    println("...done")
  }

}

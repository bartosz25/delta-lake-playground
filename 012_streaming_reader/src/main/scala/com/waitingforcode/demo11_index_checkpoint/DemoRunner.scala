package com.waitingforcode.demo11_index_checkpoint

import com.waitingforcode.{NumbersWithLettersTable, OutputDir, getOrCreateSparkSessionWithDeltaLake}
import org.apache.commons.io.FileUtils
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.{DataFrame, SaveMode}

import java.io.File

object DemoRunner {

  private val CheckpointLocation = s"${OutputDir}/demo11/checkpoint"

  def main(args: Array[String]): Unit = {
    val sparkSession = getOrCreateSparkSessionWithDeltaLake()
    FileUtils.deleteDirectory(new File(OutputDir))
    import sparkSession.implicits._
    def getNumbers(prefix: Int): DataFrame = {
      val numbersWithLetters = Seq(
        (prefix*1, "a"), (prefix*2, "b"), (prefix*3, "c"), (prefix*4, "d"),
        (prefix*5, "aa"), (prefix*6, "bb"), (prefix*7, "cc"), (prefix*8, "dd"),
        (prefix*9, "aaa"), (prefix*10, "bbb"), (prefix*11, "ccc"), (prefix*12, "ddd")
      ).toDF("number", "letter")
      numbersWithLetters
    }

    println("Generating data....")
    getNumbers(1).write.format("delta").option("maxRecordsPerFile", 1).mode(SaveMode.Overwrite).saveAsTable(NumbersWithLettersTable)
    getNumbers(2).write.format("delta").option("maxRecordsPerFile", 1).insertInto(NumbersWithLettersTable)
    getNumbers(3).write.format("delta").option("maxRecordsPerFile", 1).insertInto(NumbersWithLettersTable)
    println("...data generated, starting streaming reader")

    sparkSession.readStream.format("delta")
      .option("startingVersion", 0)
      .option("maxFilesPerTrigger", 3)
      .table(NumbersWithLettersTable)
      .writeStream.trigger(Trigger.AvailableNow())
      .option("checkpointLocation", CheckpointLocation)
      .foreachBatch((dataFrame: DataFrame, version: Long) => {
        println(s"All rows in the micro-batch ${dataFrame.count()}")
        ()
      }).start()

    sparkSession.streams.awaitAnyTermination()
  }

}

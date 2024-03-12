package com.waitingforcode.demo10_streaming_reader_and_excluded_regex_option

import com.waitingforcode.{NumbersWithLettersTable, OutputDir, getOrCreateSparkSessionWithDeltaLake}
import org.apache.commons.io.FileUtils
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.{DataFrame, SaveMode}

import java.io.File

object DemoRunner {

  def main(args: Array[String]): Unit = {
    val sparkSession = getOrCreateSparkSessionWithDeltaLake()
    FileUtils.deleteDirectory(new File(OutputDir))
    import sparkSession.implicits._
    def getNumbers(prefix: Int, indexColumn: String = "number"): DataFrame = {
      val numbersWithLetters = Seq(
        (prefix*1, "a"), (prefix*2, "b"), (prefix*3, "c"), (prefix*4, "d")
      ).toDF(indexColumn, "letter")
      numbersWithLetters
    }

    println("Generating data....")
    getNumbers(1).repartition(4).write.format("delta").option("maxRecordsPerFile", 1).mode(SaveMode.Overwrite).saveAsTable(NumbersWithLettersTable)
    getNumbers(2).repartition(4).write.format("delta").option("maxRecordsPerFile", 1).insertInto(NumbersWithLettersTable)
    getNumbers(3).repartition(4).write.format("delta").option("maxRecordsPerFile", 1).insertInto(NumbersWithLettersTable)
    println("...data generated, starting streaming reader")

    sparkSession.readStream.format("delta")
      .option("startingVersion", 0)
      .option("excludeRegex", "part-00000-") // Exclude all files for the first partition
      .table(NumbersWithLettersTable)
      .writeStream.trigger(Trigger.AvailableNow())
      .foreachBatch((dataFrame: DataFrame, version: Long) => {
        println(s"All rows in the micro-batch ${dataFrame.count()}")
        ()
      }).start()

    sparkSession.streams.awaitAnyTermination()
  }

}

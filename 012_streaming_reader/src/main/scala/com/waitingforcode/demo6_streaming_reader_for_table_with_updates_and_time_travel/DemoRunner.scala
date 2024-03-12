package com.waitingforcode.demo6_streaming_reader_for_table_with_updates_and_time_travel

import com.waitingforcode.{NumbersWithLettersTable, OutputDir, getOrCreateSparkSessionWithDeltaLake}
import org.apache.commons.io.FileUtils
import org.apache.spark.sql.SaveMode

import java.io.File

object DemoRunner {

  def main(args: Array[String]): Unit = {
    val sparkSession = getOrCreateSparkSessionWithDeltaLake()
    FileUtils.deleteDirectory(new File(OutputDir))
    println("Generating input table versions...")
    import sparkSession.implicits._
    val numbersWithLetters = Seq(
      (1, "a"), (2, "b"), (3, "a"), (4, "a")
    ).toDF("number", "letter")

    println("Generating data...")
    numbersWithLetters.write.format("delta").mode(SaveMode.Overwrite).saveAsTable(NumbersWithLettersTable)
    println("...done, starting streaming data")
    new Thread(() => {
      println("Starting streaming reader")
      StreamingReader.main(args)
    }).start()
    println("Waiting for the streaming reader to process the first micro-batch...")
    while(!StreamingReader.StreamStarted.get()) {}
    println("...processed")

    println("Updating the table...")
    sparkSession.sql(
      s"""
         |UPDATE ${NumbersWithLettersTable} SET letter = '?'
         |WHERE letter = 'a'
         |""".stripMargin)
    println("...updated.")
  }

}

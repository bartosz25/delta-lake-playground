package com.waitingforcode.demo8_streaming_reader_with_cdf_table_with_updates_and_deletes

import com.waitingforcode.{NumbersWithLettersTable, OutputDir, getOrCreateSparkSessionWithDeltaLake}
import org.apache.commons.io.FileUtils
import org.apache.spark.sql.SaveMode

import java.io.File

object DemoRunner {

  def main(args: Array[String]): Unit = {
    val sparkSession = getOrCreateSparkSessionWithDeltaLake(extraConfig = Map(
      // Let's set it as default for all created tables
      "spark.databricks.delta.properties.defaults.enableChangeDataFeed" -> "true"
    ))
    FileUtils.deleteDirectory(new File(OutputDir))
    println("Generating input table versions...")
    import sparkSession.implicits._
    val numbersWithLetters = Seq(
      (1, "a"), (2, "b"), (3, "a"), (4, "a"), (1000, "A")
    ).toDF("number", "letter")

    println("Generating data...")
    numbersWithLetters.write.format("delta").mode(SaveMode.Overwrite).saveAsTable(NumbersWithLettersTable)
    println("...done, starting streaming data")

    println("Deleting from the table...")
    sparkSession.sql(
      s"""
         |DELETE FROM ${NumbersWithLettersTable}
         |WHERE letter = 'A'
         |""".stripMargin)
    println("...deleted.")

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

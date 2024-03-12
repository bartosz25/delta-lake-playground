package com.waitingforcode.demo7_streaming_reader_for_table_with_deletes_before_job_start

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

    println("Deleting from the table...")
    sparkSession.sql(
      s"""
         |DELETE FROM ${NumbersWithLettersTable}
         |WHERE letter = 'a'
         |""".stripMargin)
    println("...deleted.")

    println("Starting streaming reader")
    StreamingReader.main(args)

  }

}

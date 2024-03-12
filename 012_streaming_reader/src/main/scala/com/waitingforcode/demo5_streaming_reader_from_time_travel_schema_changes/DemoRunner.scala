package com.waitingforcode.demo5_streaming_reader_from_time_travel_schema_changes

import com.waitingforcode.{NumbersWithLettersTable, OutputDir, getOrCreateSparkSessionWithDeltaLake}
import io.delta.tables.DeltaTable
import org.apache.commons.io.FileUtils
import org.apache.spark.sql.{DataFrame, SaveMode}

import java.io.File

object DemoRunner {

  def main(args: Array[String]): Unit = {
    val sparkSession = getOrCreateSparkSessionWithDeltaLake()
    FileUtils.deleteDirectory(new File(OutputDir))
    println("Generating input table versions...")
    import sparkSession.implicits._
    def getNumbers(prefix: Int): DataFrame = {
      val numbersWithLetters = Seq(
        (prefix*10, s"${prefix}-a"), (prefix*20, s"${prefix}-b")
      ).toDF("number", "letter")
      numbersWithLetters
    }
    println("...version 0")
    getNumbers(0).write.format("delta").mode(SaveMode.Overwrite).saveAsTable(NumbersWithLettersTable)
    println("...version 1 (metadata)")
    sparkSession.sql(
      s"""
         |  ALTER TABLE ${NumbersWithLettersTable} SET TBLPROPERTIES (
         |    'delta.minReaderVersion' = '2',
         |    'delta.minWriterVersion' = '5',
         |    'delta.columnMapping.mode' = 'name'
         |  )
         |""".stripMargin)
    println("...version 2")
    getNumbers(2).write.format("delta").insertInto(NumbersWithLettersTable)
    println("...version 3")
    getNumbers(3).write.format("delta").insertInto(NumbersWithLettersTable)
    println("...version 4 (metadata)")
    sparkSession.sql(s"ALTER TABLE ${NumbersWithLettersTable} RENAME COLUMN number TO id_number")
    println("...version 5")
    getNumbers(5).write.format("delta").insertInto(NumbersWithLettersTable)

    println("...generated all the versions. Checking the table history.")
    DeltaTable.forName(NumbersWithLettersTable).history().show(truncate = false)

    println("Starting the streaming from the version 2")
    sparkSession.readStream.format("delta")
      .option("startingVersion", 2)
      .table(NumbersWithLettersTable)
      .writeStream.format("console").start()

    sparkSession.streams.awaitAnyTermination()
  }

}

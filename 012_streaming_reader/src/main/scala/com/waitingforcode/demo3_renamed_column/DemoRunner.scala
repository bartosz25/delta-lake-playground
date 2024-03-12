package com.waitingforcode.demo3_renamed_column

import com.waitingforcode.{NumbersWithLettersTable, OutputDir, getOrCreateSparkSessionWithDeltaLake}
import org.apache.commons.io.FileUtils
import org.apache.spark.sql.SaveMode

import java.io.File

object DemoRunner {

  def main(args: Array[String]): Unit = {
    FileUtils.deleteDirectory(new File(OutputDir))
    val sparkSession = getOrCreateSparkSessionWithDeltaLake(1)
    import sparkSession.implicits._
    println("Creating table...")
    val numbersWithLetters = Seq(
      (1, "a"), (2, "b"), (3, "c"), (4, "d")
    ).toDF("number", "letter")

    numbersWithLetters.write.format("delta").mode(SaveMode.Overwrite).saveAsTable(NumbersWithLettersTable)
    sparkSession.sql(
      s"""
         |  ALTER TABLE ${NumbersWithLettersTable} SET TBLPROPERTIES (
         |    'delta.minReaderVersion' = '2',
         |    'delta.minWriterVersion' = '5',
         |    'delta.columnMapping.mode' = 'name'
         |  )
         |""".stripMargin)
    println("...table created, starting the streaming reader")

    new Thread(() => {
      println("Starting streaming reader")
      StreamingReader.main(args)
    }).start()
    println("Waiting for the streaming reader to process the first micro-batch...")
    while(!StreamingReader.StreamStarted.get()) {}
    println("...processed")

    println("Adding new column...")
    sparkSession.sql(s"ALTER TABLE ${NumbersWithLettersTable} RENAME COLUMN number TO id_number")
    val numbersWithLettersSchemaV2 = Seq(
      (1, "a"), (2, "b"), (3, "c"), (4, "d")
    ).toDF("id_number", "letter")

    numbersWithLettersSchemaV2.write.format("delta").insertInto(NumbersWithLettersTable)
    println("...new column added")
  }
}

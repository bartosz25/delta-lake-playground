package com.waitingforcode.demo1_schema_tracking_example

import com.waitingforcode.{NumbersWithLettersTable, outputDir}
import org.apache.commons.io.FileUtils
import org.apache.spark.sql.{SaveMode, SparkSession}

import java.io.File

object CreateTableWithColumnMapping {

  def run(sparkSession: SparkSession): Unit = {
    FileUtils.deleteDirectory(new File(outputDir))
    import sparkSession.implicits._
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
  }

}

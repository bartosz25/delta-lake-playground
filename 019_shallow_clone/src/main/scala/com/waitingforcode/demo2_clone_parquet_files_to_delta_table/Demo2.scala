package com.waitingforcode.demo2_clone_parquet_files_to_delta_table

import com.waitingforcode._
import io.delta.tables.DeltaTable
import org.apache.commons.io.FileUtils
import org.apache.spark.sql.SaveMode

import java.io.File

object Demo2 {

  def main(args: Array[String]): Unit = {
    import sparkSession.implicits._
    val parquetFilesDirectory = s"${OutputDir}/demo2"
    Seq(LetterWithNumber("a", 1), LetterWithNumber("b", 2), LetterWithNumber("c", 3)).toDS().repartition(1)
      .write.mode(SaveMode.Overwrite).parquet(parquetFilesDirectory)
    Seq(LetterWithNumber("a", 11), LetterWithNumber("b", 22), LetterWithNumber("c", 33)).toDS().repartition(1)
      .write.mode(SaveMode.Append).parquet(parquetFilesDirectory)

    sparkSession.sql(s"CREATE TABLE demo2_target SHALLOW CLONE parquet.`${parquetFilesDirectory}`")
    println("Target files")
    sparkSession.read.text(s"${DataWarehouseBaseDir}/demo2_target/_delta_log/*.json").show(false)

    println("Target table content")
    sparkSession.read.table("demo2_target").show()

    // Let's now change some data in both source and target tables:
    sparkSession.sql("UPDATE demo2_target SET number = number * 10")
    println("Target files after the update")
    sparkSession.read.text(s"${DataWarehouseBaseDir}/demo2_target/_delta_log/*.json").show(false)

    println("Target table content after the update")
    sparkSession.read.table("demo2_target").show()
  }
}

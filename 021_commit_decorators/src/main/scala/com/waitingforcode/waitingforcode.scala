package com

import io.delta.sql.DeltaSparkSessionExtension
import org.apache.commons.io.FileUtils
import org.apache.spark.sql.SparkSession

import java.io.File

package object waitingforcode {

  val OutputDir = "/tmp/delta-lake-playground/021_commit_decorators"
  val DataWarehouseBaseDir = s"${OutputDir}/warehouse"
  FileUtils.deleteDirectory(new File(OutputDir))
  System.setProperty("derby.system.home", DataWarehouseBaseDir)
  lazy val sparkSession = {
    SparkSession.builder().master(s"local[*]")
      .withExtensions(new DeltaSparkSessionExtension())
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .config("spark.sql.warehouse.dir", DataWarehouseBaseDir)
      //.enableHiveSupport()
      .getOrCreate()
  }

  case class LetterWithNumber(letter: String, number: Int)

  def createTable(tableName: String, partitionClause: String = ""): Unit = {
    sparkSession.sql(s"DROP TABLE IF EXISTS ${tableName}")
    sparkSession.sql(
      s"""CREATE TABLE ${tableName} (
         |  letter STRING, number INT
         |) USING DELTA ${partitionClause}
         |""".stripMargin)
  }

  def addRecords(tableName: String, letters: LetterWithNumber*) = {
    import sparkSession.implicits._
    Seq(letters: _*).toDS().repartition(1).writeTo(tableName).append()
  }


}
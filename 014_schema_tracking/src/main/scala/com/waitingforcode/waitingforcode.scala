package com

import io.delta.sql.DeltaSparkSessionExtension
import org.apache.spark.sql.SparkSession

package object waitingforcode {

  val outputDir = "/tmp/delta-lake-playground/014_schema_tracking/"
  private val dataWarehouseBaseDir = s"${outputDir}/warehouse"
  System.setProperty("derby.system.home", dataWarehouseBaseDir)
  val NumbersWithLettersTable = "numbers_with_letters"
  def getOrCreateDeltaLakeSparkSession(cores: Int = 1, extraConfig: Map[String, Any] = Map.empty): SparkSession = {
    SparkSession.builder()
      .appName("Writing").master(s"local[${cores}]")
      .withExtensions(new DeltaSparkSessionExtension())
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .config("spark.sql.warehouse.dir", dataWarehouseBaseDir)
      .config("spark.databricks.delta.properties.defaults.enableChangeDataFeed", "true")
      .config(extraConfig)
      .enableHiveSupport()
      .getOrCreate()
  }

  case class User(id: Int, login: String, isActive: Boolean)


}
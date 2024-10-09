package com

import io.delta.sql.DeltaSparkSessionExtension
import org.apache.spark.sql.SparkSession

package object waitingforcode {

  val OutputDir = "/tmp/delta-lake-playground/016_restore"
  val DataWarehouseBaseDir = s"${OutputDir}/warehouse"
  System.setProperty("derby.system.home", DataWarehouseBaseDir)
  val NumbersWithLettersTable = "numbers_with_letters"
  def getOrCreateSparkSessionWithDeltaLake(): SparkSession = {
    SparkSession.builder().master(s"local[*]")
      .withExtensions(new DeltaSparkSessionExtension())
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .config("spark.sql.warehouse.dir", DataWarehouseBaseDir)
      .enableHiveSupport()
      .getOrCreate()
  }


}
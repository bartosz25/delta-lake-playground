package com

import io.delta.sql.DeltaSparkSessionExtension
import org.apache.spark.sql.SparkSession

package object waitingforcode {

  val OutputDir = "/tmp/delta-lake-playground/013_streaming_writer"
  val DataWarehouseBaseDir = s"${OutputDir}/warehouse"
  System.setProperty("derby.system.home", DataWarehouseBaseDir)
  def getOrCreateSparkSessionWithDeltaLake(cores: Int = 2, extraConfig: Map[String, Any] = Map.empty): SparkSession = {
    SparkSession.builder()
      .master(s"local[${cores}]")
      .withExtensions(new DeltaSparkSessionExtension())
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .config("spark.sql.warehouse.dir", DataWarehouseBaseDir)
      .config(extraConfig)
      .enableHiveSupport()
      .getOrCreate()
  }

}
package com.waitingforcode

import org.apache.spark.sql.SparkSession

import java.time.{LocalDateTime, ZoneOffset}
import java.util.concurrent.TimeUnit

package object demo3_overwritten_schema {

  val DemoDir = s"${OutputDir}/demo3"
  val CheckpointLocation = s"${DemoDir}/checkpoint"
  val TableName ="demo3_table"

  def getRateStreamSource(sparkSession: SparkSession) = {
    sparkSession.readStream.format("rate-micro-batch")
      .option("rowsPerBatch", 500)
      .option("numPartitions", 10)
      .option("advanceMillisPerMicroBatch", TimeUnit.MINUTES.toMillis(2))
      .option("startTimestamp", LocalDateTime.of(2022, 5, 25, 10, 25).toInstant(ZoneOffset.UTC).toEpochMilli)
      .load()
  }
}

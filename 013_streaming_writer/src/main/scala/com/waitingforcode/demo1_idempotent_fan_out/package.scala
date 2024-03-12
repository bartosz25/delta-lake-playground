package com.waitingforcode

import org.apache.spark.sql.SparkSession

import java.time.{LocalDateTime, ZoneOffset}
import java.util.concurrent.TimeUnit

package object demo1_idempotent_fan_out {

  val AppId = "wfc-v1"
  val DemoDir = s"${OutputDir}/demo1"
  val CheckpointLocation = s"${DemoDir}/checkpoint"
  val Table1Location = s"${DemoDir}/table1"
  val Table2Location = s"${DemoDir}/table2"

  def getRateStreamSource(sparkSession: SparkSession) = {
    sparkSession.readStream.format("rate-micro-batch")
      .option("rowsPerBatch", 500)
      .option("numPartitions", 10)
      .option("advanceMillisPerMicroBatch", TimeUnit.MINUTES.toMillis(2))
      .option("startTimestamp", LocalDateTime.of(2022, 5, 25, 10, 25).toInstant(ZoneOffset.UTC).toEpochMilli)
      .load()
  }

}

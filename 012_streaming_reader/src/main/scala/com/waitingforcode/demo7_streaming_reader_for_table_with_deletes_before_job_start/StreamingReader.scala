package com.waitingforcode.demo7_streaming_reader_for_table_with_deletes_before_job_start

import com.waitingforcode.{DataWarehouseBaseDir, NumbersWithLettersTable, OutputDir, getOrCreateSparkSessionWithDeltaLake}

import java.util.concurrent.atomic.AtomicBoolean

object StreamingReader {

  val StreamStarted = new AtomicBoolean(false)

  private val DemoDir = s"${OutputDir}/demo7-${System.currentTimeMillis()}"
  //private val DemoDir = s"${OutputDir}/demo7-static"

  def main(args: Array[String]): Unit = {
    val sparkSession = getOrCreateSparkSessionWithDeltaLake()

    sparkSession.readStream.format("delta")
      //.option("startingVersion", 0)
      //.option("skipChangeCommits", false)
      .load(s"${DataWarehouseBaseDir}/${NumbersWithLettersTable}")
      .writeStream
      .option("checkpointLocation", s"${DemoDir}/checkpoint")
      .format("console").start()

    sparkSession.streams.awaitAnyTermination()
  }
}
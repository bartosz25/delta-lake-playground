package com.waitingforcode.demo1_api_batch_streaming_difference

import com.waitingforcode.{OutputDir, getOrCreateSparkSessionWithDeltaLake}
import org.apache.commons.io.FileUtils

import java.io.File
import java.time.{LocalDateTime, ZoneOffset}
import java.util.concurrent.TimeUnit

object StreamingVsBatchReader {

  private val DemoDir = s"${OutputDir}/demo1"

  def main(args: Array[String]): Unit = {
    val sparkSession = getOrCreateSparkSessionWithDeltaLake(1)
    FileUtils.deleteDirectory(new File(DemoDir))
    val tableName = "rateData"

    val rateStreamInput = sparkSession.readStream.format("rate-micro-batch")
      .option("rowsPerBatch", 5)
      .option("numPartitions", 2)
      .option("advanceMillisPerMicroBatch", TimeUnit.MINUTES.toMillis(2))
      .option("startTimestamp", LocalDateTime.of(2022, 5, 25, 10, 25).toInstant(ZoneOffset.UTC).toEpochMilli)
      .load()
    val dataGenerationQuery = rateStreamInput.writeStream
      .option("checkpointLocation", s"${DemoDir}/checkpoint")
      .format("delta").toTable(tableName)

    // Streaming
    sparkSession.readStream.format("delta").table(tableName)
      .writeStream.format("console").start()

    sparkSession.streams.awaitAnyTermination(15_000L)

    // Batch
    sparkSession.table(tableName).show(truncate = false)
  }

}

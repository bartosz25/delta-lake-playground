package com.waitingforcode.demo4_max_records

import com.waitingforcode.getOrCreateSparkSessionWithDeltaLake
import org.apache.commons.io.FileUtils
import org.apache.spark.sql.streaming.Trigger

import java.io.File
import java.time.{LocalDateTime, ZoneOffset}
import java.util.concurrent.TimeUnit

object StreamingWriter {

  def main(args: Array[String]): Unit = {
    FileUtils.deleteDirectory(new File(DemoDir))
    val sparkSession = getOrCreateSparkSessionWithDeltaLake(3)

    val rateStreamInput = sparkSession.readStream.format("rate-micro-batch")
      .option("rowsPerBatch", 500)
      .option("numPartitions", 10)
      .option("advanceMillisPerMicroBatch", TimeUnit.MINUTES.toMillis(2))
      .option("startTimestamp", LocalDateTime.of(2022, 5, 25, 10, 25).toInstant(ZoneOffset.UTC).toEpochMilli)
      .load()

    rateStreamInput.writeStream
      .trigger(Trigger.AvailableNow())
      .option("checkpointLocation", CheckpointLocation)
      .option("maxRecordsPerFile", 1)
      .option("compression", "gzip")
      .format("delta")
      .toTable(TableName)

    sparkSession.streams.awaitAnyTermination()
  }
}
